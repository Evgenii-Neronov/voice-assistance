using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using VoiceAgent.Asr;
using VoiceAgent.Gateway;
using VoiceAgent.Tts;

var builder = WebApplication.CreateBuilder(args);

// LLM (Ollama)
builder.Services.AddHttpClient<OllamaStreamer>();

var app = builder.Build();

// ===== graceful shutdown (Ctrl+C) =====
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
var shutdownCts = new CancellationTokenSource();

lifetime.ApplicationStopping.Register(() =>
{
    Console.WriteLine("[HOST] ApplicationStopping (Ctrl+C)");
    shutdownCts.Cancel();
});

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseWebSockets();

// ===== helpers =====
static Task WsSendText(WebSocket ws, string text, CancellationToken ct = default)
{
    if (ws.State != WebSocketState.Open) return Task.CompletedTask;
    var bytes = Encoding.UTF8.GetBytes(text);
    return ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
}

static double MsBetween(long a, long b)
{
    return (b - a) * 1000.0 / Stopwatch.Frequency;
}

// ===== WS endpoint =====
app.Map("/ws", async context =>
{
    var shutdownToken = shutdownCts.Token;

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    Console.WriteLine("[WS] Client connected");
    using var ws = await context.WebSockets.AcceptWebSocketAsync();

    // ===== TTS worker (Piper via WSL) =====
    var tts = new TtsWorker(
        ws,
        piperExeWsl: "/home/adv/piper/piper",
        modelWsl: "/home/adv/tts/ru_RU-irina-medium.onnx",
        configWsl: "/home/adv/tts/ru_RU-irina-medium.onnx.json");

    tts.Start();

    // ===== ASR gRPC =====
    var channel = GrpcChannel.ForAddress("http://localhost:50051");
    var asrClient = new Asr.AsrClient(channel);
    using var asrCall = asrClient.StreamRecognize();

    // ===== LLM / turn management =====
    CancellationTokenSource? llmCts = null;
    long turnId = 0;
    SentenceDispatchBuffer? sentenceBuffer = null;

    void CancelLlm(string reason)
    {
        try
        {
            llmCts?.Cancel();
            llmCts?.Dispose();
        }
        catch { }

        llmCts = null;
        sentenceBuffer?.Reset();
        sentenceBuffer = null;

        Console.WriteLine($"[LLM] Cancel reason={reason}");
    }

    CancellationTokenSource StartNewTurn(string reason, out long newTurn)
    {
        Interlocked.Increment(ref turnId);
        newTurn = Interlocked.Read(ref turnId);

        CancelLlm($"new_turn:{reason}");

        llmCts = new CancellationTokenSource();
        sentenceBuffer = new SentenceDispatchBuffer(minSentenceLength: 30);

        Console.WriteLine($"[TURN] turnId={newTurn} ({reason})");
        return llmCts;
    }

    async Task DoBargeInAsync(string reason)
    {
        Interlocked.Increment(ref turnId);
        var curTurn = Interlocked.Read(ref turnId);

        CancelLlm($"barge_in:{reason}");
        tts.Stop((int)curTurn, reason);

        Console.WriteLine($"[BARGE_IN] reason={reason} -> turnId={curTurn}");

        await WsSendText(ws, "BARGE_IN_ACK", shutdownToken);
        await WsSendText(ws, "TTS_STOP", shutdownToken);
        await WsSendText(ws, "METRIC:LLM_CANCELED", shutdownToken);
    }

    // ===== ASR → LLM pipeline =====
    var asrReceiveTask = Task.Run(async () =>
    {
        try
        {
            await foreach (var res in asrCall.ResponseStream.ReadAllAsync())
            {
                if (ws.State != WebSocketState.Open)
                    break;

                if (!res.IsFinal)
                {
                    await WsSendText(ws, $"ASR_PART:{res.Text}", shutdownToken);
                    continue;
                }

                // ===== ASR FINAL =====
                await WsSendText(ws, $"ASR_FINAL:{res.Text}", shutdownToken);

                var asrFinalStamp = Stopwatch.GetTimestamp();
                await WsSendText(ws, "METRIC:ASR_FINAL", shutdownToken);

                var cts = StartNewTurn("asr_final", out var localTurn);
                var buffer = sentenceBuffer!;
                var ollama = context.RequestServices.GetRequiredService<OllamaStreamer>();

                // ---- consumer: sentences → TTS ----
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var sentence in buffer.GetSentencesAsync(cts.Token))
                        {
                            if (localTurn != Interlocked.Read(ref turnId))
                                break;

                            await WsSendText(ws, $"TTS_TEXT:{sentence}", shutdownToken);

                            // 🔥 РЕАЛЬНЫЙ ЗАПУСК TTS
                            tts.Enqueue(localTurn, sentence);
                        }
                    }
                    catch (OperationCanceledException) { }
                });

                // ---- producer: LLM tokens ----
                _ = Task.Run(async () =>
                {
                    bool firstToken = true;
                    long firstTokenStamp = 0;

                    try
                    {
                        await foreach (var token in ollama.StreamAsync(res.Text, cts.Token))
                        {
                            if (localTurn != Interlocked.Read(ref turnId))
                                break;

                            if (firstToken)
                            {
                                firstToken = false;
                                firstTokenStamp = Stopwatch.GetTimestamp();
                                var ms = MsBetween(asrFinalStamp, firstTokenStamp);
                                await WsSendText(ws, $"METRIC:LLM_FIRST_TOKEN ms={ms:F1}", shutdownToken);
                            }

                            // 👉 в UI (под эквалайзером)
                            await WsSendText(ws, $"LLM_PART:{token}", shutdownToken);

                            // 👉 в буфер предложений
                            buffer.AppendToken(token);
                        }

                        if (cts.IsCancellationRequested || localTurn != Interlocked.Read(ref turnId))
                            return;

                        buffer.Complete();

                        var endStamp = Stopwatch.GetTimestamp();
                        var totalMs = MsBetween(asrFinalStamp, endStamp);
                        await WsSendText(ws, $"METRIC:LLM_FINAL ms={totalMs:F1}", shutdownToken);
                        await WsSendText(ws, "LLM_FINAL", shutdownToken);
                    }
                    catch (OperationCanceledException)
                    {
                        await WsSendText(ws, "METRIC:LLM_CANCELED", shutdownToken);
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ASR] error: {ex.Message}");
        }
    });

    // ===== Browser → ASR / commands =====
    var buf = ArrayPool<byte>.Shared.Rent(64 * 1024);
    try
    {
        while (ws.State == WebSocketState.Open && !shutdownToken.IsCancellationRequested)
        {
            var result = await ws.ReceiveAsync(buf, shutdownToken);

            if (result.MessageType == WebSocketMessageType.Close)
                break;

            if (result.MessageType == WebSocketMessageType.Text)
            {
                var cmd = Encoding.UTF8.GetString(buf, 0, result.Count).Trim();

                if (cmd == "reset")
                {
                    await DoBargeInAsync("reset_cmd");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }
                else if (cmd == "barge_in")
                {
                    await DoBargeInAsync("vad_start");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }

                continue;
            }

            if (result.MessageType == WebSocketMessageType.Binary)
            {
                await asrCall.RequestStream.WriteAsync(new AudioChunk
                {
                    Pcm16Le = ByteString.CopyFrom(buf, 0, result.Count)
                });
            }
        }
    }
    finally
    {
        Console.WriteLine("[WS] Client disconnected");

        ArrayPool<byte>.Shared.Return(buf);

        CancelLlm("ws_disconnect");
        tts.Stop((int)Interlocked.Read(ref turnId), "ws_disconnect");

        try { await asrCall.RequestStream.CompleteAsync(); } catch { }
        try { await asrReceiveTask; } catch { }
        try { await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", shutdownToken); } catch { }
    }
});

app.Run("http://0.0.0.0:5079");
