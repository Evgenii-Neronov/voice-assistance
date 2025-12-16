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

// Ollama streaming client
builder.Services.AddHttpClient<OllamaStreamer>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseWebSockets();

static Task WsSendText(WebSocket ws, string text, CancellationToken ct = default)
{
    var bytes = Encoding.UTF8.GetBytes(text);
    return ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
}

static double MsBetween(long startStamp, long endStamp)
{
    return (endStamp - startStamp) * 1000.0 / Stopwatch.Frequency;
}

app.Map("/ws", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    Console.WriteLine("[WS] Client connected");
    using var ws = await context.WebSockets.AcceptWebSocketAsync();

    // gRPC ASR
    var channel = GrpcChannel.ForAddress("http://localhost:50051");
    var asrClient = new Asr.AsrClient(channel);
    using var asrCall = asrClient.StreamRecognize();

    // ===== TURN / CANCELLATION =====
    CancellationTokenSource? llmCts = null;
    SentenceDispatchBuffer? sentenceBuffer = null;

    long turnId = 0;

    void CancelTurn(string reason)
    {
        try { llmCts?.Cancel(); } catch { }
        llmCts = null;

        try { sentenceBuffer?.Reset(); } catch { }
        sentenceBuffer = null;

        Console.WriteLine($"[TURN] Cancel reason={reason}");
    }

    CancellationTokenSource StartNewTurn(string reason, out long localTurn)
    {
        Interlocked.Increment(ref turnId);
        localTurn = Interlocked.Read(ref turnId);

        CancelTurn($"new_turn:{reason}");

        llmCts = new CancellationTokenSource();
        sentenceBuffer = new SentenceDispatchBuffer();

        Console.WriteLine($"[TURN] Start turnId={localTurn} ({reason})");
        return llmCts;
    }

    async Task DoBargeInAsync(string reason)
    {
        Interlocked.Increment(ref turnId);
        CancelTurn($"barge_in:{reason}");

        Console.WriteLine($"[BARGE_IN] reason={reason}");

        try { await WsSendText(ws, "BARGE_IN_ACK"); } catch { }
        try { await WsSendText(ws, "TTS_STOP"); } catch { }
        try { await WsSendText(ws, "METRIC:LLM_CANCELED"); } catch { }
    }

    // ===== ASR → LLM =====
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
                    await WsSendText(ws, $"ASR_PART:{res.Text}");
                    continue;
                }

                // ASR FINAL
                await WsSendText(ws, $"ASR_FINAL:{res.Text}");
                await WsSendText(ws, "METRIC:ASR_FINAL");

                var asrFinalStamp = Stopwatch.GetTimestamp();

                var cts = StartNewTurn("asr_final", out var localTurn);
                var buffer = sentenceBuffer!;
                var ollama = context.RequestServices.GetRequiredService<OllamaStreamer>();

                // ===== SENTENCE → WS (+ future TTS) =====
                _ = Task.Run(async () =>
                {
                    await foreach (var sentence in buffer.GetSentencesAsync(cts.Token))
                    {
                        if (localTurn != Interlocked.Read(ref turnId))
                            break;

                        Console.WriteLine($"[TTS][QUEUE] {sentence}");
                        await WsSendText(ws, $"LLM_PART:{sentence} ");
                    }
                });

                // ===== LLM STREAM =====
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
                                await WsSendText(ws, $"METRIC:LLM_FIRST_TOKEN ms_from_asr_final={ms:F1}");
                            }

                            // ✅ 1) СРАЗУ отдаём токен в UI (как раньше)
                            // Важно: отмена / stale turn автоматически оборвёт по cts.Token
                            await WsSendText(ws, $"LLM_PART:{token}", cts.Token);

                            // ✅ 2) И параллельно кормим сплиттер, чтобы TTS получал предложения
                            buffer.AppendToken(token);
                        }

                        if (cts.IsCancellationRequested || localTurn != Interlocked.Read(ref turnId))
                            return;

                        buffer.Complete();

                        var endStamp = Stopwatch.GetTimestamp();
                        var totalMs = MsBetween(asrFinalStamp, endStamp);
                        var fromFirst = firstTokenStamp == 0 ? -1 : MsBetween(firstTokenStamp, endStamp);

                        await WsSendText(
                            ws,
                            $"METRIC:LLM_FINAL ms_from_asr_final={totalMs:F1} ms_from_first_token={fromFirst:F1}"
                        );

                        await WsSendText(ws, "LLM_FINAL");
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine($"[LLM] canceled turnId={localTurn}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[LLM] error {ex.Message}");
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ASR] error {ex.Message}");
        }
    });

    // ===== WS → ASR / COMMANDS =====
    var bufferBytes = ArrayPool<byte>.Shared.Rent(64 * 1024);
    try
    {
        while (ws.State == WebSocketState.Open)
        {
            var result = await ws.ReceiveAsync(bufferBytes, CancellationToken.None);

            if (result.MessageType == WebSocketMessageType.Close)
                break;

            if (result.MessageType == WebSocketMessageType.Text)
            {
                var cmd = Encoding.UTF8.GetString(bufferBytes, 0, result.Count).Trim();

                if (cmd == "reset")
                {
                    await DoBargeInAsync("reset_cmd");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }
                else if (cmd == "barge_in")
                {
                    await DoBargeInAsync("vad_start");
                }
                else if (cmd == "end")
                {
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { End = true });
                }

                continue;
            }

            if (result.MessageType == WebSocketMessageType.Binary)
            {
                await asrCall.RequestStream.WriteAsync(
                    new AudioChunk
                    {
                        Pcm16Le = ByteString.CopyFrom(bufferBytes, 0, result.Count)
                    });
            }
        }
    }
    finally
    {
        Console.WriteLine("[WS] disconnected");

        ArrayPool<byte>.Shared.Return(bufferBytes);
        CancelTurn("ws_disconnect");

        try { await asrCall.RequestStream.CompleteAsync(); } catch { }
        try { await asrReceiveTask; } catch { }

        try
        {
            await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        }
        catch { }
    }
});

app.Run("http://0.0.0.0:5079");
