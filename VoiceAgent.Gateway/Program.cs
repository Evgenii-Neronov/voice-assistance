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

var appLifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
var shutdownCts = new CancellationTokenSource();

appLifetime.ApplicationStopping.Register(() =>
{
    Console.WriteLine("[HOST] ApplicationStopping (Ctrl+C)");
    shutdownCts.Cancel();
});

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
    var shutdownToken = shutdownCts.Token;

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    Console.WriteLine("[WS] Client connected");
    using var ws = await context.WebSockets.AcceptWebSocketAsync();

    // gRPC channel to ASR (local)
    var channel = GrpcChannel.ForAddress("http://localhost:50051");
    var asrClient = new Asr.AsrClient(channel);
    using var asrCall = asrClient.StreamRecognize();

    // LLM cancellation + turn tracking
    CancellationTokenSource? llmCts = null;
    long turnId = 0;

    // sentence buffer for current turn
    SentenceDispatchBuffer? sentenceBuffer = null;

    void CancelLlm(string reason)
    {
        try
        {
            llmCts?.Cancel();
            llmCts?.Dispose();
        }
        catch { /* ignore */ }

        llmCts = null;

        try { sentenceBuffer?.Reset(); } catch { }
        sentenceBuffer = null;

        Console.WriteLine($"[LLM] Cancel reason={reason}");
    }

    CancellationTokenSource StartNewLlmTurn(string reason, out long newTurnId)
    {
        Interlocked.Increment(ref turnId);
        newTurnId = Interlocked.Read(ref turnId);

        CancelLlm($"new_turn:{reason}");

        llmCts = new CancellationTokenSource();
        sentenceBuffer = new SentenceDispatchBuffer(minSentenceLength: 30);

        Console.WriteLine($"[TURN] turnId={newTurnId} ({reason})");
        return llmCts;
    }

    async Task DoBargeInAsync(string reason)
    {
        // new global turn => anything in-flight becomes stale
        Interlocked.Increment(ref turnId);
        var curTurn = Interlocked.Read(ref turnId);

        CancelLlm($"barge_in:{reason}");

        Console.WriteLine($"[BARGE_IN] reason={reason} -> turnId={curTurn}");

        // notify UI (and later stop TTS)
        try { await WsSendText(ws, "BARGE_IN_ACK", shutdownToken); } catch { }
        try { await WsSendText(ws, "TTS_STOP", shutdownToken); } catch { }
        try { await WsSendText(ws, "METRIC:LLM_CANCELED", shutdownToken); } catch { }
    }

    // === ASR → Browser + ASR → LLM ===
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
                    Console.WriteLine($"[ASR][PART] {res.Text}");
                    await WsSendText(ws, $"ASR_PART:{res.Text}", shutdownToken);
                    continue;
                }

                // FINAL
                Console.WriteLine($"[ASR][FINAL] {res.Text}");
                await WsSendText(ws, $"ASR_FINAL:{res.Text}", shutdownToken);

                // Anchor timestamp for metrics for THIS turn
                var asrFinalStamp = Stopwatch.GetTimestamp();
                try { await WsSendText(ws, "METRIC:ASR_FINAL", shutdownToken); } catch { }

                // Interrupt previous LLM and start new turn
                var cts = StartNewLlmTurn("asr_final", out var localTurn);
                var buffer = sentenceBuffer!;

                Console.WriteLine($"[LLM] Start generation turnId={localTurn}");

                var ollama = context.RequestServices.GetRequiredService<OllamaStreamer>();

                // Consumer: sentences ready for TTS → UI log table as TTS_TEXT
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var sentence in buffer.GetSentencesAsync(cts.Token))
                        {
                            if (localTurn != Interlocked.Read(ref turnId))
                                break;

                            Console.WriteLine($"[TTS_TEXT] {sentence}");
                            await WsSendText(ws, $"TTS_TEXT:{sentence}", shutdownToken);

                            // позже здесь:
                            // ttsWorker.Enqueue(sentence, cts.Token);
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[TTS_TEXT] consumer error: {ex.Message}");
                    }
                });

                // Producer: LLM tokens → UI + sentence buffer
                _ = Task.Run(async () =>
                {
                    long firstTokenStamp = 0;
                    bool firstToken = true;

                    try
                    {
                        await foreach (var token in ollama.StreamAsync(res.Text, cts.Token))
                        {
                            // Stop emitting if barge-in/new turn happened
                            if (localTurn != Interlocked.Read(ref turnId))
                                break;

                            if (firstToken)
                            {
                                firstToken = false;
                                firstTokenStamp = Stopwatch.GetTimestamp();

                                var ms = MsBetween(asrFinalStamp, firstTokenStamp);
                                Console.WriteLine($"[LLM] First token (turnId={localTurn}) +{ms:F1}ms");
                                try { await WsSendText(ws, $"METRIC:LLM_FIRST_TOKEN ms_from_asr_final={ms:F1}", shutdownToken); } catch { }
                            }

                            // ✅ 1) токен в UI (под эквалайзером)
                            await WsSendText(ws, $"LLM_PART:{token}", cts.Token);

                            // ✅ 2) токен в буфер предложений
                            buffer.AppendToken(token);
                        }

                        // If canceled or stale -> don't finalize
                        if (cts.IsCancellationRequested || localTurn != Interlocked.Read(ref turnId))
                            return;

                        // ✅ закрываем поток предложений (consumer завершится)
                        buffer.Complete();

                        var endStamp = Stopwatch.GetTimestamp();
                        var msFromAsr = MsBetween(asrFinalStamp, endStamp);
                        var msFromFirst = firstTokenStamp == 0 ? -1 : MsBetween(firstTokenStamp, endStamp);

                        Console.WriteLine($"[LLM] Generation finished (turnId={localTurn}) total={msFromAsr:F1}ms");

                        try
                        {
                            await WsSendText(ws, $"METRIC:LLM_FINAL ms_from_asr_final={msFromAsr:F1} ms_from_first_token={msFromFirst:F1}", shutdownToken);
                        }
                        catch { }

                        await WsSendText(ws, "LLM_FINAL", shutdownToken);
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine($"[LLM] Generation interrupted (turnId={localTurn})");
                        try { await WsSendText(ws, "METRIC:LLM_CANCELED", shutdownToken); } catch { }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[LLM] Error: {ex.Message}");
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ASR] Stream error: {ex.Message}");
        }
    });

    // === Browser → ASR / Commands ===
    var bufferBytes = ArrayPool<byte>.Shared.Rent(64 * 1024);
    try
    {
        while (ws.State == WebSocketState.Open && !shutdownToken.IsCancellationRequested)
        {
            var result = await ws.ReceiveAsync(bufferBytes, shutdownToken);

            if (result.MessageType == WebSocketMessageType.Close)
                break;

            if (result.MessageType == WebSocketMessageType.Text)
            {
                var cmd = Encoding.UTF8.GetString(bufferBytes, 0, result.Count).Trim();
                Console.WriteLine($"[WS][CMD] {cmd}");

                if (cmd.Equals("reset", StringComparison.OrdinalIgnoreCase))
                {
                    await DoBargeInAsync("reset_cmd");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }
                else if (cmd.Equals("end", StringComparison.OrdinalIgnoreCase))
                {
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { End = true });
                }
                else if (cmd.Equals("barge_in", StringComparison.OrdinalIgnoreCase))
                {
                    await DoBargeInAsync("vad_start");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
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
        Console.WriteLine("[WS] Client disconnected");

        ArrayPool<byte>.Shared.Return(bufferBytes);

        CancelLlm("ws_disconnect");

        try { await asrCall.RequestStream.CompleteAsync(); } catch { }
        try { await asrReceiveTask; } catch { }

        try { await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", shutdownToken); } catch { }
    }
});

app.Run("http://0.0.0.0:5079");
