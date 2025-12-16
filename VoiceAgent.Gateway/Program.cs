using System.Buffers;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using Google.Protobuf;
using Grpc.Net.Client;
using VoiceAgent.Asr;
using VoiceAgent.Gateway;
using VoiceAgent.Tts;
using VoiceAgent.Dialog;
using Grpc.Core;

var builder = WebApplication.CreateBuilder(args);

// =========================
// SERVICES
// =========================
builder.Services.AddHttpClient<OllamaStreamer>();

var app = builder.Build();

// =========================
// HOST LIFETIME (Ctrl+C)
// =========================
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
var shutdownCts = new CancellationTokenSource();

lifetime.ApplicationStopping.Register(() =>
{
    Console.WriteLine("[HOST] Ctrl+C → shutdown");
    shutdownCts.Cancel();
});

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseWebSockets();

// =========================
// HELPERS
// =========================
static Task WsSend(WebSocket ws, string msg, CancellationToken ct)
{
    var bytes = Encoding.UTF8.GetBytes(msg);
    return ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
}

// =========================
// WS ENDPOINT
// =========================
app.Map("/ws", async ctx =>
{
    var shutdown = shutdownCts.Token;

    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = 400;
        return;
    }

    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine("[WS] connected");

    // =========================
    // STATE
    // =========================
    long turnSeq = 0;
    TurnState? currentTurn = null;

    CancellationTokenSource? llmCts = null;

    // =========================
    // TTS
    // =========================
    var tts = new TtsWorker(
        ws,
        piperExeWsl: "/home/adv/bin/piper/piper",
        modelWsl: "/home/adv/tts/ru_RU-irina-medium.onnx",
        configWsl: "/home/adv/tts/ru_RU-irina-medium.onnx.json");

    tts.Start();

    // =========================
    // ASR
    // =========================
    var channel = GrpcChannel.ForAddress("http://localhost:50051");
    var asr = new Asr.AsrClient(channel);
    using var asrCall = asr.StreamRecognize();

    void CancelLlm(string reason)
    {
        llmCts?.Cancel();
        llmCts?.Dispose();
        llmCts = null;

        currentTurn?.Assistant.MarkInterrupted();

        Console.WriteLine($"[LLM] cancel ({reason})");
    }

    async Task BargeIn(string reason)
    {
        Interlocked.Increment(ref turnSeq);

        CancelLlm(reason);

        currentTurn?.User.MarkInterrupted();

        tts.Stop(currentTurn?.TurnId ?? 0, reason);

        await WsSend(ws, "BARGE_IN_ACK", shutdown);
        await WsSend(ws, "TTS_STOP", shutdown);
    }

    // =========================
    // ASR RECEIVE
    // =========================
    _ = Task.Run(async () =>
    {
        await foreach (var res in asrCall.ResponseStream.ReadAllAsync())
        {
            if (ws.State != WebSocketState.Open) break;

            if (!res.IsFinal)
            {
                currentTurn?.User.AppendPartial(res.Text);
                await WsSend(ws, $"ASR_PART:{res.Text}", shutdown);
                continue;
            }

            // ⭐ ASR FINAL → новый turn
            var turnId = Interlocked.Increment(ref turnSeq);
            currentTurn = new TurnState(turnId);
            tts.StartTurn(turnId);
            currentTurn.User.MarkFinal(res.Text);

            await WsSend(ws, $"ASR_FINAL:{res.Text}", shutdown);

            CancelLlm("new_turn");

            llmCts = new CancellationTokenSource();
            var localTurn = currentTurn;
            var ollama = ctx.RequestServices.GetRequiredService<OllamaStreamer>();

            // =========================
            // LLM STREAM
            // =========================
            var buffer = new SentenceDispatchBuffer();
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var token in ollama.StreamAsync(res.Text, llmCts.Token))
                    {
                        if (localTurn != currentTurn) break;

                        localTurn.Assistant.AppendLlmToken(token);
                        await WsSend(ws, $"LLM_PART:{token}", shutdown);

             
                        buffer.AppendToken(token);
                    }
                    buffer.Complete();

                    await WsSend(ws, "LLM_FINAL", shutdown);
                }
                catch (OperationCanceledException) { }
            });

            // =========================
            // SENTENCE → TTS
            // =========================
            

            _ = Task.Run(async () =>
            {
                await foreach (var sentence in buffer.GetSentencesAsync(llmCts.Token))
                {
                    if (localTurn != currentTurn) break;

                    localTurn.Assistant.AppendSpokenSentence(sentence);
                    tts.Enqueue((int)localTurn.TurnId, sentence);
                }
            });
        }
    });

    // =========================
    // WS RECEIVE
    // =========================
    var buf = ArrayPool<byte>.Shared.Rent(64 * 1024);
    try
    {
        while (ws.State == WebSocketState.Open && !shutdown.IsCancellationRequested)
        {
            var r = await ws.ReceiveAsync(buf, shutdown);

            if (r.MessageType == WebSocketMessageType.Close)
                break;

            if (r.MessageType == WebSocketMessageType.Text)
            {
                var cmd = Encoding.UTF8.GetString(buf, 0, r.Count).Trim();

                if (cmd == "barge_in")
                    await BargeIn("vad");

                if (cmd == "reset")
                    await BargeIn("reset");

                continue;
            }

            if (r.MessageType == WebSocketMessageType.Binary)
            {
                await asrCall.RequestStream.WriteAsync(
                    new AudioChunk
                    {
                        Pcm16Le = ByteString.CopyFrom(buf, 0, r.Count)
                    });
            }
        }
    }
    finally
    {
        Console.WriteLine("[WS] disconnected");
        CancelLlm("ws_close");

        ArrayPool<byte>.Shared.Return(buf);
        try { await asrCall.RequestStream.CompleteAsync(); } catch { }
    }
});

app.Run("http://0.0.0.0:5079");
