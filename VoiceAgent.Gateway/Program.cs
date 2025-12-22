using System.Buffers;
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

builder.Services.AddHttpClient<OllamaStreamer>();

var app = builder.Build();

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

static Task WsSend(WebSocket ws, string msg, CancellationToken ct)
{
    var bytes = Encoding.UTF8.GetBytes(msg);
    return ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
}

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

    var dialogHistory = new List<TurnState>(capacity: 64);

    // System prompt (один на всё)
    const string SystemPrompt = """
Ты душевный собеседник, который готов поддержать разговор.
С тобой общаются голосом, и ты готов ответить честно на все вопросы, а так же сгенерировать стихотворение. 
Не используй markdown, списки, эмодзи, спецсимволы. 
Если последняя реплика кажется не полной,возможно она разделилась на сколько - проверь предыдущие реплики, может там начало фразы.
Избегай кавычек, скобок, двоеточий в середине фраз. Будь максимально лаконичной. Порой отвечай одним-двумя словами. 
Пиши обычным текстом. Не повторяйся. В первую очередь учитывай последние вопросы и просьбы. 
Если тебя прервали речью пользователя, не извиняйся и не упоминай прерывание.
Если пользователь продолжает мысль, учитывай предыдущие реплики. Помни какой вопрос тебе задали первым.
Честно говори о том на базе какой модели ты работаешь и честно отвечай что знаешь на все вопросы.
Не вводи в заблуждение собеседника. Говори то, что знаешь, не галлюционируй.
Если просят рассказать потробно - то отвечай максимально длинно и подробно.    
""";

    var promptBuilder = new DialogPromptBuilder(SystemPrompt, maxTurns: 10, maxChars: 12_000);

    long turnSeq = 0;
    TurnState? currentTurn = null;
    CancellationTokenSource? llmCts = null;

    var tts = new TtsWorker(
        ws,
        piperExeWsl: "/home/adv/bin/piper/piper",
        modelWsl: "/home/adv/tts/ru_RU-irina-medium.onnx",
        configWsl: "/home/adv/tts/ru_RU-irina-medium.onnx.json"); 

    tts.Start();

    var channel = GrpcChannel.ForAddress("http://localhost:50051");
    var asr = new Asr.AsrClient(channel);
    using var asrCall = asr.StreamRecognize();

    void CancelLlm(string reason)
    {
        try
        {
            llmCts?.Cancel();
            llmCts?.Dispose();
        }
        catch { /* ignore */ }

        llmCts = null;

        currentTurn?.Assistant.MarkInterrupted();
        Console.WriteLine($"[LLM] cancel ({reason})");
    }

    async Task BargeIn(string reason)
    {
        var newSeq = Interlocked.Increment(ref turnSeq);

        CancelLlm(reason);

        currentTurn?.User.MarkInterrupted();

        tts.Stop(newSeq, reason);

        await WsSend(ws, "BARGE_IN_ACK", shutdown);
        await WsSend(ws, "TTS_STOP", shutdown);
    }

    _ = Task.Run(async () =>
    {
        try
        {
            await foreach (var res in asrCall.ResponseStream.ReadAllAsync(shutdown))
            {
                if (ws.State != WebSocketState.Open) break;

                if (!res.IsFinal)
                {
                    currentTurn?.User.AppendPartial(res.Text);
                    await WsSend(ws, $"ASR_PART:{res.Text}", shutdown);
                    continue;
                }

                var turnId = Interlocked.Increment(ref turnSeq);

                currentTurn = new TurnState(turnId);
                currentTurn.User.MarkFinal(res.Text);

                // Сохраняем в историю
                dialogHistory.Add(currentTurn);

                if (dialogHistory.Count > 50)
                    dialogHistory.RemoveRange(0, dialogHistory.Count - 50);

                await WsSend(ws, $"ASR_FINAL:{res.Text}", shutdown);

                // новый turn => отменяем прошлый LLM
                CancelLlm("new_turn");

                // Для stale-логики TTS: объявляем старт turn'а
                tts.StartTurn(turnId);

                llmCts = new CancellationTokenSource();
                var localTurn = currentTurn;
                var ollama = ctx.RequestServices.GetRequiredService<OllamaStreamer>();

                // ===== PROMPT из истории =====
                var prompt = promptBuilder.Build(dialogHistory, res.Text);

                // ===== STREAM =====
                var buffer = new SentenceDispatchBuffer();

                // Consumer: предложения -> TTS
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var sentence in buffer.GetSentencesAsync(llmCts.Token))
                        {
                            if (localTurn != currentTurn) break;

                            localTurn.Assistant.AppendSpokenSentence(sentence);

                            // очередь в TTS
                            tts.Enqueue(localTurn.TurnId, sentence);
                        }
                    }
                    catch (OperationCanceledException) { }
                });

                // Producer: токены -> UI + buffer + TurnState
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await foreach (var token in ollama.StreamAsync(prompt, llmCts.Token))
                        {
                            if (localTurn != currentTurn) break;

                            localTurn.Assistant.AppendLlmToken(token);

                            // UI текстом (под эквалайзером)
                            await WsSend(ws, $"LLM_PART:{token}", shutdown);

                            // сплит в предложения
                            buffer.AppendToken(token);
                        }

                        // финальный хвост
                        buffer.Complete();

                        if (localTurn == currentTurn)
                            await WsSend(ws, "LLM_FINAL", shutdown);
                    }
                    catch (OperationCanceledException) { }
                });
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"[ASR] stream error: {ex.Message}");
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
                {
                    await BargeIn("vad");
                    // Важно: ресетим ASR, чтобы “склеек” не было
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }
                else if (cmd == "reset")
                {
                    await BargeIn("reset");
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { Reset = true });
                }
                else if (cmd == "end")
                {
                    await asrCall.RequestStream.WriteAsync(new AudioChunk { End = true });
                }

                continue;
            }

            if (r.MessageType == WebSocketMessageType.Binary)
            {
                await asrCall.RequestStream.WriteAsync(
                    new AudioChunk { Pcm16Le = ByteString.CopyFrom(buf, 0, r.Count) });
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
