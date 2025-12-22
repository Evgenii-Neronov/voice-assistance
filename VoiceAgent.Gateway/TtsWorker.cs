using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;

namespace VoiceAgent.Tts;

public sealed class TtsWorker : IAsyncDisposable
{
    private readonly WebSocket _ws;
    private readonly string _piperExeWsl;
    private readonly string _modelWsl;
    private readonly string _configWsl;

    private readonly ConcurrentQueue<(long TurnId, string Text)> _queue = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly CancellationTokenSource _cts = new();

    private long _currentTurnId = 0;

    public TtsWorker(
        WebSocket ws,
        string piperExeWsl,
        string modelWsl,
        string configWsl)
    {
        _ws = ws;
        _piperExeWsl = piperExeWsl;
        _modelWsl = modelWsl;
        _configWsl = configWsl;
    }

    // =========================
    // lifecycle
    // =========================

    public void Start()
    {
        Console.WriteLine("[TTS] Worker started");
        _ = Task.Run(LoopAsync); 
    }

    public void StartTurn(long turnId)
    {
        _currentTurnId = turnId;
        while (_queue.TryDequeue(out _)) { }

        Console.WriteLine($"[TTS] StartTurn turnId={turnId}");
        _ = WsSendTextSafeAsync($"METRIC:TTS_START_TURN turnId={turnId}");
    }

    public void Stop(long newTurnId, string reason)
    {
        _currentTurnId = newTurnId;
        while (_queue.TryDequeue(out _)) { }

        Console.WriteLine($"[TTS] Stop reason={reason} newTurnId={newTurnId}");
        _ = WsSendTextSafeAsync("TTS_STOP");
        _ = WsSendTextSafeAsync($"METRIC:TTS_STOP reason={reason} newTurnId={newTurnId}");
    }

    public async ValueTask DisposeAsync()
    {
        Console.WriteLine("[TTS] Dispose requested");
        _cts.Cancel();
        try { _signal.Release(); } catch { }
        _signal.Dispose();
        _cts.Dispose();
        await Task.CompletedTask;
    }

    // =========================
    // public API
    // =========================

    public void Enqueue(long turnId, string sentence)
    {
        if (string.IsNullOrWhiteSpace(sentence))
            return;

        var text = sentence.Trim();
        _queue.Enqueue((turnId, text));
        _signal.Release();

        Console.WriteLine($"[TTS] Enqueue turnId={turnId} len={text.Length}");
        _ = WsSendTextSafeAsync($"METRIC:TTS_ENQUEUE turnId={turnId} len={text.Length}");
    }

    // =========================
    // main loop
    // =========================

    private async Task LoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await _signal.WaitAsync(_cts.Token);
            }
            catch
            {
                break;
            }

            while (_queue.TryDequeue(out var item))
            {
                var (turnId, text) = item;

                if (IsStale(turnId))
                {
                    Console.WriteLine($"[TTS] DROP_STALE turnId={turnId} current={_currentTurnId}");
                    await WsSendTextSafeAsync(
                        $"METRIC:TTS_DROP_STALE turnId={turnId} current={_currentTurnId}");
                    continue;
                }

                Console.WriteLine($"[TTS] Dequeue turnId={turnId} len={text.Length}");
                await WsSendTextSafeAsync($"TTS_TEXT:{text}");

                byte[] wav;
                var sw = Stopwatch.StartNew();

                try
                {
                    Console.WriteLine($"[TTS] Synth start turnId={turnId}");
                    await WsSendTextSafeAsync($"METRIC:TTS_SYNTH_START turnId={turnId}");

                    wav = await SynthesizeWithPiperInWslAsync(text, _cts.Token);

                    sw.Stop();
                    Console.WriteLine(
                        $"[TTS] Synth done turnId={turnId} bytes={wav.Length} ms={sw.ElapsedMilliseconds}");

                    await WsSendTextSafeAsync(
                        $"METRIC:TTS_SYNTH_DONE turnId={turnId} bytes={wav.Length} ms={sw.ElapsedMilliseconds}");
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    Console.WriteLine(
                        $"[TTS] Synth ERROR turnId={turnId} ms={sw.ElapsedMilliseconds} err={ex.Message}");

                    await WsSendTextSafeAsync(
                        $"METRIC:TTS_ERROR turnId={turnId} ms={sw.ElapsedMilliseconds} err={ex.Message}");
                    continue;
                }

                if (IsStale(turnId))
                {
                    Console.WriteLine(
                        $"[TTS] DROP_AFTER_SYNTH turnId={turnId} current={_currentTurnId}");

                    await WsSendTextSafeAsync(
                        $"METRIC:TTS_DROP_AFTER_SYNTH turnId={turnId} current={_currentTurnId}");
                    continue;
                }

                var b64 = Convert.ToBase64String(wav);
                Console.WriteLine(
                    $"[TTS] base64 ready turnId={turnId} len={b64.Length} (~{b64.Length / 1024}KB)");

                await WsSendTextSafeAsync(
                    $"METRIC:TTS_B64_READY turnId={turnId} len={b64.Length}");

                var payload = "TTS_WAV_BASE64:" + b64;
                var sendSw = Stopwatch.StartNew();

                var ok = await WsSendTextSafeAsync(payload);
                sendSw.Stop();

                if (ok)
                {
                    Console.WriteLine(
                        $"[TTS] Sent WAV turnId={turnId} chars={payload.Length} ms={sendSw.ElapsedMilliseconds}");

                    await WsSendTextSafeAsync(
                        $"METRIC:TTS_SENT turnId={turnId} chars={payload.Length} ms={sendSw.ElapsedMilliseconds}");
                }
                else
                {
                    Console.WriteLine($"[TTS] Send failed turnId={turnId}");
                }
            }
        }

        Console.WriteLine("[TTS] Loop finished");
    }

    // =========================
    // helpers
    // =========================

    private bool IsStale(long turnId)
        => _currentTurnId != 0 && turnId != _currentTurnId;

    private async Task<byte[]> SynthesizeWithPiperInWslAsync(
        string text,
        CancellationToken ct)
    {
        var tmp = Path.Combine(
            Path.GetTempPath(),
            $"voiceagent_tts_{Guid.NewGuid():N}.wav");

        var wslPath = ToWslPath(tmp);
        var b64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));

        var bash =
            $"echo '{b64}' | base64 -d | '{_piperExeWsl}' " +
            $"--model '{_modelWsl}' --config '{_configWsl}' --output_file '{wslPath}'";

        var psi = new ProcessStartInfo
        {
            FileName = "wsl.exe",
            Arguments = $"bash -lc \"{bash}\"",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var p = Process.Start(psi)
            ?? throw new Exception("Failed to start wsl.exe");

        var stderrTask = p.StandardError.ReadToEndAsync();
        _ = p.StandardOutput.ReadToEndAsync();

        await p.WaitForExitAsync(ct);
        var stderr = await stderrTask;

        if (p.ExitCode != 0)
            throw new Exception($"piper failed exit={p.ExitCode} err={stderr.Trim()}");

        var wav = await File.ReadAllBytesAsync(tmp, ct);
        try { File.Delete(tmp); } catch { }

        return wav;
    }

    private static string ToWslPath(string winPath)
    {
        var p = winPath.Replace('\\', '/');
        if (p.Length >= 2 && p[1] == ':')
        {
            var drive = char.ToLowerInvariant(p[0]);
            p = $"/mnt/{drive}{p.Substring(2)}";
        }
        return p;
    }

    private async Task<bool> WsSendTextSafeAsync(string msg)
    {
        if (_ws.State != WebSocketState.Open)
            return false;

        try
        {
            var bytes = Encoding.UTF8.GetBytes(msg);
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TTS][WS] Send error: {ex.Message}");
            return false;
        }
    }
}
