using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Net.WebSockets;

namespace VoiceAgent.Tts;

public sealed class TtsWorker : IAsyncDisposable
{
    private readonly WebSocket _ws;
    private readonly string _piperExeWsl;   // например: /home/adv/piper/piper
    private readonly string _modelWsl;      // например: /home/adv/tts/ru_RU-irina-medium.onnx
    private readonly string _configWsl;     // например: /home/adv/tts/ru_RU-irina-medium.onnx.json

    private readonly ConcurrentQueue<(long TurnId, string Text)> _q = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly CancellationTokenSource _cts = new();

    private volatile int _currentTurnId = 0;

    public TtsWorker(WebSocket ws, string piperExeWsl, string modelWsl, string configWsl)
    {
        _ws = ws;
        _piperExeWsl = piperExeWsl;
        _modelWsl = modelWsl;
        _configWsl = configWsl;
    }

    public void Start()
    {
        _ = Task.Run(LoopAsync);
    }

    public void Enqueue(long turnId, string sentence)
    {
        if (string.IsNullOrWhiteSpace(sentence)) return;
        _q.Enqueue((turnId, sentence.Trim()));
        _signal.Release();
    }

    public void Stop(int newTurnId, string reason)
    {
        _currentTurnId = newTurnId; // всё старое станет "stale"
        // чистим очередь
        while (_q.TryDequeue(out _)) { }
        // UI пусть остановит проигрывание
        _ = WsSendTextSafeAsync($"TTS_STOP");
        _ = WsSendTextSafeAsync($"METRIC:TTS_STOP reason={reason}");
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { _signal.Release(); } catch { }
        _signal.Dispose();
        _cts.Dispose();
        await Task.CompletedTask;
    }

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

            while (_q.TryDequeue(out var item))
            {
                var (turnId, text) = item;

                // если turn уже сменился (barge-in) — пропускаем
                if (turnId != _currentTurnId && _currentTurnId != 0)
                    continue;

                // лог в UI
                await WsSendTextSafeAsync($"TTS_TEXT:{text}");

                // synth
                byte[]? wav = null;
                try
                {
                    wav = await SynthesizeWithPiperInWslAsync(text, _cts.Token);
                }
                catch (Exception ex)
                {
                    await WsSendTextSafeAsync($"METRIC:TTS_ERROR {ex.Message}");
                }

                if (wav == null || wav.Length == 0)
                    continue;

                // если turn сменился пока синтезили — не шлём
                if (turnId != _currentTurnId && _currentTurnId != 0)
                    continue;

                // отправляем WAV как base64 (MVP)
                var b64 = Convert.ToBase64String(wav);
                await WsSendTextSafeAsync($"TTS_WAV_BASE64:{b64}");
            }
        }
    }

    private async Task<byte[]> SynthesizeWithPiperInWslAsync(string text, CancellationToken ct)
    {
        // Пишем WAV в temp Windows, чтобы WSL мог в /mnt/c/...
        var tmp = Path.Combine(Path.GetTempPath(), $"voiceagent_tts_{Guid.NewGuid():N}.wav");

        var wslPath = ToWslPath(tmp);

        // Чтобы не мучаться с quoting русских строк — передаём base64 внутрь bash
        var b64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));

        // Команда:
        // echo '<b64>' | base64 -d | piper --model ... --config ... --output_file <wslPath>
        var bash = $"echo '{b64}' | base64 -d | '{_piperExeWsl}' --model '{_modelWsl}' --config '{_configWsl}' --output_file '{wslPath}'";

        var psi = new ProcessStartInfo
        {
            FileName = "wsl.exe",
            Arguments = $"bash -lc \"{bash}\"",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var p = Process.Start(psi) ?? throw new Exception("Failed to start wsl.exe");
        var stderrTask = p.StandardError.ReadToEndAsync();
        var stdoutTask = p.StandardOutput.ReadToEndAsync();

        await p.WaitForExitAsync(ct);

        var stderr = await stderrTask;
        _ = await stdoutTask;

        if (p.ExitCode != 0)
            throw new Exception($"piper failed exit={p.ExitCode} err={stderr.Trim()}");

        var wav = await File.ReadAllBytesAsync(tmp, ct);
        try { File.Delete(tmp); } catch { }
        return wav;
    }

    private static string ToWslPath(string winPath)
    {
        // C:\Users\...\file.wav -> /mnt/c/Users/.../file.wav
        var p = winPath.Replace('\\', '/');
        if (p.Length >= 2 && p[1] == ':')
        {
            var drive = char.ToLowerInvariant(p[0]);
            p = $"/mnt/{drive}{p.Substring(2)}";
        }
        return p;
    }

    private async Task WsSendTextSafeAsync(string msg)
    {
        if (_ws.State != WebSocketState.Open) return;
        var bytes = Encoding.UTF8.GetBytes(msg);
        try
        {
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch { /* ignore */ }
    }
}
