using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

namespace VoiceAgent.Tts;

/// <summary>
/// Последовательный TTS-воркер.
///  - принимает предложения
///  - озвучивает строго по очереди
///  - корректно отменяется (barge-in)
/// </summary>
public sealed class TtsWorker : IDisposable
{
    private readonly Channel<TtsItem> _queue;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loopTask;

    private readonly string _piperExe;
    private readonly string _modelPath;
    private readonly string _configPath;
    private readonly string _outputDir;

    public TtsWorker(
        string piperExe,
        string modelPath,
        string configPath,
        string outputDir)
    {
        _piperExe = piperExe;
        _modelPath = modelPath;
        _configPath = configPath;
        _outputDir = outputDir;

        Directory.CreateDirectory(_outputDir);

        _queue = Channel.CreateUnbounded<TtsItem>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _loopTask = Task.Run(ProcessLoopAsync);
    }

    // =========================
    // PUBLIC API
    // =========================

    /// <summary>
    /// Добавить предложение в очередь озвучки
    /// </summary>
    public void Enqueue(string text, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(text))
            return;

        var item = new TtsItem(text.Trim(), ct);
        _queue.Writer.TryWrite(item);
    }

    /// <summary>
    /// Полный сброс очереди (barge-in)
    /// </summary>
    public void Reset()
    {
        Console.WriteLine("[TTS] Reset queue");

        while (_queue.Reader.TryRead(out _)) { }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _queue.Writer.TryComplete();

        try { _loopTask.Wait(); } catch { }
    }

    // =========================
    // WORK LOOP
    // =========================

    private async Task ProcessLoopAsync()
    {
        Console.WriteLine("[TTS] Worker started");

        try
        {
            while (await _queue.Reader.WaitToReadAsync(_cts.Token))
            {
                while (_queue.Reader.TryRead(out var item))
                {
                    if (item.CancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine("[TTS] Skip canceled item");
                        continue;
                    }

                    await SpeakAsync(item);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TTS] Fatal error: {ex}");
        }
        finally
        {
            Console.WriteLine("[TTS] Worker stopped");
        }
    }

    // =========================
    // PIPER INVOCATION
    // =========================

    private async Task SpeakAsync(TtsItem item)
    {
        var text = item.Text;
        var ct = item.CancellationToken;

        var fileName = $"{DateTime.UtcNow:HHmmssfff}_{Guid.NewGuid():N}.wav";
        var outPath = Path.Combine(_outputDir, fileName);

        Console.WriteLine($"[TTS] Speak: \"{text}\"");

        var psi = new ProcessStartInfo
        {
            FileName = _piperExe,
            Arguments =
                $"--model \"{_modelPath}\" " +
                $"--config \"{_configPath}\" " +
                $"--output_file \"{outPath}\"",
            RedirectStandardInput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var proc = new Process { StartInfo = psi };

        proc.Start();

        await proc.StandardInput.WriteAsync(text);
        await proc.StandardInput.FlushAsync();
        proc.StandardInput.Close();

        using var reg = ct.Register(() =>
        {
            try
            {
                if (!proc.HasExited)
                {
                    Console.WriteLine("[TTS] Killing piper (barge-in)");
                    proc.Kill(entireProcessTree: true);
                }
            }
            catch { }
        });

        var stderr = await proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync(ct);

        if (proc.ExitCode != 0)
        {
            Console.WriteLine($"[TTS] Piper error: {stderr}");
            return;
        }

        Console.WriteLine($"[TTS] Done → {outPath}");

        // TODO:
        //  - отправить WAV в браузер
        //  - или декодировать и стримить PCM
    }

    // =========================
    // INTERNAL TYPES
    // =========================

    private sealed record TtsItem(
        string Text,
        CancellationToken CancellationToken
    );
}
