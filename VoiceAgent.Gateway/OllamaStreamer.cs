using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace VoiceAgent.Gateway;

public sealed class OllamaStreamer
{
    private readonly HttpClient _http;

    public OllamaStreamer(HttpClient http)
    {
        _http = http;
        _http.BaseAddress = new Uri("http://localhost:11434");
        _http.Timeout = Timeout.InfiniteTimeSpan;
    }

    public async IAsyncEnumerable<string> StreamAsync(
        string prompt,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var request = new
        {
            model = "llama3.1:8b",
            prompt,
            stream = true
        };

        using var httpReq = new HttpRequestMessage(HttpMethod.Post, "/api/generate")
        {
            Content = JsonContent.Create(request)
        };

        using var resp = await _http.SendAsync(
            httpReq,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);

        resp.EnsureSuccessStatusCode();

        await using var stream = await resp.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 8192);

        while (!reader.EndOfStream && !cancellationToken.IsCancellationRequested)
        {
            string? line;

            try
            {
                line = await reader.ReadLineAsync().WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }

            if (string.IsNullOrWhiteSpace(line))
                continue;

            string? tokenText = null;
            bool done = false;

            try
            {
                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                if (root.TryGetProperty("response", out var token))
                    tokenText = token.GetString();

                if (root.TryGetProperty("done", out var doneEl) && doneEl.ValueKind == JsonValueKind.True)
                    done = true;
            }
            catch (JsonException)
            {
                // Иногда при отмене прилетает обрезанная строка — просто игнорируем
                continue;
            }

            if (!string.IsNullOrEmpty(tokenText))
                yield return tokenText;

            if (done)
                yield break;
        }
    }
}
