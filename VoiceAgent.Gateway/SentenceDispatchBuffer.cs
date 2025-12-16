using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace VoiceAgent.Tts;

/// <summary>
/// Буфер, который принимает поток токенов от LLM,
/// режет их на законченные предложения
/// и отдаёт в виде асинхронной очереди.
/// Поддерживает barge-in (Reset).
/// </summary>
public sealed class SentenceDispatchBuffer
{
    private readonly Channel<string> _channel;
    private readonly StringBuilder _buffer = new();
    private readonly object _lock = new();

    private readonly int _minSentenceLength;
    private bool _completed;

    // конец фразы: . ! ? … или перевод строки
    private static readonly Regex SentenceEndRegex =
        new(@"[.!?\n…]+", RegexOptions.Compiled);

    public SentenceDispatchBuffer(int minSentenceLength = 30)
    {
        _minSentenceLength = minSentenceLength;

        _channel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <summary>
    /// Добавить токен от LLM (streaming)
    /// </summary>
    public void AppendToken(string token)
    {
        if (string.IsNullOrEmpty(token))
            return;

        lock (_lock)
        {
            if (_completed)
                return;

            _buffer.Append(token);
            TryFlushSentences();
        }
    }

    /// <summary>
    /// Сообщить, что LLM закончил генерацию
    /// </summary>
    public void Complete()
    {
        lock (_lock)
        {
            if (_completed)
                return;

            _completed = true;

            var tail = _buffer.ToString().Trim();
            _buffer.Clear();

            if (tail.Length >= _minSentenceLength)
            {
                _channel.Writer.TryWrite(tail);
            }

            _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Barge-in: сбросить всё немедленно
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _buffer.Clear();
            _completed = true;

            _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Асинхронный поток готовых предложений
    /// </summary>
    public IAsyncEnumerable<string> GetSentencesAsync(
        CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }

    // ------------------------
    // INTERNAL
    // ------------------------

    private void TryFlushSentences()
    {
        var text = _buffer.ToString();

        var matches = SentenceEndRegex.Matches(text);
        if (matches.Count == 0)
            return;

        int lastFlushIndex = -1;

        foreach (Match match in matches)
        {
            int end = match.Index + match.Length;
            var sentence = text[..end].Trim();

            if (sentence.Length < _minSentenceLength)
                continue;

            _channel.Writer.TryWrite(sentence);
            lastFlushIndex = end;
        }

        if (lastFlushIndex > 0)
        {
            _buffer.Remove(0, lastFlushIndex);
        }
    }
}
