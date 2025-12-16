using System.Text;
using System.Threading.Channels;

namespace VoiceAgent.Tts;

public sealed class SentenceDispatchBuffer
{
    private readonly StringBuilder _sb = new();
    private readonly Channel<string> _out = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    private readonly int _minSentenceLength;

    public SentenceDispatchBuffer(int minSentenceLength = 30)
    {
        _minSentenceLength = Math.Max(1, minSentenceLength);
    }

    public void AppendToken(string token)
    {
        if (string.IsNullOrEmpty(token))
            return;

        _sb.Append(token);

        // сплит по . ! ? … \n
        while (TryExtractSentence(out var s))
            _out.Writer.TryWrite(s);
    }

    public async IAsyncEnumerable<string> GetSentencesAsync(CancellationToken ct)
    {
        while (await _out.Reader.WaitToReadAsync(ct))
        {
            while (_out.Reader.TryRead(out var s))
                yield return s;
        }
    }

    public void Complete()
    {
        // финальный хвост (если есть)
        var tail = _sb.ToString().Trim();
        if (!string.IsNullOrWhiteSpace(tail))
            _out.Writer.TryWrite(tail);

        _sb.Clear();
        _out.Writer.TryComplete();
    }

    public void Reset()
    {
        _sb.Clear();
        while (_out.Reader.TryRead(out _)) { }
        // канал НЕ закрываем — буфер может использоваться дальше
    }

    private bool TryExtractSentence(out string sentence)
    {
        sentence = "";

        if (_sb.Length < _minSentenceLength)
            return false;

        var text = _sb.ToString();

        int idx = FindSentenceBoundary(text);
        if (idx < 0)
            return false;

        var part = text[..(idx + 1)].Trim();
        var rest = text[(idx + 1)..];

        if (part.Length < _minSentenceLength && !part.EndsWith("\n"))
            return false;

        sentence = NormalizeSpaces(part);
        _sb.Clear();
        _sb.Append(rest);

        return !string.IsNullOrWhiteSpace(sentence);
    }

    private static int FindSentenceBoundary(string s)
    {
        // берём ближайшую "концовку предложения"
        // точка/воскл/вопр/многоточие/перевод строки
        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            if (c == '.' || c == '!' || c == '?' || c == '\n' || c == '…' || c == ',')
                return i;
        }
        return -1;
    }

    private static string NormalizeSpaces(string s)
    {
        // минимальная нормализация
        return s.Replace("\r", "").Trim();
    }
}
