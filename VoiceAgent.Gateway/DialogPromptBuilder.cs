using System.Text;

namespace VoiceAgent.Dialog;

public sealed class DialogPromptBuilder
{
    private readonly string _systemPrompt;
    private readonly int _maxTurns;
    private readonly int _maxChars;

    public DialogPromptBuilder(string systemPrompt, int maxTurns = 10, int maxChars = 12_000)
    {
        _systemPrompt = systemPrompt ?? "";
        _maxTurns = Math.Max(1, maxTurns);
        _maxChars = Math.Max(1_000, maxChars);
    }

    /// <summary>
    /// Собирает prompt для LLM:
    /// SYSTEM + последние N turn'ов (только то, что пользователь реально слышал) + текущий ASR_FINAL. 
    /// </summary>
    public string Build(IReadOnlyList<TurnState> history, string currentUserText)
    {
        currentUserText ??= "";

        var sb = new StringBuilder(8_192);

        // 1) SYSTEM
        sb.AppendLine("SYSTEM:");
        sb.AppendLine(_systemPrompt.Trim());
        sb.AppendLine();

        // 2) DIALOG
        sb.AppendLine("DIALOG:");

        // Берём последние N turn'ов
        var start = Math.Max(0, history.Count - _maxTurns);
        for (int i = start; i < history.Count; i++)
        {
            var t = history[i];

            // USER: берём только финальный текст, а если не финализировался (прерван) — берём то, что есть.
            var userText = t.User.AsrText?.Trim() ?? "";
            if (!string.IsNullOrWhiteSpace(userText))
            {
                // Можно отметить прерывание, но без лишних спецсимволов
                if (t.User.WasInterrupted && !t.User.IsFinal)
                    sb.Append("Пользователь (прерван): ");
                else
                    sb.Append("Пользователь: ");

                sb.AppendLine(Norm(userText));
            }

            // ASSISTANT: ВАЖНО — используем Spoken (то, что реально произнесли), иначе LLM "помнит" то, что юзер не слышал.
            var assistantSpoken = t.Assistant.TtsSpokenText?.Trim() ?? "";
            if (!string.IsNullOrWhiteSpace(assistantSpoken))
            {
                if (t.Assistant.WasInterrupted)
                    sb.Append("Ассистент (прерван): ");
                else
                    sb.Append("Ассистент: ");

                sb.AppendLine(Norm(assistantSpoken));
            }

            // пустая строка между turn'ами
            if (!string.IsNullOrWhiteSpace(userText) || !string.IsNullOrWhiteSpace(assistantSpoken))
                sb.AppendLine();
        }

        // 3) CURRENT USER INPUT
        // Важно: даже если история пустая — всегда добавляем текущего пользователя
        sb.Append("Пользователь: ");
        sb.AppendLine(Norm(currentUserText.Trim()));
        sb.AppendLine("Ассистент:");

        // 4) Ограничение по длине: если разрослось — обрезаем сверху (после SYSTEM)
        var text = sb.ToString();
        if (text.Length <= _maxChars)
            return text;

        return TrimToMaxChars(text, _maxChars);
    }

    private static string Norm(string s)
    {
        // минимальная нормализация под TTS: убираем лишние переносы и табы
        s = s.Replace("\r", " ").Replace("\n", " ").Replace("\t", " ");
        while (s.Contains("  "))
            s = s.Replace("  ", " ");
        return s.Trim();
    }

    private static string TrimToMaxChars(string text, int maxChars)
    {
        // Сохраняем начало SYSTEM, режем середину/историю так, чтобы конец (текущий запрос) остался
        if (text.Length <= maxChars) return text;

        // стратегия простая и надёжная: оставляем хвост, но SYSTEM должен остаться
        // найдём конец SYSTEM блока (первый двойной перенос после SYSTEM)
        var sysEnd = text.IndexOf("\n\n", StringComparison.Ordinal);
        if (sysEnd < 0) sysEnd = Math.Min(300, text.Length);

        var systemPart = text.Substring(0, sysEnd).TrimEnd();

        // оставляем хвост так, чтобы уложиться
        var tailBudget = maxChars - systemPart.Length - 2;
        if (tailBudget < 200) tailBudget = 200;

        var tail = text.Length > tailBudget ? text.Substring(text.Length - tailBudget) : text;

        return systemPart + "\n\n" + tail;
    }
}
