using System.Text.Json.Serialization;

namespace VoiceAgent.Dialog;

public sealed class TurnState
{
    public long TurnId { get; }

    public UserUtterance User { get; } = new();
    public AssistantUtterance Assistant { get; } = new();

    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

    public TurnState(long turnId)
    {
        TurnId = turnId;
    }

    // =========================
    // USER
    // =========================
    public sealed class UserUtterance
    {
        public string AsrText { get; private set; } = "";
        public bool IsFinal { get; private set; }
        public bool WasInterrupted { get; private set; }

        public DateTimeOffset StartedAt { get; private set; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? EndedAt { get; private set; }

        public void AppendPartial(string text)
        {
            AsrText = text;
        }

        public void MarkFinal(string text)
        {
            AsrText = text;
            IsFinal = true;
            EndedAt = DateTimeOffset.UtcNow;
        }

        public void MarkInterrupted()
        {
            WasInterrupted = true;
            EndedAt = DateTimeOffset.UtcNow;
        }
    }

    // =========================
    // ASSISTANT
    // =========================
    public sealed class AssistantUtterance
    {
        public string LlmTextFull { get; private set; } = "";
        public string TtsSpokenText { get; private set; } = "";

        public bool WasInterrupted { get; private set; }

        [JsonIgnore]
        public bool HasStarted => LlmTextFull.Length > 0;

        public void AppendLlmToken(string token)
        {
            LlmTextFull += token;
        }

        public void AppendSpokenSentence(string sentence)
        {
            if (!string.IsNullOrWhiteSpace(sentence))
            {
                if (TtsSpokenText.Length > 0)
                    TtsSpokenText += " ";

                TtsSpokenText += sentence.Trim();
            }
        }

        public void MarkInterrupted()
        {
            WasInterrupted = true;
        }
    }
}
