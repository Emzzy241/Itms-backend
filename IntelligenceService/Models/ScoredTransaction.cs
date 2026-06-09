namespace IntelligenceService.Models;

public class ScoredTransaction
{
    public string TransactionId { get; set; } = string.Empty;
    public string SenderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public double RiskScore { get; set; }
    public bool IsFlagged { get; set; }
    public DateTime TimeStamp { get; set; }
    public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;
    public Object V_Vectors { get; set; }
}