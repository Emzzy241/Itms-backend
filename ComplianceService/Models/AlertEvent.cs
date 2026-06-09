namespace ComplianceService.Models
{
    public class AlertEvent
    {
        public string TransactionId { get; set; } = string.Empty;
        public string SenderId { get; set; } = string.Empty;
        public double RiskScore { get; set; }
        public string AlertLevel { get; set; } = "High"; // Based on Chapter 2 risk levels
        public decimal Amount { get; set; }
        public DateTime AlertGeneratedAt { get; set; } = DateTime.UtcNow;
    }
}