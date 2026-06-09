namespace IngestionService.Models;

public class TransactionEvent 
{
    public string TransactionId { get; set; } = Guid.NewGuid().ToString();
    public string SenderId { get; set; } = string.Empty;
    public string RecipientId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime TimeStamp { get; set; } = DateTime.UtcNow;
}