using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;
using IngestionService.Models;
using System.Text.Json;

[ApiController]
[Route("api/v1/[controller]")]

public class TransactionController: ControllerBase
{
    private readonly IConfiguration _config;
    private readonly ProducerConfig _producerConfig;
    private readonly string _topic;

    public TransactionController(IConfiguration config) 
    {
        _config = config;
        _topic = _config["Kafka:Topic"] ?? "raw_transactions";
        _producerConfig = new ProducerConfig
        {
            BootstrapServers = _config["Kafka:BootstrapServers"],
            // Ensures the producer waits for Kafka to acknowledge the message
            Acks = Acks.All
        };
    }

    [HttpPost("ingest")]
    public async Task<IActionResult> IngestTransaction([FromBody] TransactionEvent transaction) 
    {
        // Simple Validation (Step 2 of Algorithm 1)
        if (transaction.Amount <= 0 || string.IsNullOrEmpty(transaction.SenderId))
        {
            return BadRequest(new { error = "Invalid transaction data: Amount must be > 0" });
        }

        using (var producer = new ProducerBuilder<Null, string>(_producerConfig).Build())
        {
            try {
                var messageValue = JsonSerializer.Serialize(transaction);

                // Step 2 (Cont.): PUBLISH event to "raw_transactions"
                var result = await producer.ProduceAsync(_topic, new Message<Null, string>
                {
                    Value = messageValue
                });

                return Ok(new 
                { 
                    status = "Event Streamed", 
                    id = transaction.TransactionId,
                    partition = result.Partition.Value,
                    offset = result.Offset.Value 
                });
            }
            catch (ProduceException<Null, string> e)
            {
                return StatusCode(500, new { error = $"Kafka delivery failed: {e.Error.Reason}" });
            }
        }
    }

}


    // public TransactionController(IConfiguration config) 
    // {
    //     _config = config;
    //     _producerConfig = new ProducerConfig
    //     {
    //         BootstrapServers = _config["Kafka:BootstrapServers"]
    //     };
    // }

    // [HttpPost("send")]
    // public async Task<IActionResult> PostTransaction([FromBody] TransactionEvent transaction)
    // {
    //     using (var producer = new ProducerBuilder<Null, string>(_producerConfig).Build())
    //     {
    //         var message = JsonSerializer.Serialize(transaction);

    //         await producer.ProduceAsync("raw_transactions", new Message<NUll, string>
    //         {
    //             Value = message
    //         });

    //         return Ok(new { status = "Transaction Streamed", id = transaction.TransactionId});
    //     }
    // }
// }