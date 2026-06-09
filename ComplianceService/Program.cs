using Confluent.Kafka;
using Newtonsoft.Json;
using ComplianceService.Models;

// const string bootstrapServers = "kafka:9092";
const string bootstrapServers = "localhost:9092";
const string inputTopic = "scored_transactions";
const string outputTopic = "alerts";

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = "compliance-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

consumer.Subscribe(inputTopic);
Console.WriteLine(">>> Compliance Layer Running: Monitoring for High-Risk Events...");

while (true)
{
    try
    {
        var result = consumer.Consume();
        Console.WriteLine($"DEBUG RAW JSON: {result.Message.Value}");

        // sending the above to the frontend since it has the values v1-v28 with which I seek.

        // Here we use 'dynamic' to read the incoming scored event
        dynamic? scoredTx = JsonConvert.DeserializeObject(result.Message.Value);

        if (scoredTx != null)
        {
            ComplianceStore.LatestEvents.Add(scoredTx);

            Console.WriteLine($"DEBUG RAW JSON: {result.Message.Value}");
        }
        // Use the TimeStamp we just ensured is being passed through
        if (scoredTx != null && scoredTx.TimeStamp != null)
        {
            DateTime ingestionTime = (DateTime)scoredTx.TimeStamp; 
            
            // Calculate latency: The time the Alert was generated MINUS the time it was ingested
            var latency = DateTime.UtcNow - ingestionTime;

            // Log the realistic value
            Console.WriteLine($"[METRIC] ID: {scoredTx.TransactionId} | Latency: {latency.TotalMilliseconds:F2}ms");
        }

        // THE COMPLIANCE FILTER
        if (scoredTx.IsFlagged == true)
        {
            // // --- LATENCY CALCULATION ---
            // // 1. Safe check for TimeStamp
            // // Use ?? (null-coalescing operator) to provide a fallback time if it's missing
            // DateTime ingestionTime = scoredTx.TimeStamp != null ? (DateTime)scoredTx.TimeStamp : DateTime.UtcNow;

            // // 2. Calculate Latency safely
            // var latency = DateTime.UtcNow - ingestionTime;

            // 3. Clean Print Statement (Avoids ambiguity)
            // string logMsg = string.Format("[METRIC] ID: {0} | Latency: {1:F2}ms", 
            //                             (string)scoredTx.TransactionId, 
            //                             latency.TotalMilliseconds);
            // Console.WriteLine(logMsg);

            // Log to console so you can copy/paste this into your thesis metrics
            // Console.WriteLine($"[METRIC] ID: {scoredTx.TransactionId} | Latency: {latency.TotalMilliseconds:F2}ms");
            
            var alert = new AlertEvent
            {
                TransactionId = scoredTx.TransactionId,
                SenderId = scoredTx.SenderId,
                RiskScore = scoredTx.RiskScore,
                AlertLevel = (double)scoredTx.RiskScore > 90 ? "CRITICAL" : "SUSPICIOUS",
                AlertGeneratedAt = DateTime.UtcNow // This records when the alert was created
            };

            var alertJson = JsonConvert.SerializeObject(alert);
            await producer.ProduceAsync(outputTopic, new Message<Null, string> { Value = alertJson });

            Console.WriteLine($"[ALERT GENERATED] TXN: {alert.TransactionId} | Level: {alert.AlertLevel} | Score: {alert.RiskScore}");

        }
    }
    catch (Exception ex) { Console.WriteLine($"Error: {ex.Message}"); }
}