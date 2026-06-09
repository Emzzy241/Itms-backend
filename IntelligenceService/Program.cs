using System.Text;
using Newtonsoft.Json;
using IntelligenceService.Models;
using Confluent.Kafka;

// 1. Configuration
// const string bootstrapServers = "kafka:9092";
const string bootstrapServers = "localhost:9092";
const string rawTopic = "raw_transactions";
const string scoredTopic = "scored_transactions";
const string aiEndpoint = "http://localhost:8000/predict"; 
const double tau = 80.0; // Threshold value 

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = "intelligence-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

consumer.Subscribe(rawTopic);
Console.WriteLine(">>> Intelligence Layer Running: Awaiting Events from Kafka...");

var userProfiles = new Dictionary<string, double[]>()
{
    // "Legitimate" Profile (USER-999) - Based on the Python-verified non-fraud data
    { "USER-999", new double[] { 
        -0.6000, 1.6868, -0.6437, -0.6327, 0.9224, -0.1943, 0.2157, -2.5827, -0.0950, -0.7569, 
        -0.4850, 0.7488, 1.2273, -1.1877, -0.3837, 0.2329, 0.5242, -0.4080, -0.3091, -0.5443, 
        2.0367, -1.4742, 0.4294, 0.4927, -0.4059, 0.1029, -0.0448, 0.0460 } 
    },
    // "High-Risk" Profile (USER-666) - Based on your Python-verified fraud data
    { "USER-666", new double[] { 
        -0.1, 0.05, 0.2, -0.02, -0.522, -1.426, -2.537, 1.391, -2.770, -2.772, 
        3.202, -2.899, -0.595, -4.289, 0.389, -1.140, -2.830, -0.016, 0.416, 0.126, 
        0.517, -0.035, -0.465, -0.018, -0.010, -0.002, -0.154, -0.048 } 
    }
};

while (true)
{
    try
    {
        var result = consumer.Consume();
        // Dynamic parse to handle the Ingestion Layer's format
        var rawData = JsonConvert.DeserializeObject<dynamic>(result.Message.Value);

        if (rawData != null)
    {
        string sender = rawData.SenderId ?? "DEFAULT";
        // double currentAmount = (double)rawData.Amount;
        // PredictionRequest aiRequest;

        // TEST LOGIC: If amount is 1.29, use the "Legitimate" features you found
        // LOOKUP: Get the profile for this user, or default to neutral zeros if not found
        if (!userProfiles.TryGetValue(sender, out double[]? features))
        {
            features = new double[28]; // Neutral fallback
        }

        var aiRequest = new PredictionRequest {
            Time = DateTime.Now.TimeOfDay.TotalSeconds,
            Amount = (double)rawData.Amount,
            V1 = features[0], V2 = features[1], V3 = features[2], V4 = features[3], V5 = features[4],
            V6 = features[5], V7 = features[6], V8 = features[7], V9 = features[8], V10 = features[9],
            V11 = features[10], V12 = features[11], V13 = features[12], V14 = features[13], V15 = features[14],
            V16 = features[15], V17 = features[16], V18 = features[17], V19 = features[18], V20 = features[19],
            V21 = features[20], V22 = features[21], V23 = features[22], V24 = features[23], V25 = features[24],
            V26 = features[25], V27 = features[26], V28 = features[27]
        };

            // Call the local AI API
            var aiResponse = await CallXGBoostApi(aiRequest, aiEndpoint);

            // Map results to the framework's mathematical model (S > tau)
            var scoredEvent = new ScoredTransaction {
                TransactionId = rawData.TransactionId ?? Guid.NewGuid().ToString(),
                SenderId = rawData.SenderId ?? "Unknown",
                Amount = rawData.Amount,
                RiskScore = aiResponse.fraud_probability * 100, 
                IsFlagged = (aiResponse.fraud_probability * 100) > tau,
                TimeStamp = rawData.TimeStamp, // Preserve the original birth time
                ProcessedAt = DateTime.UtcNow,
                V_Vectors = aiRequest
            };
            // Console.WriteLine(scoredEvent);

            // Produce enriched event to the next pipe
            var jsonOut = JsonConvert.SerializeObject(scoredEvent);
            await producer.ProduceAsync(scoredTopic, new Message<Null, string> { Value = jsonOut });

            Console.WriteLine($"[AI SCORED] ID: {scoredEvent.TransactionId.Substring(0,8)} | Score: {scoredEvent.RiskScore:F2} | Flagged: {scoredEvent.IsFlagged}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($">> Error: {ex.Message}");
    }
}

// 3. Helper Methods (Must be at the bottom in Top-Level Statements)
static async Task<PredictionResponse> CallXGBoostApi(PredictionRequest request, string url)
{
    using var client = new HttpClient();
    var json = JsonConvert.SerializeObject(request);
    var content = new StringContent(json, Encoding.UTF8, "application/json");

    try 
    {
        var response = await client.PostAsync(url, content);
        var responseString = await response.Content.ReadAsStringAsync();
        return JsonConvert.DeserializeObject<PredictionResponse>(responseString) ?? new PredictionResponse();
    }
    catch 
    {
        return new PredictionResponse { fraud_probability = 0.0 };
    }
}
