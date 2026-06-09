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

// using Confluent.Kafka;
// using Newtonsoft.Json;
// using IntelligenceService.Models;
// using System.Text;

// var bootstrapServers  = "localhost:9092";
// var rawTopic = "raw_transactions";
// var scoredTopic = "scored_transactions";
// var alertThreshold = 80.0;

// const string hfApiKey = "hf_your_token_here"; // Replace with your actual token
// const string modelUrl = "https://api-inference.huggingface.co/models/cardiffnlp/twitter-roberta-base-sentiment"; // Example model or your specific fraud model

// // Configuration
// const string apiEndpoint = "http://localhost:8000/predict"; 
// const double tau = 80.0; // Threshold from your Chapter 3


// var config = new ConsumerConfig
// {
//     BootstrapServers = bootstrapServers,
//     GroupId = "intelligence-group",
//     AutoOffsetReset = AutoOffsetReset.Earliest
// };

// var  producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

// using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
// using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

// consumer.Subscribe(rawTopic);

// Console.WriteLine("Intelligence Layer Running: Listening for transactions....");

// while (true)
// {
//     var result = consumer.Consume();
//     var rawData = JsonConvert.DeserializeObject<dynamic>(result.Message.Value);

//     // 1. Prepare the payload for XGBoost
//     var aiRequest = new PredictionRequest {
//         Time = 100000, // You can randomize these or pull from a feature store
//         Amount = (double)rawData.Amount,
//         V1 = -0.1, 
//         V2 = 0.05,
//         V3: 0.2,
//         V4: -0.02,
//         V5: -0.522, V6: -1.426, V7: -2.537, V8: 1.391,
//         V9: -2.770, V10: -2.772, V11: 3.202, V12: -2.899,
//         V13: -0.595, V14: -4.289, V15: 0.389, V16: -1.140,
//         V17: -2.830, V18: -0.016, V19: 0.416, V20: 0.126,
//         V21: 0.517, V22: -0.035, V23: -0.465, V24: -0.018,
//         V25: -0.010, V26: -0.002, V27: -0.154, V28: -0.048,
//     };

//     // 2. Call the Local AI Service
//     var aiResponse = await CallXGBoostApi(aiRequest);

//     // 3. Map to your Framework's Scored Event
//     var scoredEvent = new ScoredTransaction {
//         TransactionId = rawData.TransactionId,
//         RiskScore = aiResponse.fraud_probability * 100, // Scale to 0-100
//         IsFlagged = (aiResponse.fraud_probability * 100) > tau,
//         ProcessedAt = DateTime.UtcNow
//     };

//     // 4. Produce to scored_transactions
//     await producer.ProduceAsync("scored_transactions", 
//         new Message<Null, string> { Value = JsonConvert.SerializeObject(scoredEvent) });

//     Console.WriteLine($"[AI] ID: {scoredEvent.TransactionId} | Prob: {aiResponse.fraud_probability:P2} | Flagged: {scoredEvent.IsFlagged}");
// }

// static async Task<PredictionResponse> CallXGBoostApi(PredictionRequest request)
// {
//     using var client = new HttpClient();
//     var json = JsonConvert.SerializeObject(request);
//     var content = new StringContent(json, Encoding.UTF8, "application/json");

//     var response = await client.PostAsync(apiEndpoint, content);
//     var responseString = await response.Content.ReadAsStringAsync();
    
//     return JsonConvert.DeserializeObject<PredictionResponse>(responseString) 
//            ?? new PredictionResponse();
// }

// while (true) 
// {
//     try 
//     {
//         var result = consumer.Consume();
//         var transaction = JsonConvert.DeserializeObject<ScoredTransaction>(result.Message.Value);

//         if (transaction != null) 
//         {
//             // Placeholder for Hugging Face Inference
//             // In a real scenario, one would use HttpClient to POST to Hugging face here
//             transaction.RiskScore = MockAiInference(transaction.Amount);
//             // transaction.RiskScore = GetHuggingFaceScore(transaction.Amount);

//             // DECISION ENGINE (The Threshold Check)
//             transaction.IsFlagged = transaction.RiskScore > alertThreshold;

//             // PUBLISH to Stored Topic
//             var json = JsonConvert.SerializeObject(transaction);
//             await producer.ProduceAsync(scoredTopic, new Message<Null, string> { Value = json });

//             Console.WriteLine($"Processed: {transaction.TransactionId} | Score: {transaction.RiskScore} | Alert: {transaction.IsFlagged}");
//         }
//     }
//     catch (Exception ex) { Console.WriteLine($"Error: {ex.Message}"); }
// }

/*
NO LONGER A MOCK Inference:
2. Modify the main processing loop
while (true)
{
    var result = consumer.Consume();
    var transaction = JsonConvert.DeserializeObject<ScoredTransaction>(result.Message.Value);

    if (transaction != null)
    {
        // CALL REAL AI
        transaction.RiskScore = await GetActualRiskScore(transaction, hfApiKey, modelUrl);

        // DECISION ENGINE
        transaction.IsFlagged = transaction.RiskScore > alertThreshold;

        var json = JsonConvert.SerializeObject(transaction);
        await producer.ProduceAsync(scoredTopic, new Message<Null, string> { Value = json });

        Console.WriteLine($"[AI Scored] ID: {transaction.TransactionId} | Score: {transaction.RiskScore:F2} | Alert: {transaction.IsFlagged}");
    }
}

3. The Inference Method
static async Task<double> GetActualRiskScore(ScoredTransaction tx, string apiKey, string url)
{
    using var client = new HttpClient();
    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);

    // Prepare payload based on your methodology's data attributes
    var payload = new { inputs = $"Amount: {tx.Amount}, Sender: {tx.SenderId}" };
    var content = new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json");

    try
    {
        var response = await client.PostAsync(url, content);
        var responseString = await response.Content.ReadAsStringAsync();

        // Hugging Face Classification models usually return a nested list: [[{"label":"X", "score":0.9}]]
        dynamic? result = JsonConvert.DeserializeObject(responseString);
        
        // Extracting a score (Example logic: mapping label confidence to a 0-100 scale)
        double score = result?[0][0]?.score ?? 0.0;
        return score * 100; 
    }
    catch (Exception ex)
    {
        Console.WriteLine($"AI Inference Error: {ex.Message}");
        return 0.0; // Fallback
    }
}

*/

// The Previous Method for the AI Reference
// static async Task<double> GetHuggingFaceScore(decimal amount, string senderId, string apiKey)
// {
//     using var client = new HttpClient();
//     client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);

//     // Using a placeholder fraud detection model URL
//     string modelUrl = "https://api-inference.huggingface.co/models/fraud-detection-model-path";
    
//     var payload = new { inputs = $"Transaction of {amount} from {senderId}" };
//     var jsonPayload = JsonConvert.SerializeObject(payload);
    
//     try 
//     {
//         var response = await client.PostAsync(modelUrl, new StringContent(jsonPayload, Encoding.UTF8, "application/json"));
//         var responseBody = await response.Content.ReadAsStringAsync();
        
//         // Note: You will need to parse the specific JSON structure returned by your chosen model
//         // Usually it looks like: [[{"label":"FRAUD","score":0.99}, {"label":"LEGIT","score":0.01}]]
//         return 90.0; // Placeholder for the parsed score
//     }
//     catch { return 50.0; } // Fallback score
// }

// Simple logic: High amounts get higher risk scores for testing
// double MockAiInference(decimal amount) => amount > 1000 ? 85.5 : 12.0;

// Token: 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