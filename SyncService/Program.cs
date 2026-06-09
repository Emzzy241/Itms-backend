using Confluent.Kafka;
using Google.Cloud.Firestore;
using Newtonsoft.Json;

// Set the path to your credentials
Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "firebase-key.json");

// const string bootstrapServers = "kafka:9092";
const string bootstrapServers = "localhost:9092";
const string projectId = "itms-project-43973";

var db = FirestoreDb.Create(projectId);
var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = "sync-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(new[] { "scored_transactions", "alerts" });

Console.WriteLine(">>> Sync Service Running: Bridging Kafka to Firestore...");

while (true)
{
    try
    {
        var result = consumer.Consume();
        var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(result.Message.Value);
        
        if (data != null && data.ContainsKey("TransactionId"))
        {
            string collection = result.Topic == "alerts" ? "alerts" : "transactions";
            string docId = data["TransactionId"].ToString() ?? Guid.NewGuid().ToString();

            // Explicitly cast or convert values if needed for Firestore
            await db.Collection(collection)
                    .Document(docId)
                    .SetAsync(data);

            Console.WriteLine($"[SYNCED] Event {docId.Substring(0,8)} saved to Firestore '{collection}' collection.");
        }
    }
    catch (Exception ex) 
    { 
        Console.WriteLine($"Sync Error: {ex.Message}"); 
        // Optional: Wait a bit before retrying if it's a network error
        Thread.Sleep(2000); 
    }
}