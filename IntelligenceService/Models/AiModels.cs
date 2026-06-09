namespace IntelligenceService.Models;

// The request body for your http://0.0.0.0:8000/predict
public class PredictionRequest
{
    public double Time { get; set; }
    public double V1 { get; set; }
    public double V2 { get; set; }
    public double V3 { get; set; }
    public double V4 { get; set; }
    public double V5 { get; set; }
    public double V6 { get; set; }
    public double V7 { get; set; }
    public double V8 { get; set; }
    public double V9 { get; set; }
    public double V10 { get; set; }
    public double V11 { get; set; }
    public double V12 { get; set; }
    public double V13 { get; set; }
    public double V14 { get; set; }
    public double V15 { get; set; }
    public double V16 { get; set; }
    public double V17 { get; set; }
    public double V18 { get; set; }
    public double V19 { get; set; }
    public double V20 { get; set; }
    public double V21 { get; set; }
    public double V22 { get; set; }
    public double V23 { get; set; }
    public double V24 { get; set; }
    public double V25 { get; set; }
    public double V26 { get; set; }
    public double V27 { get; set; }
    public double V28 { get; set; }
    public double Amount { get; set; }
}

// The response body from your API
public class PredictionResponse
{
    public string transaction_id { get; set; }
    public double fraud_probability { get; set; }
    public string decision { get; set; }
    public string risk_level { get; set; }
    public double response_time_ms { get; set; }
}