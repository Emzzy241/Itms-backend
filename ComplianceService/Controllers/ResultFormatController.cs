using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;
// using IngestionService.Models;
using System.Text.Json;

[ApiController]
[Route("api/v1/[controller]")]
public class ResultFormatController : ControllerBase
{
    [HttpGet("latest")]
    public IActionResult GetLatest()
    {
        return Ok(ComplianceStore.LatestEvents);
    }
}

// using Microsoft.AspNetCore.Mvc;
// using Confluent.Kafka;
// using IngestionService.Models;
// using System.Text.Json;

// [ApiController]
// [Route("api/v1/[controller]")]

// public class ResultFormatController: ControllerBase
// {
//     private readonly IConfiguration _config;
//     private readonly ProducerConfig _producerConfig;
//     private readonly string _topic;

//     public ResultFormatController(IConfiguration config) 
//     {
//         _config = config;
//         _topic = _config["Kafka:Topic"] ?? "raw_transactions";
//         _producerConfig = new ProducerConfig
//         {
//             BootstrapServers = _config["Kafka:BootstrapServers"],
//             Acks = Acks.All
//         }
//     }

//     [HttpGet("latest")]
//      public IActionResult GetLatest()
//     {
//         return Ok(ComplianceStore.LatestEvents);
//     }
// }