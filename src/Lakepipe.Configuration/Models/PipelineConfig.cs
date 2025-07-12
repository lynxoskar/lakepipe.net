using System.Text.Json.Serialization;
using Lakepipe.Core.Streams;

namespace Lakepipe.Configuration.Models;

/// <summary>
/// Main pipeline configuration.
/// </summary>
public record PipelineConfig
{
    /// <summary>
    /// Logging configuration.
    /// </summary>
    public LogConfig Log { get; init; } = new();

    /// <summary>
    /// Source configuration.
    /// </summary>
    public SourceConfig Source { get; init; } = new();

    /// <summary>
    /// Sink configuration.
    /// </summary>
    public SinkConfig Sink { get; init; } = new();

    /// <summary>
    /// Transform configuration.
    /// </summary>
    public TransformConfig Transform { get; init; } = new();

    /// <summary>
    /// Cache configuration.
    /// </summary>
    public CacheConfig Cache { get; init; } = new();

    /// <summary>
    /// Monitoring configuration.
    /// </summary>
    public MonitoringConfig Monitoring { get; init; } = new();

    /// <summary>
    /// Streaming configuration.
    /// </summary>
    public StreamingConfig Streaming { get; init; } = new();
}

/// <summary>
/// Logging configuration.
/// </summary>
public record LogConfig
{
    /// <summary>
    /// Log level (Verbose, Debug, Information, Warning, Error, Fatal).
    /// </summary>
    public string Level { get; init; } = "Information";

    /// <summary>
    /// Log format template.
    /// </summary>
    public string Format { get; init; } = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}";

    /// <summary>
    /// Optional log file path.
    /// </summary>
    public string? FilePath { get; init; }

    /// <summary>
    /// Enable console logging.
    /// </summary>
    public bool Console { get; init; } = true;

    /// <summary>
    /// Enable structured logging.
    /// </summary>
    public bool Structured { get; init; } = true;
}

/// <summary>
/// Source configuration.
/// </summary>
public record SourceConfig
{
    /// <summary>
    /// URI of the data source.
    /// </summary>
    public string Uri { get; init; } = "";

    /// <summary>
    /// Data format.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public DataFormat Format { get; init; } = DataFormat.Parquet;

    /// <summary>
    /// Compression type.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public CompressionType? Compression { get; init; }

    /// <summary>
    /// Schema configuration.
    /// </summary>
    public Dictionary<string, object>? Schema { get; init; }

    /// <summary>
    /// Cache configuration for this source.
    /// </summary>
    public CacheConfig? Cache { get; init; }

    /// <summary>
    /// Kafka-specific configuration.
    /// </summary>
    public KafkaConfig? Kafka { get; init; }
}

/// <summary>
/// Sink configuration.
/// </summary>
public record SinkConfig
{
    /// <summary>
    /// URI of the data sink.
    /// </summary>
    public string Uri { get; init; } = "";

    /// <summary>
    /// Data format.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public DataFormat Format { get; init; } = DataFormat.Parquet;

    /// <summary>
    /// Compression type.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public CompressionType? Compression { get; init; }

    /// <summary>
    /// Columns to partition by.
    /// </summary>
    public List<string>? PartitionBy { get; init; }

    /// <summary>
    /// Kafka-specific configuration.
    /// </summary>
    public KafkaConfig? Kafka { get; init; }
}

/// <summary>
/// Transform configuration.
/// </summary>
public record TransformConfig
{
    /// <summary>
    /// Transform engine to use.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public TransformEngine Engine { get; init; } = TransformEngine.Arrow;

    /// <summary>
    /// List of transform operations.
    /// </summary>
    public List<TransformOperation> Operations { get; init; } = new();

    /// <summary>
    /// User-defined function modules.
    /// </summary>
    public List<string>? UserFunctions { get; init; }
}

/// <summary>
/// Transform engines available.
/// </summary>
public enum TransformEngine
{
    Arrow,
    DuckDb,
    Custom
}

/// <summary>
/// Transform operation configuration.
/// </summary>
public record TransformOperation
{
    /// <summary>
    /// Operation type.
    /// </summary>
    public string Type { get; init; } = "";

    /// <summary>
    /// Operation-specific configuration.
    /// </summary>
    public Dictionary<string, object>? Config { get; init; }

    /// <summary>
    /// Parallelism level for this operation.
    /// </summary>
    public int? Parallelism { get; init; }
}

/// <summary>
/// Monitoring configuration.
/// </summary>
public record MonitoringConfig
{
    /// <summary>
    /// Enable metrics collection.
    /// </summary>
    public bool EnableMetrics { get; init; } = true;

    /// <summary>
    /// Memory threshold for alerts.
    /// </summary>
    public string MemoryThreshold { get; init; } = "8GB";

    /// <summary>
    /// Enable progress bar display.
    /// </summary>
    public bool ProgressBar { get; init; } = true;

    /// <summary>
    /// Metrics export interval.
    /// </summary>
    public TimeSpan MetricsInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// OpenTelemetry endpoint.
    /// </summary>
    public string? OtelEndpoint { get; init; }
}