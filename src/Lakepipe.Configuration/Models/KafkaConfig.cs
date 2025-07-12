using System.Text.Json.Serialization;

namespace Lakepipe.Configuration.Models;

/// <summary>
/// Kafka-specific configuration for streaming sources and sinks.
/// </summary>
public record KafkaConfig
{
    /// <summary>
    /// Kafka bootstrap servers.
    /// </summary>
    public List<string> BootstrapServers { get; init; } = new() { "localhost:9092" };

    /// <summary>
    /// Kafka topic name.
    /// </summary>
    public string Topic { get; init; } = "";

    /// <summary>
    /// Consumer group ID.
    /// </summary>
    public string? GroupId { get; init; }

    /// <summary>
    /// Auto offset reset policy.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public KafkaAutoOffsetReset AutoOffsetReset { get; init; } = KafkaAutoOffsetReset.Latest;

    /// <summary>
    /// Message serialization format.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public SerializationFormat Serialization { get; init; } = SerializationFormat.Json;

    /// <summary>
    /// Schema registry URL for Avro/Protobuf.
    /// </summary>
    public string? SchemaRegistryUrl { get; init; }

    /// <summary>
    /// Partition key field for producers.
    /// </summary>
    public string? PartitionKey { get; init; }

    /// <summary>
    /// Compression type.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public Core.Streams.CompressionType CompressionType { get; init; } = Core.Streams.CompressionType.Snappy;

    /// <summary>
    /// Producer batch size in bytes.
    /// </summary>
    public int BatchSize { get; init; } = 16384;

    /// <summary>
    /// Maximum records per poll for consumers.
    /// </summary>
    public int MaxPollRecords { get; init; } = 1000;

    /// <summary>
    /// Number of consumer threads.
    /// </summary>
    public int? ConsumerThreads { get; init; } = 2;

    /// <summary>
    /// Enable auto commit.
    /// </summary>
    public bool EnableAutoCommit { get; init; } = false;

    /// <summary>
    /// Security protocol.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public KafkaSecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>
    /// SASL mechanism.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public KafkaSaslMechanism? SaslMechanism { get; init; }

    /// <summary>
    /// SASL username.
    /// </summary>
    public string? SaslUsername { get; init; }

    /// <summary>
    /// SASL password.
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>
    /// SSL certificate location.
    /// </summary>
    public string? SslCertificateLocation { get; init; }

    /// <summary>
    /// SSL key location.
    /// </summary>
    public string? SslKeyLocation { get; init; }

    /// <summary>
    /// SSL CA location.
    /// </summary>
    public string? SslCaLocation { get; init; }

    /// <summary>
    /// Additional Kafka configuration properties.
    /// </summary>
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}

/// <summary>
/// Message serialization formats.
/// </summary>
public enum SerializationFormat
{
    /// <summary>
    /// JSON serialization.
    /// </summary>
    Json,

    /// <summary>
    /// Apache Avro serialization.
    /// </summary>
    Avro,

    /// <summary>
    /// Protocol Buffers serialization.
    /// </summary>
    Protobuf,

    /// <summary>
    /// Raw bytes.
    /// </summary>
    Raw
}


/// <summary>
/// Kafka auto offset reset options.
/// </summary>
public enum KafkaAutoOffsetReset
{
    Latest,
    Earliest,
    None
}

/// <summary>
/// Kafka security protocols.
/// </summary>
public enum KafkaSecurityProtocol
{
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl
}

/// <summary>
/// Kafka SASL mechanisms.
/// </summary>
public enum KafkaSaslMechanism
{
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi
}