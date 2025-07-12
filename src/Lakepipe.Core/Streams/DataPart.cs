namespace Lakepipe.Core.Streams;

/// <summary>
/// Represents a unit of data flowing through the pipeline.
/// </summary>
public record DataPart
{
    /// <summary>
    /// The actual data payload.
    /// </summary>
    public object Data { get; init; } = null!;

    /// <summary>
    /// Metadata associated with this data part.
    /// </summary>
    public DataPartMetadata Metadata { get; init; } = new();

    /// <summary>
    /// Information about the data source.
    /// </summary>
    public SourceInfo SourceInfo { get; init; } = new();

    /// <summary>
    /// Schema information for the data.
    /// </summary>
    public SchemaInfo? Schema { get; init; }

    /// <summary>
    /// Cache metadata for intelligent caching.
    /// </summary>
    public CacheMetadata CacheMetadata { get; init; } = new();
}

/// <summary>
/// Metadata for a data part.
/// </summary>
public record DataPartMetadata
{
    /// <summary>
    /// Unique identifier for this data part.
    /// </summary>
    public string Id { get; init; } = Guid.NewGuid().ToString();

    /// <summary>
    /// Source system or component.
    /// </summary>
    public string? Source { get; init; }

    /// <summary>
    /// Timestamp when this data part was created.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Number of records/messages in this part.
    /// </summary>
    public int MessageCount { get; init; }

    /// <summary>
    /// Position in the stream (for ordered streams).
    /// </summary>
    public long? StreamPosition { get; init; }

    /// <summary>
    /// Topic name for Kafka sources.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>
    /// Partition offsets for Kafka sources.
    /// </summary>
    public Dictionary<int, long>? PartitionOffsets { get; init; }

    /// <summary>
    /// Hash of any transformations applied.
    /// </summary>
    public string? TransformHash { get; init; }

    /// <summary>
    /// Indicates if this data was retrieved from cache.
    /// </summary>
    public bool CacheHit { get; init; }

    /// <summary>
    /// Indicates if this data was stored in cache.
    /// </summary>
    public bool CacheStored { get; init; }

    /// <summary>
    /// Custom metadata properties.
    /// </summary>
    public Dictionary<string, object> Properties { get; init; } = new();
}

/// <summary>
/// Information about the data source.
/// </summary>
public record SourceInfo
{
    /// <summary>
    /// URI of the data source.
    /// </summary>
    public string Uri { get; init; } = "";

    /// <summary>
    /// Format of the data.
    /// </summary>
    public DataFormat Format { get; init; }

    /// <summary>
    /// Compression type if applicable.
    /// </summary>
    public CompressionType? Compression { get; init; }
}

/// <summary>
/// Schema information for structured data.
/// </summary>
public record SchemaInfo
{
    /// <summary>
    /// Schema type (e.g., "arrow", "avro", "json-schema").
    /// </summary>
    public string Type { get; init; } = "";

    /// <summary>
    /// Schema definition or reference.
    /// </summary>
    public object? Schema { get; init; }

    /// <summary>
    /// Version of the schema.
    /// </summary>
    public string? Version { get; init; }
}

/// <summary>
/// Metadata for cache management.
/// </summary>
public record CacheMetadata
{
    /// <summary>
    /// Indicates if the data is immutable.
    /// </summary>
    public bool IsImmutable { get; init; }

    /// <summary>
    /// ETag for cache validation.
    /// </summary>
    public string? ETag { get; init; }

    /// <summary>
    /// Last modified timestamp.
    /// </summary>
    public DateTime? LastModified { get; init; }

    /// <summary>
    /// Time-to-live in seconds.
    /// </summary>
    public int? TtlSeconds { get; init; }

    /// <summary>
    /// Custom cache metadata.
    /// </summary>
    public Dictionary<string, string> CustomMetadata { get; init; } = new();
}

/// <summary>
/// Data formats supported by the pipeline.
/// </summary>
public enum DataFormat
{
    Parquet,
    Csv,
    Json,
    Avro,
    Arrow,
    Kafka,
    DuckDb,
    Iceberg,
    Delta,
    Custom
}

/// <summary>
/// Compression types supported.
/// </summary>
public enum CompressionType
{
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
    Brotli
}