using System.Text.Json.Serialization;
using Lakepipe.Core.Streams;

namespace Lakepipe.Configuration.Models;

/// <summary>
/// Cache configuration for intelligent local caching.
/// </summary>
public record CacheConfig
{
    /// <summary>
    /// Enable caching.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Cache directory path.
    /// </summary>
    public string CacheDirectory { get; init; } = "/tmp/lakepipe_cache";

    /// <summary>
    /// Maximum cache size (e.g., "10GB", "1TB").
    /// </summary>
    public string MaxSize { get; init; } = "10GB";

    /// <summary>
    /// Compression type for cached data.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public Core.Streams.CompressionType Compression { get; init; } = Core.Streams.CompressionType.Zstd;

    /// <summary>
    /// Time-to-live in days for cached items.
    /// </summary>
    public int? TtlDays { get; init; } = 7;

    /// <summary>
    /// Only cache immutable datasets.
    /// </summary>
    public bool ImmutableOnly { get; init; } = true;

    /// <summary>
    /// Patterns for sources to include in caching.
    /// </summary>
    public List<string> IncludePatterns { get; init; } = new();

    /// <summary>
    /// Patterns for sources to exclude from caching.
    /// </summary>
    public List<string> ExcludePatterns { get; init; } = new();

    /// <summary>
    /// Enable memory cache tier.
    /// </summary>
    public bool EnableMemoryCache { get; init; } = true;

    /// <summary>
    /// Maximum memory cache size as percentage of total cache size.
    /// </summary>
    public int MemoryCachePercentage { get; init; } = 10;

    /// <summary>
    /// Cache eviction policy.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public CacheEvictionPolicy EvictionPolicy { get; init; } = CacheEvictionPolicy.Lru;
}

/// <summary>
/// Cache eviction policies.
/// </summary>
public enum CacheEvictionPolicy
{
    /// <summary>
    /// Least Recently Used.
    /// </summary>
    Lru,

    /// <summary>
    /// Least Frequently Used.
    /// </summary>
    Lfu,

    /// <summary>
    /// First In First Out.
    /// </summary>
    Fifo,

    /// <summary>
    /// Time-based eviction.
    /// </summary>
    Ttl
}