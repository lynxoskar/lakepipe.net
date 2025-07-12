namespace Lakepipe.Core.Processors;

/// <summary>
/// Metrics for monitoring processor performance.
/// </summary>
public record ProcessingMetrics
{
    /// <summary>
    /// Total number of items processed.
    /// </summary>
    public long ItemsProcessed { get; init; }

    /// <summary>
    /// Total number of bytes processed.
    /// </summary>
    public long BytesProcessed { get; init; }

    /// <summary>
    /// Number of errors encountered.
    /// </summary>
    public long ErrorCount { get; init; }

    /// <summary>
    /// Current throughput in items per second.
    /// </summary>
    public double ItemsPerSecond { get; init; }

    /// <summary>
    /// Current throughput in bytes per second.
    /// </summary>
    public double BytesPerSecond { get; init; }

    /// <summary>
    /// Average processing latency.
    /// </summary>
    public TimeSpan AverageLatency { get; init; }

    /// <summary>
    /// 99th percentile latency.
    /// </summary>
    public TimeSpan P99Latency { get; init; }

    /// <summary>
    /// Current CPU usage percentage.
    /// </summary>
    public double CpuUsage { get; init; }

    /// <summary>
    /// Current memory usage in bytes.
    /// </summary>
    public long MemoryUsage { get; init; }

    /// <summary>
    /// Total available memory in bytes.
    /// </summary>
    public long TotalMemory { get; init; }

    /// <summary>
    /// Number of items currently in flight.
    /// </summary>
    public int ItemsInFlight { get; init; }

    /// <summary>
    /// Timestamp when metrics were collected.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}