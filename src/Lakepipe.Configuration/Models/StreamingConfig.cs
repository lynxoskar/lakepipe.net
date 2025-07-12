namespace Lakepipe.Configuration.Models;

/// <summary>
/// Streaming configuration for TPL Dataflow and continuous processing.
/// </summary>
public record StreamingConfig
{
    /// <summary>
    /// Enable streaming mode.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Enable CPU affinity for optimal cache locality.
    /// </summary>
    public bool EnableCpuAffinity { get; init; } = true;

    /// <summary>
    /// CPU cores assigned to source processing.
    /// </summary>
    public int[] SourceCpuAffinity { get; init; } = { 0, 1 };

    /// <summary>
    /// CPU cores assigned to transform operations.
    /// Can be array of arrays for multiple transform stages.
    /// </summary>
    public int[][]? TransformCpuAffinity { get; init; }

    /// <summary>
    /// CPU cores assigned to sink operations.
    /// </summary>
    public int[] SinkCpuAffinity { get; init; } = { 2, 3 };

    /// <summary>
    /// Dataflow bounded capacity for backpressure - source buffer.
    /// </summary>
    public int SourceBufferSize { get; init; } = 1000;

    /// <summary>
    /// Dataflow bounded capacity for backpressure - transform buffer.
    /// </summary>
    public int TransformBufferSize { get; init; } = 5000;

    /// <summary>
    /// Dataflow bounded capacity for backpressure - sink buffer.
    /// </summary>
    public int SinkBufferSize { get; init; } = 1000;

    /// <summary>
    /// Source parallelism level.
    /// </summary>
    public int SourceParallelism { get; init; } = 2;

    /// <summary>
    /// Default parallelism for transforms.
    /// </summary>
    public int DefaultParallelism { get; init; } = 4;

    /// <summary>
    /// Sink parallelism level.
    /// </summary>
    public int SinkParallelism { get; init; } = 2;

    /// <summary>
    /// Batch size for sink operations.
    /// </summary>
    public int SinkBatchSize { get; init; } = 1000;

    /// <summary>
    /// Batch timeout for sink operations.
    /// </summary>
    public TimeSpan SinkBatchTimeout { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum messages in flight across the pipeline.
    /// </summary>
    public int MaxMessagesInFlight { get; init; } = 10000;

    /// <summary>
    /// Maximum memory per pipeline in MB.
    /// </summary>
    public int MaxMemoryPerPipelineMB { get; init; } = 4096;

    /// <summary>
    /// Enable detailed metrics collection.
    /// </summary>
    public bool EnableDetailedMetrics { get; init; } = true;

    /// <summary>
    /// Checkpoint interval for stateful processing.
    /// </summary>
    public TimeSpan CheckpointInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Enable automatic backpressure handling.
    /// </summary>
    public bool EnableAutoBackpressure { get; init; } = true;

    /// <summary>
    /// Backpressure configuration.
    /// </summary>
    public BackpressureConfig Backpressure { get; init; } = new();
}

/// <summary>
/// Backpressure configuration for adaptive flow control.
/// </summary>
public record BackpressureConfig
{
    /// <summary>
    /// Initial concurrency limit.
    /// </summary>
    public int InitialLimit { get; init; } = 100;

    /// <summary>
    /// Minimum concurrency limit.
    /// </summary>
    public int MinLimit { get; init; } = 10;

    /// <summary>
    /// Maximum concurrency limit.
    /// </summary>
    public int MaxLimit { get; init; } = 1000;

    /// <summary>
    /// High pressure threshold (0.0 - 1.0).
    /// </summary>
    public double HighPressureThreshold { get; init; } = 0.8;

    /// <summary>
    /// Low pressure threshold (0.0 - 1.0).
    /// </summary>
    public double LowPressureThreshold { get; init; } = 0.3;

    /// <summary>
    /// Ratio to decrease limit under high pressure.
    /// </summary>
    public double DecreaseRatio { get; init; } = 0.8;

    /// <summary>
    /// Ratio to increase limit under low pressure.
    /// </summary>
    public double IncreaseRatio { get; init; } = 1.2;

    /// <summary>
    /// Interval for adapting limits.
    /// </summary>
    public TimeSpan AdaptInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Target latency in milliseconds.
    /// </summary>
    public double TargetLatencyMs { get; init; } = 100;
}