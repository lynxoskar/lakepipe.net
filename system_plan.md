# Lakepipe.NET System Architecture Plan

## Executive Summary

Lakepipe.NET is a high-performance, functional data pipeline library built on .NET 10, designed for batch and streaming transformations across multiple lakehouse formats. Leveraging railway-oriented programming with zero-allocation operations, it provides unified access to DuckDB, S3, Iceberg, Parquet, CSV, Kafka streams, and Apache Arrow data sources. Features intelligent local caching for immutable datasets and real-time Kafka integration with best-in-class .NET performance.

## Reference Architecture

- **DuckDB.NET**: https://github.com/Giorgi/DuckDB.NET
- **Apache Arrow C#**: https://github.com/apache/arrow/tree/main/csharp
- **Confluent Kafka**: https://github.com/confluentinc/confluent-kafka-dotnet
- **Style Guide**: Railway-oriented programming with functional composition using Language.Ext

## 1. Core Architecture Overview

### 1.1 Stream-Based Processing System

Lakepipe.NET implements an async stream-based architecture where:
- All data flows through `IAsyncEnumerable<DataPart>` streams
- Processors are composable using functional operators and LINQ
- Zero-allocation operations between Apache Arrow, DuckDB, and native memory
- Automatic concurrent execution with `System.Threading.Channels`
- Intelligent local caching using hybrid memory/disk storage
- Real-time Kafka streaming with Confluent.Kafka

```csharp
// Core pipeline composition
var source = new S3ParquetSource(config);
var cache = new LocalCache(config);  // Intelligent caching layer
var transform = new UserTransform(config);
var kafkaSink = new KafkaSink(config);  // Kafka streaming sink

// Pipeline composition using functional operators
var pipeline = source
    .Then(cache)
    .Then(transform)
    .Then(kafkaSink);

// Execute with streaming
await foreach (var result in pipeline.ProcessAsync(streamConfig))
{
    await HandleResultAsync(result);
}
```

### 1.2 Technology Stack

**Core Data Stack (Zero-Allocation Focus)**:
- **.NET 10**: Latest runtime with enhanced performance features
- **Apache Arrow C# 16+**: Zero-copy columnar data operations
- **DuckDB.NET 1.0+**: Embedded analytical database with SQL support
- **Parquet.NET 5+**: Native .NET Parquet implementation
- **System.IO.Pipelines**: High-performance I/O with minimal allocations

**Streaming & Caching Libraries**:
- **Confluent.Kafka 2.5+**: High-performance Kafka client
- **Microsoft.Extensions.Caching.Memory**: In-memory caching
- **Akavache 10+**: Persistent key-value store for disk caching
- **FASTER 2.6+**: Microsoft's hybrid log-structured key-value store

**Supporting Libraries**:
- **Serilog 4+**: Structured logging with enrichers
- **Spectre.Console 0.49+**: Beautiful console output and CLI
- **Microsoft.Extensions.Configuration 10+**: Configuration with environment support
- **System.Text.Json 10+**: High-performance JSON with source generators
- **Language.Ext 5+**: Functional programming extensions and Result types
- **Polly 8+**: Resilience and transient fault handling

## 2. Component Architecture

### 2.1 Core Components

```
Lakepipe/
├── Lakepipe.Core/
│   ├── Processors/
│   │   ├── IProcessor.cs          # Base processor interface
│   │   ├── ProcessorBase.cs       # Abstract base implementation
│   │   └── CompositeProcessor.cs  # Pipeline composition
│   ├── Streams/
│   │   ├── DataPart.cs           # Stream data unit
│   │   ├── StreamExtensions.cs   # IAsyncEnumerable extensions
│   │   └── ChannelProcessor.cs   # Channel-based streaming
│   ├── Results/
│   │   ├── Result.cs             # Railway-oriented Result type
│   │   ├── ResultExtensions.cs   # Functional composition
│   │   └── ValidationResult.cs   # Validation results
│   ├── Monitoring/
│   │   ├── IMetricsCollector.cs  # Metrics interface
│   │   ├── ResourceMonitor.cs    # Resource monitoring
│   │   └── TelemetryProcessor.cs # OpenTelemetry integration
│   └── Caching/
│       ├── ICache.cs             # Cache abstraction
│       ├── CacheKey.cs           # Deterministic cache keys
│       └── CachePolicy.cs        # Eviction policies
├── Lakepipe.Sources/
│   ├── S3/
│   │   ├── S3ParquetSource.cs   # AWS S3 source
│   │   └── S3Configuration.cs   # S3-specific config
│   ├── DuckDB/
│   │   ├── DuckDbSource.cs      # DuckDB source
│   │   └── DuckDbExtensions.cs  # Query extensions
│   ├── Iceberg/
│   │   ├── IcebergSource.cs     # Apache Iceberg source
│   │   └── IcebergCatalog.cs    # Catalog integration
│   ├── Parquet/
│   │   ├── ParquetSource.cs     # Local Parquet files
│   │   └── ParquetReader.cs     # Optimized reader
│   ├── Arrow/
│   │   ├── ArrowStreamSource.cs # Arrow IPC streams
│   │   └── ArrowMemoryPool.cs   # Memory management
│   └── Kafka/
│       ├── KafkaSource.cs        # Kafka consumer source
│       └── KafkaDeserializer.cs  # Message deserialization
├── Lakepipe.Sinks/
│   ├── Iceberg/
│   │   ├── IcebergSink.cs       # Iceberg table writer
│   │   └── IcebergCommitter.cs  # Transaction handling
│   ├── S3/
│   │   ├── S3ParquetSink.cs     # S3 Parquet writer
│   │   └── S3MultipartUpload.cs # Efficient uploads
│   ├── Parquet/
│   │   ├── ParquetSink.cs       # Local Parquet writer
│   │   └── ParquetWriter.cs     # Optimized writer
│   ├── Delta/
│   │   ├── DeltaLakeSink.cs     # Delta Lake writer
│   │   └── DeltaTransaction.cs  # ACID transactions
│   └── Kafka/
│       ├── KafkaSink.cs          # Kafka producer sink
│       └── KafkaSerializer.cs    # Message serialization
├── Lakepipe.Cache/
│   ├── Local/
│   │   ├── LocalFileCache.cs    # File system cache
│   │   └── CacheFileManager.cs  # File management
│   ├── Memory/
│   │   ├── MemoryCache.cs       # In-memory hot cache
│   │   └── MemoryCacheEntry.cs  # Cache entries
│   ├── Hybrid/
│   │   ├── HybridCache.cs       # Memory + disk cache
│   │   └── FasterCache.cs       # FASTER integration
│   └── Policies/
│       ├── LruPolicy.cs         # LRU eviction
│       ├── TtlPolicy.cs         # Time-based eviction
│       └── SizePolicy.cs        # Size-based limits
├── Lakepipe.Transforms/
│   ├── Arrow/
│   │   ├── ArrowTransform.cs    # Arrow compute operations
│   │   └── ArrowCompute.cs      # Compute kernels
│   ├── DuckDB/
│   │   ├── DuckDbTransform.cs   # SQL transformations
│   │   └── SqlBuilder.cs        # Query building
│   ├── User/
│   │   ├── IUserTransform.cs    # User-defined interface
│   │   └── TransformContext.cs  # Execution context
│   └── Optimizations/
│       ├── PredicatePushdown.cs # Query optimization
│       └── ProjectionPruning.cs # Column pruning
├── Lakepipe.Configuration/
│   ├── Models/
│   │   ├── PipelineConfig.cs    # Main config model
│   │   ├── SourceConfig.cs      # Source configuration
│   │   ├── SinkConfig.cs        # Sink configuration
│   │   └── CacheConfig.cs       # Cache configuration
│   ├── Builders/
│   │   ├── ConfigBuilder.cs     # Fluent config builder
│   │   └── EnvironmentConfig.cs # Environment variables
│   └── Validation/
│       ├── ConfigValidator.cs   # Configuration validation
│       └── ValidationRules.cs   # Validation rule sets
├── Lakepipe.Cli/
│   ├── Commands/
│   │   ├── RunCommand.cs        # Main pipeline command
│   │   ├── CacheCommand.cs      # Cache management
│   │   └── KafkaCommand.cs      # Kafka utilities
│   ├── Display/
│   │   ├── ProgressDisplay.cs   # Progress visualization
│   │   └── TableDisplay.cs      # Tabular output
│   └── Program.cs               # CLI entry point
└── Lakepipe.Api/
    ├── Pipeline/
    │   ├── PipelineBuilder.cs    # Fluent API builder
    │   ├── PipelineExecutor.cs   # Execution engine
    │   └── PipelineContext.cs    # Execution context
    ├── Operators/
    │   ├── ThenOperator.cs       # Sequential composition
    │   ├── ParallelOperator.cs   # Parallel execution
    │   └── ConditionalOperator.cs # Conditional branching
    └── Monitoring/
        ├── MetricsExporter.cs    # Metrics export
        └── HealthChecks.cs       # Health monitoring
```

### 2.2 Enhanced Configuration Schema

```csharp
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;

namespace Lakepipe.Configuration.Models;

// Cache configuration
public record CacheConfig
{
    public bool Enabled { get; init; } = true;
    public string CacheDirectory { get; init; } = "/tmp/lakepipe_cache";
    public string MaxSize { get; init; } = "10GB";
    public CompressionType Compression { get; init; } = CompressionType.Zstd;
    public int? TtlDays { get; init; } = 7;
    public bool ImmutableOnly { get; init; } = true;
    public List<string> IncludePatterns { get; init; } = new();
    public List<string> ExcludePatterns { get; init; } = new();
}

// Kafka configuration
public record KafkaConfig
{
    public List<string> BootstrapServers { get; init; } = new() { "localhost:9092" };
    public string Topic { get; init; } = "";
    public string? GroupId { get; init; }
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Latest;
    public SerializationFormat Serialization { get; init; } = SerializationFormat.Json;
    public string? SchemaRegistryUrl { get; init; }
    public string? PartitionKey { get; init; }
    public CompressionType CompressionType { get; init; } = CompressionType.Snappy;
    public int BatchSize { get; init; } = 16384;
    public int MaxPollRecords { get; init; } = 1000;
}

// Source configuration
public record SourceConfig
{
    public string Uri { get; init; } = "";
    public DataFormat Format { get; init; } = DataFormat.Parquet;
    public CompressionType? Compression { get; init; }
    public Dictionary<string, object>? Schema { get; init; }
    public CacheConfig? Cache { get; init; }
    public KafkaConfig? Kafka { get; init; }
}

// Sink configuration
public record SinkConfig
{
    public string Uri { get; init; } = "";
    public DataFormat Format { get; init; } = DataFormat.Parquet;
    public CompressionType? Compression { get; init; }
    public List<string>? PartitionBy { get; init; }
    public KafkaConfig? Kafka { get; init; }
}

// Transform configuration
public record TransformConfig
{
    public TransformEngine Engine { get; init; } = TransformEngine.Arrow;
    public List<TransformOperation> Operations { get; init; } = new();
    public List<string>? UserFunctions { get; init; }
}

// Main pipeline configuration
public record PipelineConfig
{
    public LogConfig Log { get; init; } = new();
    public SourceConfig Source { get; init; } = new();
    public SinkConfig Sink { get; init; } = new();
    public TransformConfig Transform { get; init; } = new();
    public CacheConfig Cache { get; init; } = new();
    public MonitoringConfig Monitoring { get; init; } = new();
    public StreamingConfig Streaming { get; init; } = new();
}

// Configuration builder with environment support
public class PipelineConfigBuilder
{
    private readonly IConfiguration _configuration;
    
    public PipelineConfigBuilder(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    
    public PipelineConfig Build(Action<PipelineConfig>? configure = null)
    {
        // Load from configuration sources (appsettings.json, environment, etc.)
        var config = new PipelineConfig
        {
            Cache = new CacheConfig
            {
                Enabled = _configuration.GetValue("LAKEPIPE_CACHE_ENABLED", true),
                CacheDirectory = _configuration.GetValue("LAKEPIPE_CACHE_DIR", "/tmp/lakepipe_cache")!,
                MaxSize = _configuration.GetValue("LAKEPIPE_CACHE_MAX_SIZE", "10GB")!,
                Compression = Enum.Parse<CompressionType>(
                    _configuration.GetValue("LAKEPIPE_CACHE_COMPRESSION", "Zstd")!),
                TtlDays = _configuration.GetValue<int?>("LAKEPIPE_CACHE_TTL_DAYS", 7),
                ImmutableOnly = _configuration.GetValue("LAKEPIPE_CACHE_IMMUTABLE_ONLY", true),
                IncludePatterns = _configuration.GetValue("LAKEPIPE_CACHE_INCLUDE_PATTERNS", "")!
                    .Split(',', StringSplitOptions.RemoveEmptyEntries).ToList(),
                ExcludePatterns = _configuration.GetValue("LAKEPIPE_CACHE_EXCLUDE_PATTERNS", "")!
                    .Split(',', StringSplitOptions.RemoveEmptyEntries).ToList()
            },
            Source = new SourceConfig
            {
                Uri = _configuration.GetValue("LAKEPIPE_SOURCE_URI", "")!,
                Format = Enum.Parse<DataFormat>(
                    _configuration.GetValue("LAKEPIPE_SOURCE_FORMAT", "Parquet")!),
                Kafka = _configuration.GetValue("LAKEPIPE_SOURCE_FORMAT", "") == "Kafka" 
                    ? new KafkaConfig
                    {
                        BootstrapServers = _configuration.GetValue("LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")!
                            .Split(',').ToList(),
                        Topic = _configuration.GetValue("LAKEPIPE_KAFKA_SOURCE_TOPIC", "")!,
                        GroupId = _configuration.GetValue<string?>("LAKEPIPE_KAFKA_GROUP_ID", null),
                        AutoOffsetReset = Enum.Parse<AutoOffsetReset>(
                            _configuration.GetValue("LAKEPIPE_KAFKA_AUTO_OFFSET_RESET", "Latest")!),
                        Serialization = Enum.Parse<SerializationFormat>(
                            _configuration.GetValue("LAKEPIPE_KAFKA_SERIALIZATION", "Json")!),
                        SchemaRegistryUrl = _configuration.GetValue<string?>("LAKEPIPE_KAFKA_SCHEMA_REGISTRY_URL", null),
                        CompressionType = Enum.Parse<CompressionType>(
                            _configuration.GetValue("LAKEPIPE_KAFKA_COMPRESSION_TYPE", "Snappy")!),
                        BatchSize = _configuration.GetValue("LAKEPIPE_KAFKA_BATCH_SIZE", 16384),
                        MaxPollRecords = _configuration.GetValue("LAKEPIPE_KAFKA_MAX_POLL_RECORDS", 1000)
                    } 
                    : null
            },
            // ... additional configuration sections
        };
        
        // Apply any programmatic overrides
        configure?.Invoke(config);
        
        return config;
    }
}

// Enums with JSON serialization support
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum CompressionType
{
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
    Brotli
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum DataFormat
{
    Parquet,
    Csv,
    Iceberg,
    DuckDb,
    Arrow,
    Kafka,
    Delta
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SerializationFormat
{
    Json,
    Avro,
    Protobuf,
    Raw
}
```

### 2.3 Environment Configuration Examples

Example `appsettings.json`:
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Lakepipe": "Debug"
    }
  },
  "Lakepipe": {
    "Source": {
      "Uri": "s3://data-lake/raw/*.parquet",
      "Format": "Parquet",
      "Compression": "Zstd"
    },
    "Sink": {
      "Uri": "kafka://processed-events",
      "Format": "Kafka",
      "Compression": "Snappy",
      "PartitionBy": ["user_id", "event_type"]
    },
    "Cache": {
      "Enabled": true,
      "CacheDirectory": "/opt/lakepipe/cache",
      "MaxSize": "50GB",
      "TtlDays": 30,
      "ImmutableOnly": true,
      "IncludePatterns": ["*.parquet", "s3://data-lake/immutable/*"],
      "ExcludePatterns": ["*/temp/*", "*/staging/*"]
    },
    "Kafka": {
      "BootstrapServers": ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
      "GroupId": "lakepipe-processors",
      "AutoOffsetReset": "Latest",
      "Serialization": "Json",
      "CompressionType": "Snappy",
      "BatchSize": 32768,
      "MaxPollRecords": 5000
    }
  }
}
```

Environment variable overrides (.env file or system):
```bash
# Override configuration via environment variables
LAKEPIPE_LOG_LEVEL=Debug
LAKEPIPE_SOURCE_URI=s3://special-bucket/data/*.parquet
LAKEPIPE_CACHE_ENABLED=true
LAKEPIPE_CACHE_DIR=/mnt/fast-ssd/lakepipe-cache
LAKEPIPE_CACHE_MAX_SIZE=100GB
LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS=kafka-prod-1:9092,kafka-prod-2:9092
```

## 3. Local Cache Layer

### 3.1 Intelligent Cache System

```csharp
using System.Security.Cryptography;
using Microsoft.Extensions.Caching.Memory;
using FASTER.core;

namespace Lakepipe.Cache.Hybrid;

public class HybridCache : ICache
{
    private readonly CacheConfig _config;
    private readonly IMemoryCache _memoryCache;
    private readonly FasterKV<CacheKey, CacheValue> _diskCache;
    private readonly SemaphoreSlim _cacheLock;
    private readonly ILogger<HybridCache> _logger;
    
    public HybridCache(CacheConfig config, ILogger<HybridCache> logger)
    {
        _config = config;
        _logger = logger;
        _cacheLock = new SemaphoreSlim(1, 1);
        
        // Initialize memory cache with size limits
        _memoryCache = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = ParseSize(_config.MaxSize) / 10, // 10% for memory
            CompactionPercentage = 0.25
        });
        
        // Initialize FASTER for disk cache
        var logDevice = Devices.CreateLogDevice(
            Path.Combine(_config.CacheDirectory, "cache.log"),
            preallocateFile: false);
            
        _diskCache = new FasterKV<CacheKey, CacheValue>(
            size: 1L << 20, // 1M buckets
            logSettings: new LogSettings
            {
                LogDevice = logDevice,
                PageSizeBits = 25, // 32MB pages
                MemorySizeBits = 34 // 16GB memory
            });
    }
    
    public async ValueTask<Result<ArrowRecordBatch>> GetCachedDataAsync(
        string sourceUri, 
        string transformHash, 
        CacheMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        if (!IsCacheable(sourceUri, metadata))
            return Result<ArrowRecordBatch>.Failure("Not cacheable");
        
        var cacheKey = GenerateCacheKey(sourceUri, transformHash);
        
        // Check memory cache first
        if (_memoryCache.TryGetValue(cacheKey, out ArrowRecordBatch? cachedBatch))
        {
            _logger.LogDebug("Memory cache hit for {Uri}", sourceUri);
            return Result<ArrowRecordBatch>.Success(cachedBatch);
        }
        
        // Check disk cache
        using var session = _diskCache.NewSession();
        var result = await session.ReadAsync(cacheKey);
        
        if (result.status.Found)
        {
            if (IsCacheValid(result.value))
            {
                var batch = await LoadFromDiskAsync(result.value.FilePath);
                
                // Promote to memory cache if frequently accessed
                if (ShouldPromoteToMemory(result.value))
                {
                    _memoryCache.Set(cacheKey, batch, new MemoryCacheEntryOptions
                    {
                        Size = batch.Length,
                        SlidingExpiration = TimeSpan.FromHours(1)
                    });
                }
                
                _logger.LogDebug("Disk cache hit for {Uri}", sourceUri);
                return Result<ArrowRecordBatch>.Success(batch);
            }
        }
        
        return Result<ArrowRecordBatch>.Failure("Cache miss");
    }
    
    public async ValueTask<Result<string>> StoreCachedDataAsync(
        string sourceUri,
        string transformHash,
        ArrowRecordBatch data,
        CacheMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        if (!IsCacheable(sourceUri, metadata))
            return Result<string>.Success("Not cacheable - skipped");
        
        try
        {
            var cacheKey = GenerateCacheKey(sourceUri, transformHash);
            var cacheFile = Path.Combine(_config.CacheDirectory, $"{cacheKey.Hash}.arrow");
            
            // Write to disk with compression
            await WriteArrowFileAsync(cacheFile, data, _config.Compression);
            
            // Store metadata in FASTER
            var cacheValue = new CacheValue
            {
                FilePath = cacheFile,
                Size = new FileInfo(cacheFile).Length,
                CreatedAt = DateTime.UtcNow,
                AccessCount = 0,
                Metadata = metadata
            };
            
            using var session = _diskCache.NewSession();
            await session.UpsertAsync(cacheKey, cacheValue);
            
            // Add to memory cache if small enough
            if (ShouldMemoryCache(cacheValue.Size))
            {
                _memoryCache.Set(cacheKey, data, new MemoryCacheEntryOptions
                {
                    Size = data.Length,
                    SlidingExpiration = TimeSpan.FromHours(1)
                });
            }
            
            _logger.LogInformation("Cached {Uri} to {File}", sourceUri, cacheFile);
            return Result<string>.Success($"Cached to {cacheFile}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cache storage failed for {Uri}", sourceUri);
            return Result<string>.Failure($"Cache storage failed: {ex.Message}");
        }
    }
    
    private CacheKey GenerateCacheKey(string sourceUri, string transformHash)
    {
        var keyData = $"{sourceUri}:{transformHash}";
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(keyData));
        return new CacheKey { Hash = Convert.ToHexString(hashBytes) };
    }
    
    private bool IsCacheable(string sourceUri, CacheMetadata metadata)
    {
        if (!_config.Enabled)
            return false;
        
        if (_config.ImmutableOnly && !metadata.IsImmutable)
            return false;
        
        // Check exclude patterns
        if (_config.ExcludePatterns.Any(pattern => 
            sourceUri.Contains(pattern, StringComparison.OrdinalIgnoreCase)))
            return false;
        
        // Check include patterns
        if (_config.IncludePatterns.Any())
            return _config.IncludePatterns.Any(pattern => 
                sourceUri.Contains(pattern, StringComparison.OrdinalIgnoreCase));
        
        return true;
    }
    
    private bool IsCacheValid(CacheValue cacheValue)
    {
        if (!_config.TtlDays.HasValue)
            return true;
        
        var age = DateTime.UtcNow - cacheValue.CreatedAt;
        return age.TotalDays < _config.TtlDays.Value;
    }
    
    public async Task<CacheCleanupResult> CleanupCacheAsync()
    {
        var result = new CacheCleanupResult();
        
        await _cacheLock.WaitAsync();
        try
        {
            // Clean disk cache
            if (_config.TtlDays.HasValue)
            {
                var cutoffDate = DateTime.UtcNow.AddDays(-_config.TtlDays.Value);
                
                await foreach (var file in GetCacheFilesAsync())
                {
                    if (File.GetCreationTimeUtc(file) < cutoffDate)
                    {
                        File.Delete(file);
                        result.DeletedFiles++;
                    }
                }
            }
            
            // Compact FASTER log
            await _diskCache.TakeFullCheckpointAsync(CheckpointType.Snapshot);
            
            // Memory cache is self-managing with size limits
            _memoryCache.Compact(0.25);
            
            return result;
        }
        finally
        {
            _cacheLock.Release();
        }
    }
}

// Supporting types
public record struct CacheKey
{
    public string Hash { get; init; }
}

public record CacheValue
{
    public string FilePath { get; init; }
    public long Size { get; init; }
    public DateTime CreatedAt { get; init; }
    public int AccessCount { get; init; }
    public CacheMetadata Metadata { get; init; }
}

public record CacheMetadata
{
    public bool IsImmutable { get; init; }
    public string? ETag { get; init; }
    public DateTime? LastModified { get; init; }
    public Dictionary<string, string> CustomMetadata { get; init; } = new();
}
```

### 3.2 Cache-Aware Source Processor

```csharp
namespace Lakepipe.Core.Processors;

public abstract class CachedSourceProcessor<T> : ProcessorBase<T>
{
    private readonly ICache _cache;
    private readonly ILogger _logger;
    
    protected CachedSourceProcessor(
        SourceConfig config, 
        ICache cache,
        ILogger logger) : base(config)
    {
        _cache = cache;
        _logger = logger;
    }
    
    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            var sourceUri = part.SourceInfo.Uri;
            var transformHash = part.Metadata.TransformHash ?? "";
            
            // Try cache first
            var cacheResult = await _cache.GetCachedDataAsync(
                sourceUri, 
                transformHash, 
                part.CacheMetadata,
                cancellationToken);
            
            if (cacheResult.IsSuccess)
            {
                // Cache hit - return cached data
                _logger.LogDebug("Cache hit for {Uri}", sourceUri);
                
                yield return part with
                {
                    Data = cacheResult.Value,
                    Metadata = part.Metadata with { CacheHit = true }
                };
                continue;
            }
            
            // Cache miss - process normally
            var processedData = await ProcessSingleAsync(part, cancellationToken);
            
            if (processedData != null)
            {
                // Store in cache for future use
                var storeResult = await _cache.StoreCachedDataAsync(
                    sourceUri,
                    transformHash,
                    processedData.Data,
                    part.CacheMetadata,
                    cancellationToken);
                
                if (storeResult.IsSuccess)
                {
                    processedData = processedData with
                    {
                        Metadata = processedData.Metadata with { CacheStored = true }
                    };
                }
                
                yield return processedData;
            }
        }
    }
    
    protected abstract Task<DataPart?> ProcessSingleAsync(
        DataPart input, 
        CancellationToken cancellationToken);
}
```

## 4. Advanced Streaming Architecture with TPL Dataflow

### 4.1 Core Streaming Infrastructure

```csharp
using System.Threading.Tasks.Dataflow;
using System.Runtime.InteropServices;

namespace Lakepipe.Streaming;

/// <summary>
/// High-performance streaming engine using TPL Dataflow with CPU affinity
/// </summary>
public class DataflowStreamingEngine
{
    private readonly StreamingConfig _config;
    private readonly ILogger<DataflowStreamingEngine> _logger;
    private readonly Dictionary<string, ProcessorAffinity> _affinityMap;
    
    public DataflowStreamingEngine(StreamingConfig config, ILogger<DataflowStreamingEngine> logger)
    {
        _config = config;
        _logger = logger;
        _affinityMap = new Dictionary<string, ProcessorAffinity>();
    }
    
    /// <summary>
    /// Creates an optimized dataflow pipeline with CPU-pinned execution blocks
    /// </summary>
    public IDataflowBlock CreatePipeline(PipelineConfig config)
    {
        // Configure execution options with bounded capacity for backpressure
        var linkOptions = new DataflowLinkOptions 
        { 
            PropagateCompletion = true,
            MaxMessages = _config.MaxMessagesInFlight
        };
        
        // Source block with parallelism and CPU affinity
        var sourceBlock = CreateSourceBlock(config.Source, linkOptions);
        
        // Transform blocks with dedicated CPU cores
        var transformBlocks = CreateTransformBlocks(config.Transform, linkOptions);
        
        // Sink block with batching and CPU affinity
        var sinkBlock = CreateSinkBlock(config.Sink, linkOptions);
        
        // Link the pipeline
        sourceBlock.LinkTo(transformBlocks.First(), linkOptions);
        
        for (int i = 0; i < transformBlocks.Count - 1; i++)
        {
            transformBlocks[i].LinkTo(transformBlocks[i + 1], linkOptions);
        }
        
        transformBlocks.Last().LinkTo(sinkBlock, linkOptions);
        
        return sourceBlock;
    }
    
    private TransformBlock<byte[], DataPart> CreateSourceBlock(
        SourceConfig config, 
        DataflowLinkOptions linkOptions)
    {
        var executionOptions = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = _config.SourceBufferSize,
            MaxDegreeOfParallelism = _config.SourceParallelism,
            SingleProducerConstrained = config.Format == DataFormat.Kafka,
            TaskScheduler = CreatePinnedTaskScheduler("source", _config.SourceCpuAffinity)
        };
        
        return new TransformBlock<byte[], DataPart>(
            async rawData => await DeserializeDataPartAsync(rawData, config),
            executionOptions);
    }
    
    private List<IPropagatorBlock<DataPart, DataPart>> CreateTransformBlocks(
        TransformConfig config,
        DataflowLinkOptions linkOptions)
    {
        var blocks = new List<IPropagatorBlock<DataPart, DataPart>>();
        
        // Create transform blocks with CPU pinning
        for (int i = 0; i < config.Operations.Count; i++)
        {
            var operation = config.Operations[i];
            var cpuSet = _config.TransformCpuAffinity?[i] ?? new[] { i % Environment.ProcessorCount };
            
            var executionOptions = new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = _config.TransformBufferSize,
                MaxDegreeOfParallelism = operation.Parallelism ?? _config.DefaultParallelism,
                TaskScheduler = CreatePinnedTaskScheduler($"transform_{i}", cpuSet)
            };
            
            var transformBlock = new TransformBlock<DataPart, DataPart>(
                async part => await ExecuteTransformAsync(part, operation),
                executionOptions);
            
            blocks.Add(transformBlock);
        }
        
        return blocks;
    }
    
    private ActionBlock<DataPart> CreateSinkBlock(
        SinkConfig config,
        DataflowLinkOptions linkOptions)
    {
        var batchBlock = new BatchBlock<DataPart>(
            _config.SinkBatchSize,
            new GroupingDataflowBlockOptions
            {
                BoundedCapacity = _config.SinkBufferSize,
                Greedy = true
            });
        
        var executionOptions = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = _config.SinkBufferSize,
            MaxDegreeOfParallelism = _config.SinkParallelism,
            TaskScheduler = CreatePinnedTaskScheduler("sink", _config.SinkCpuAffinity)
        };
        
        var sinkBlock = new ActionBlock<DataPart[]>(
            async batch => await WriteBatchAsync(batch, config),
            executionOptions);
        
        batchBlock.LinkTo(sinkBlock, linkOptions);
        
        return DataflowBlock.Encapsulate(batchBlock, sinkBlock);
    }
    
    /// <summary>
    /// Creates a custom TaskScheduler that pins tasks to specific CPU cores
    /// </summary>
    private TaskScheduler CreatePinnedTaskScheduler(string name, int[] cpuSet)
    {
        return new CpuAffinityTaskScheduler(name, cpuSet, _logger);
    }
}

/// <summary>
/// Custom TaskScheduler with CPU affinity for optimal cache locality
/// </summary>
public class CpuAffinityTaskScheduler : TaskScheduler
{
    private readonly string _name;
    private readonly int[] _cpuSet;
    private readonly ILogger _logger;
    private readonly BlockingCollection<Task> _tasks;
    private readonly Thread[] _threads;
    
    public CpuAffinityTaskScheduler(string name, int[] cpuSet, ILogger logger)
    {
        _name = name;
        _cpuSet = cpuSet;
        _logger = logger;
        _tasks = new BlockingCollection<Task>();
        _threads = new Thread[cpuSet.Length];
        
        InitializeThreads();
    }
    
    private void InitializeThreads()
    {
        for (int i = 0; i < _cpuSet.Length; i++)
        {
            var cpuIndex = _cpuSet[i];
            var thread = new Thread(() => ProcessTasks(cpuIndex))
            {
                Name = $"{_name}_CPU{cpuIndex}",
                IsBackground = true,
                Priority = ThreadPriority.AboveNormal
            };
            
            _threads[i] = thread;
            thread.Start();
            
            // Pin thread to CPU core
            SetThreadAffinity(thread, cpuIndex);
        }
    }
    
    private void SetThreadAffinity(Thread thread, int cpuIndex)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // Windows: Use P/Invoke to set thread affinity
            var affinity = (IntPtr)(1 << cpuIndex);
            SetThreadAffinityMask(thread.ManagedThreadId, affinity);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // Linux: Use sched_setaffinity through P/Invoke
            var cpuSet = new CpuSet();
            cpuSet.Set(cpuIndex);
            SchedSetAffinity(thread.ManagedThreadId, cpuSet);
        }
        
        _logger.LogInformation("Thread {ThreadName} pinned to CPU {Cpu}", 
            thread.Name, cpuIndex);
    }
    
    private void ProcessTasks(int cpuIndex)
    {
        foreach (var task in _tasks.GetConsumingEnumerable())
        {
            TryExecuteTask(task);
        }
    }
    
    protected override IEnumerable<Task> GetScheduledTasks() => _tasks;
    
    protected override void QueueTask(Task task)
    {
        _tasks.Add(task);
    }
    
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        // Only allow inline execution if we're on one of our pinned threads
        var currentThread = Thread.CurrentThread;
        return _threads.Contains(currentThread) && TryExecuteTask(task);
    }
    
    [DllImport("kernel32.dll")]
    private static extern IntPtr SetThreadAffinityMask(int threadId, IntPtr affinityMask);
    
    [DllImport("libc.so.6")]
    private static extern int SchedSetAffinity(int threadId, ref CpuSet cpuSet);
}
```

### 4.2 Streaming Configuration

```csharp
namespace Lakepipe.Configuration.Models;

public record StreamingConfig
{
    /// <summary>
    /// Enable CPU affinity for optimal cache locality
    /// </summary>
    public bool EnableCpuAffinity { get; init; } = true;
    
    /// <summary>
    /// CPU cores assigned to source processing (e.g., Kafka consumers)
    /// </summary>
    public int[] SourceCpuAffinity { get; init; } = { 0, 1 };
    
    /// <summary>
    /// CPU cores assigned to transform operations
    /// Can be array of arrays for multiple transform stages
    /// </summary>
    public int[][]? TransformCpuAffinity { get; init; }
    
    /// <summary>
    /// CPU cores assigned to sink operations
    /// </summary>
    public int[] SinkCpuAffinity { get; init; } = { 2, 3 };
    
    /// <summary>
    /// Dataflow bounded capacity for backpressure
    /// </summary>
    public int SourceBufferSize { get; init; } = 1000;
    public int TransformBufferSize { get; init; } = 5000;
    public int SinkBufferSize { get; init; } = 1000;
    
    /// <summary>
    /// Parallelism settings
    /// </summary>
    public int SourceParallelism { get; init; } = 2;
    public int DefaultParallelism { get; init; } = 4;
    public int SinkParallelism { get; init; } = 2;
    
    /// <summary>
    /// Batching configuration
    /// </summary>
    public int SinkBatchSize { get; init; } = 1000;
    public TimeSpan SinkBatchTimeout { get; init; } = TimeSpan.FromMilliseconds(100);
    
    /// <summary>
    /// Flow control
    /// </summary>
    public int MaxMessagesInFlight { get; init; } = 10000;
    public int MaxMemoryPerPipelineMB { get; init; } = 4096;
    
    /// <summary>
    /// Monitoring and metrics
    /// </summary>
    public bool EnableDetailedMetrics { get; init; } = true;
    public TimeSpan MetricsInterval { get; init; } = TimeSpan.FromSeconds(10);
}
```

### 4.3 Infinite Stream Processing with Kafka

```csharp
namespace Lakepipe.Sources.Kafka;

/// <summary>
/// High-performance Kafka source optimized for infinite streaming
/// </summary>
public class KafkaStreamingSource : IStreamingSource
{
    private readonly KafkaConfig _config;
    private readonly ILogger<KafkaStreamingSource> _logger;
    private readonly IMetricsCollector _metrics;
    private readonly SemaphoreSlim _pauseSemaphore;
    private IConsumer<string, byte[]>? _consumer;
    private volatile bool _isPaused;
    
    public KafkaStreamingSource(
        KafkaConfig config,
        IMetricsCollector metrics,
        ILogger<KafkaStreamingSource> logger)
    {
        _config = config;
        _metrics = metrics;
        _logger = logger;
        _pauseSemaphore = new SemaphoreSlim(1, 1);
    }
    
    /// <summary>
    /// Creates an infinite stream of data parts from Kafka
    /// </summary>
    public async IAsyncEnumerable<DataPart> StreamAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = string.Join(",", _config.BootstrapServers),
            GroupId = _config.GroupId,
            AutoOffsetReset = _config.AutoOffsetReset,
            EnableAutoCommit = false, // Manual commit for better control
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 10000,
            FetchMaxBytes = 52428800, // 50MB
            MaxPartitionFetchBytes = 10485760, // 10MB per partition
            QueuedMaxMessagesKbytes = 1000000, // 1GB buffer
            StatisticsIntervalMs = 5000,
            EnablePartitionEof = true
        };
        
        _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig)
            .SetStatisticsHandler((_, statistics) => HandleStatistics(statistics))
            .SetOffsetsCommittedHandler((_, offsets) => HandleOffsetsCommitted(offsets))
            .SetErrorHandler((_, error) => HandleError(error))
            .SetPartitionsAssignedHandler((c, partitions) => HandlePartitionsAssigned(c, partitions))
            .SetPartitionsRevokedHandler((c, partitions) => HandlePartitionsRevoked(c, partitions))
            .Build();
        
        _consumer.Subscribe(_config.Topic);
        
        // Use channels for better async handling
        var channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(
            new BoundedChannelOptions(10000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
        
        // Start consumer tasks on dedicated threads
        var consumerTasks = Enumerable.Range(0, _config.ConsumerThreads ?? 2)
            .Select(i => Task.Run(async () => 
                await ConsumeLoopAsync(channel.Writer, cancellationToken), 
                cancellationToken))
            .ToArray();
        
        // Process messages from channel
        var buffer = new List<ConsumeResult<string, byte[]>>(_config.MaxPollRecords);
        var flushTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Check for pause requests
                if (_isPaused)
                {
                    await _pauseSemaphore.WaitAsync(cancellationToken);
                    _pauseSemaphore.Release();
                    continue;
                }
                
                // Read from channel with timeout for periodic flushing
                var readTask = channel.Reader.ReadAsync(cancellationToken).AsTask();
                var timerTask = flushTimer.WaitForNextTickAsync(cancellationToken).AsTask();
                
                var completedTask = await Task.WhenAny(readTask, timerTask);
                
                if (completedTask == readTask && readTask.IsCompletedSuccessfully)
                {
                    buffer.Add(await readTask);
                }
                
                // Yield batch if buffer is full or timer elapsed
                if (buffer.Count >= _config.MaxPollRecords || completedTask == timerTask)
                {
                    if (buffer.Any())
                    {
                        var batch = await CreateOptimizedBatchAsync(buffer);
                        
                        yield return new DataPart
                        {
                            Data = batch,
                            Metadata = new DataPartMetadata
                            {
                                Source = "kafka",
                                Topic = _config.Topic,
                                PartitionOffsets = GetPartitionOffsets(buffer),
                                MessageCount = buffer.Count,
                                Timestamp = DateTime.UtcNow,
                                StreamPosition = buffer.Last().Offset.Value
                            },
                            SourceInfo = new SourceInfo
                            {
                                Uri = $"kafka://{_config.Topic}",
                                Format = DataFormat.Kafka
                            }
                        };
                        
                        // Commit offsets after successful processing
                        await CommitOffsetsAsync(buffer);
                        
                        _metrics.RecordMessages("kafka_consumed", buffer.Count);
                        buffer.Clear();
                    }
                }
            }
        }
        finally
        {
            channel.Writer.Complete();
            await Task.WhenAll(consumerTasks);
            _consumer?.Close();
            flushTimer.Dispose();
        }
    }
    
    private async Task ConsumeLoopAsync(
        ChannelWriter<ConsumeResult<string, byte[]>> writer,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer!.Consume(TimeSpan.FromMilliseconds(100));
                
                if (consumeResult != null && !consumeResult.IsPartitionEOF)
                {
                    await writer.WriteAsync(consumeResult, cancellationToken);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error");
                _metrics.RecordError("kafka_consume_error");
            }
        }
    }
    
    private async Task<ArrowRecordBatch> CreateOptimizedBatchAsync(
        List<ConsumeResult<string, byte[]>> messages)
    {
        // Pre-allocate arrays for better performance
        var capacity = messages.Count;
        var keyBuilder = new StringArray.Builder(capacity);
        var valueBuilder = new BinaryArray.Builder(capacity);
        var topicBuilder = new StringArray.Builder(capacity);
        var partitionBuilder = new Int32Array.Builder(capacity);
        var offsetBuilder = new Int64Array.Builder(capacity);
        var timestampBuilder = new TimestampArray.Builder(capacity);
        var headerBuilder = new StringArray.Builder(capacity);
        
        // Process messages in parallel batches for CPU efficiency
        var partitioner = Partitioner.Create(messages, true);
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount
        };
        
        await Task.Run(() =>
        {
            Parallel.ForEach(partitioner, parallelOptions, message =>
            {
                var index = messages.IndexOf(message);
                
                lock (keyBuilder) keyBuilder.Set(index, message.Message.Key ?? "");
                lock (valueBuilder) valueBuilder.Set(index, message.Message.Value);
                lock (topicBuilder) topicBuilder.Set(index, message.Topic);
                lock (partitionBuilder) partitionBuilder.Set(index, message.Partition.Value);
                lock (offsetBuilder) offsetBuilder.Set(index, message.Offset.Value);
                lock (timestampBuilder) timestampBuilder.Set(index, message.Message.Timestamp.UtcDateTime);
                
                if (message.Message.Headers != null)
                {
                    var headers = string.Join(";", 
                        message.Message.Headers.Select(h => $"{h.Key}={Encoding.UTF8.GetString(h.GetValueBytes())}"));
                    lock (headerBuilder) headerBuilder.Set(index, headers);
                }
            });
        });
        
        var schema = new Schema.Builder()
            .Field("key", StringType.Default)
            .Field("value", BinaryType.Default)
            .Field("topic", StringType.Default)
            .Field("partition", Int32Type.Default)
            .Field("offset", Int64Type.Default)
            .Field("timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"))
            .Field("headers", StringType.Default)
            .Build();
        
        return new RecordBatch(schema, new IArrowArray[]
        {
            keyBuilder.Build(),
            valueBuilder.Build(),
            topicBuilder.Build(),
            partitionBuilder.Build(),
            offsetBuilder.Build(),
            timestampBuilder.Build(),
            headerBuilder.Build()
        }, messages.Count);
    }
    
    /// <summary>
    /// Pause consumption for backpressure handling
    /// </summary>
    public async Task PauseAsync()
    {
        await _pauseSemaphore.WaitAsync();
        try
        {
            _isPaused = true;
            _consumer?.Pause(_consumer.Assignment);
            _logger.LogInformation("Kafka consumption paused");
        }
        finally
        {
            _pauseSemaphore.Release();
        }
    }
    
    /// <summary>
    /// Resume consumption after backpressure relief
    /// </summary>
    public async Task ResumeAsync()
    {
        await _pauseSemaphore.WaitAsync();
        try
        {
            _isPaused = false;
            _consumer?.Resume(_consumer.Assignment);
            _logger.LogInformation("Kafka consumption resumed");
        }
        finally
        {
            _pauseSemaphore.Release();
        }
    }
}
```

### 4.4 Dataflow Pipeline Examples

```csharp
namespace Lakepipe.Api.Pipeline;

public class StreamingPipelineBuilder
{
    private readonly IServiceProvider _serviceProvider;
    private readonly DataflowStreamingEngine _engine;
    
    public StreamingPipelineBuilder(
        IServiceProvider serviceProvider,
        DataflowStreamingEngine engine)
    {
        _serviceProvider = serviceProvider;
        _engine = engine;
    }
    
    /// <summary>
    /// Build a high-performance streaming pipeline with CPU affinity
    /// </summary>
    public async Task<IStreamingPipeline> BuildAsync(PipelineConfig config)
    {
        // Configure CPU affinity based on available cores
        var coreCount = Environment.ProcessorCount;
        var streamingConfig = config.Streaming with
        {
            // Dedicate cores for each stage
            SourceCpuAffinity = Enumerable.Range(0, Math.Min(2, coreCount / 4)).ToArray(),
            TransformCpuAffinity = new[]
            {
                Enumerable.Range(2, Math.Min(4, coreCount / 2)).ToArray(),
                Enumerable.Range(6, Math.Min(4, coreCount / 4)).ToArray()
            },
            SinkCpuAffinity = Enumerable.Range(coreCount - 2, 2).ToArray()
        };
        
        // Create dataflow blocks
        var sourceBlock = CreateKafkaSourceBlock(config.Source, streamingConfig);
        var transformBlock = CreateTransformBlock(config.Transform, streamingConfig);
        var monitorBlock = CreateMonitoringBlock(streamingConfig);
        var sinkBlock = CreateSinkBlock(config.Sink, streamingConfig);
        
        // Link with propagation and filtering
        var linkOptions = new DataflowLinkOptions 
        { 
            PropagateCompletion = true,
            MaxMessages = streamingConfig.MaxMessagesInFlight
        };
        
        sourceBlock.LinkTo(transformBlock, linkOptions);
        transformBlock.LinkTo(monitorBlock, linkOptions);
        monitorBlock.LinkTo(sinkBlock, linkOptions, part => part != null);
        monitorBlock.LinkTo(DataflowBlock.NullTarget<DataPart>(), part => part == null);
        
        return new DataflowPipeline(sourceBlock, sinkBlock, streamingConfig);
    }
    
    private IPropagatorBlock<DataPart, DataPart> CreateMonitoringBlock(
        StreamingConfig config)
    {
        var executionOptions = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = config.TransformBufferSize,
            MaxDegreeOfParallelism = 1,
            EnsureOrdered = false
        };
        
        return new TransformBlock<DataPart, DataPart>(async part =>
        {
            // Monitor memory usage
            var memoryUsage = GC.GetTotalMemory(false);
            if (memoryUsage > config.MaxMemoryPerPipelineMB * 1024 * 1024)
            {
                // Trigger backpressure
                await HandleBackpressureAsync();
            }
            
            // Record metrics
            _metrics.RecordThroughput(part.Metadata.MessageCount);
            _metrics.RecordLatency(DateTime.UtcNow - part.Metadata.Timestamp);
            
            return part;
        }, executionOptions);
    }
}

/// <summary>
/// Example: Real-time anomaly detection pipeline
/// </summary>
public class AnomalyDetectionPipeline
{
    public static async Task<IStreamingPipeline> CreateAsync(
        IServiceProvider serviceProvider)
    {
        var config = new PipelineConfig
        {
            Source = new SourceConfig
            {
                Uri = "kafka://sensor-data",
                Format = DataFormat.Kafka,
                Kafka = new KafkaConfig
                {
                    BootstrapServers = new[] { "kafka-1:9092", "kafka-2:9092" },
                    Topic = "sensor-data",
                    GroupId = "anomaly-detector",
                    ConsumerThreads = 4,
                    MaxPollRecords = 5000
                }
            },
            Transform = new TransformConfig
            {
                Engine = TransformEngine.Custom,
                Operations = new[]
                {
                    new TransformOperation
                    {
                        Type = "WindowAggregate",
                        Config = new { WindowSize = "5m", Step = "1m" },
                        Parallelism = 8
                    },
                    new TransformOperation
                    {
                        Type = "AnomalyDetection",
                        Config = new { Algorithm = "IsolationForest", Threshold = 0.95 },
                        Parallelism = 4
                    }
                }
            },
            Sink = new SinkConfig
            {
                Uri = "kafka://anomalies",
                Format = DataFormat.Kafka,
                Kafka = new KafkaConfig
                {
                    BootstrapServers = new[] { "kafka-1:9092", "kafka-2:9092" },
                    Topic = "anomalies",
                    CompressionType = CompressionType.Snappy,
                    BatchSize = 32768
                }
            },
            Streaming = new StreamingConfig
            {
                EnableCpuAffinity = true,
                SourceBufferSize = 10000,
                TransformBufferSize = 50000,
                SinkBufferSize = 10000,
                SinkBatchSize = 1000,
                MaxMessagesInFlight = 100000,
                MaxMemoryPerPipelineMB = 8192
            }
        };
        
        var builder = serviceProvider.GetRequiredService<StreamingPipelineBuilder>();
        return await builder.BuildAsync(config);
    }
}
```

## 5. Kafka Streaming Integration

### 5.1 Kafka Source Processor

```csharp
using Confluent.Kafka;
using Apache.Arrow;
using System.Threading.Channels;

namespace Lakepipe.Sources.Kafka;

public class KafkaSource : IProcessor
{
    private readonly KafkaConfig _config;
    private readonly ILogger<KafkaSource> _logger;
    private readonly IDeserializer<ArrowRecordBatch> _deserializer;
    private IConsumer<string, byte[]>? _consumer;
    
    public KafkaSource(
        KafkaConfig config, 
        IDeserializer<ArrowRecordBatch> deserializer,
        ILogger<KafkaSource> logger)
    {
        _config = config;
        _deserializer = deserializer;
        _logger = logger;
    }
    
    public async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = string.Join(",", _config.BootstrapServers),
            GroupId = _config.GroupId,
            AutoOffsetReset = _config.AutoOffsetReset,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000,
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 10000,
            FetchMaxBytes = 52428800, // 50MB
            MaxPartitionFetchBytes = 1048576 // 1MB
        };
        
        _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Partitions assigned: {Partitions}", 
                    string.Join(", ", partitions));
            })
            .Build();
        
        _consumer.Subscribe(_config.Topic);
        
        var channel = Channel.CreateUnbounded<ConsumeResult<string, byte[]>>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        
        // Start consumer task
        var consumerTask = Task.Run(async () =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(cancellationToken);
                    await channel.Writer.WriteAsync(result, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            finally
            {
                channel.Writer.Complete();
                _consumer.Close();
            }
        }, cancellationToken);
        
        // Process messages in batches
        var messageBatch = new List<ConsumeResult<string, byte[]>>();
        var batchSize = 1000;
        
        await foreach (var message in channel.Reader.ReadAllAsync(cancellationToken))
        {
            messageBatch.Add(message);
            
            if (messageBatch.Count >= batchSize)
            {
                var batch = await CreateBatchAsync(messageBatch);
                
                yield return new DataPart
                {
                    Data = batch,
                    Metadata = new DataPartMetadata
                    {
                        Source = "kafka",
                        Topic = _config.Topic,
                        PartitionOffsets = GetPartitionOffsets(messageBatch),
                        MessageCount = messageBatch.Count,
                        Timestamp = DateTime.UtcNow
                    },
                    SourceInfo = new SourceInfo
                    {
                        Uri = $"kafka://{_config.Topic}",
                        Format = DataFormat.Kafka
                    }
                };
                
                messageBatch.Clear();
            }
        }
        
        // Process remaining messages
        if (messageBatch.Any())
        {
            var batch = await CreateBatchAsync(messageBatch);
            yield return new DataPart
            {
                Data = batch,
                Metadata = new DataPartMetadata
                {
                    Source = "kafka",
                    Topic = _config.Topic,
                    MessageCount = messageBatch.Count
                }
            };
        }
        
        await consumerTask;
    }
    
    private async Task<ArrowRecordBatch> CreateBatchAsync(
        List<ConsumeResult<string, byte[]>> messages)
    {
        var builder = new RecordBatch.Builder();
        
        // Build Arrow arrays from messages
        var keys = new StringBuilder();
        var values = new List<byte[]>();
        var topics = new StringBuilder();
        var partitions = new Int32Array.Builder();
        var offsets = new Int64Array.Builder();
        var timestamps = new TimestampArray.Builder();
        
        foreach (var msg in messages)
        {
            keys.Append(msg.Message.Key ?? "");
            values.Add(await DeserializeMessageAsync(msg.Message.Value));
            topics.Append(msg.Topic);
            partitions.Append(msg.Partition.Value);
            offsets.Append(msg.Offset.Value);
            timestamps.Append(msg.Message.Timestamp.UtcDateTime);
        }
        
        var schema = new Schema.Builder()
            .Field("key", StringType.Default)
            .Field("value", BinaryType.Default)
            .Field("topic", StringType.Default)
            .Field("partition", Int32Type.Default)
            .Field("offset", Int64Type.Default)
            .Field("timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"))
            .Build();
        
        return new RecordBatch(schema, new IArrowArray[]
        {
            keys.Build(),
            new BinaryArray.Builder().AppendRange(values).Build(),
            topics.Build(),
            partitions.Build(),
            offsets.Build(),
            timestamps.Build()
        }, messages.Count);
    }
    
    private async Task<byte[]> DeserializeMessageAsync(byte[] data)
    {
        return _config.Serialization switch
        {
            SerializationFormat.Json => data, // Keep as bytes for Arrow
            SerializationFormat.Avro => await DeserializeAvroAsync(data),
            SerializationFormat.Protobuf => await DeserializeProtobufAsync(data),
            _ => data
        };
    }
}
```

### 4.2 Kafka Sink Processor

```csharp
namespace Lakepipe.Sinks.Kafka;

public class KafkaSink : IProcessor
{
    private readonly KafkaConfig _config;
    private readonly ISerializer<ArrowRecordBatch> _serializer;
    private readonly ILogger<KafkaSink> _logger;
    private IProducer<string, byte[]>? _producer;
    
    public KafkaSink(
        KafkaConfig config,
        ISerializer<ArrowRecordBatch> serializer,
        ILogger<KafkaSink> logger)
    {
        _config = config;
        _serializer = serializer;
        _logger = logger;
    }
    
    public async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = string.Join(",", _config.BootstrapServers),
            CompressionType = _config.CompressionType,
            BatchSize = _config.BatchSize,
            LingerMs = 100,
            Acks = Acks.All,
            MessageSendMaxRetries = 3,
            EnableIdempotence = true,
            MaxInFlight = 5
        };
        
        _producer = new ProducerBuilder<string, byte[]>(producerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka producer error: {Reason}", e.Reason))
            .Build();
        
        try
        {
            await foreach (var part in inputStream.WithCancellation(cancellationToken))
            {
                var messages = await ConvertToMessagesAsync(part.Data);
                var sentCount = 0;
                var deliveryReports = new List<DeliveryReport<string, byte[]>>();
                
                foreach (var message in messages)
                {
                    try
                    {
                        var deliveryReport = await _producer.ProduceAsync(
                            _config.Topic,
                            message,
                            cancellationToken);
                        
                        deliveryReports.Add(deliveryReport);
                        sentCount++;
                    }
                    catch (ProduceException<string, byte[]> ex)
                    {
                        _logger.LogError(ex, "Failed to send message to Kafka");
                    }
                }
                
                // Yield result with delivery metadata
                yield return part with
                {
                    Metadata = part.Metadata with
                    {
                        KafkaSent = sentCount,
                        SinkTopic = _config.Topic,
                        DeliveryReports = deliveryReports.Select(r => new KafkaDeliveryInfo
                        {
                            Partition = r.Partition.Value,
                            Offset = r.Offset.Value,
                            Timestamp = r.Timestamp.UtcDateTime
                        }).ToList()
                    }
                };
            }
        }
        finally
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
        }
    }
    
    private async Task<List<Message<string, byte[]>>> ConvertToMessagesAsync(
        ArrowRecordBatch batch)
    {
        var messages = new List<Message<string, byte[]>>();
        
        // Convert Arrow batch to Kafka messages
        for (int i = 0; i < batch.Length; i++)
        {
            var key = GetPartitionKey(batch, i);
            var value = await SerializeRowAsync(batch, i);
            
            messages.Add(new Message<string, byte[]>
            {
                Key = key,
                Value = value,
                Headers = new Headers
                {
                    { "lakepipe-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
                    { "lakepipe-source", Encoding.UTF8.GetBytes("batch-processing") }
                }
            });
        }
        
        return messages;
    }
    
    private string? GetPartitionKey(ArrowRecordBatch batch, int rowIndex)
    {
        if (string.IsNullOrEmpty(_config.PartitionKey))
            return null;
        
        var columnIndex = batch.Schema.GetFieldIndex(_config.PartitionKey);
        if (columnIndex < 0)
            return null;
        
        var array = batch.Column(columnIndex);
        return array.GetScalar(rowIndex)?.ToString();
    }
    
    private async Task<byte[]> SerializeRowAsync(ArrowRecordBatch batch, int rowIndex)
    {
        return _config.Serialization switch
        {
            SerializationFormat.Json => await SerializeJsonAsync(batch, rowIndex),
            SerializationFormat.Avro => await SerializeAvroAsync(batch, rowIndex),
            SerializationFormat.Protobuf => await SerializeProtobufAsync(batch, rowIndex),
            _ => SerializeRaw(batch, rowIndex)
        };
    }
}
```

## 5. Enhanced CLI Interface

### 5.1 Modern CLI with Spectre.Console

```csharp
using Spectre.Console.Cli;
using Spectre.Console;

namespace Lakepipe.Cli.Commands;

public class RunCommand : AsyncCommand<RunCommand.Settings>
{
    private readonly IPipelineBuilder _pipelineBuilder;
    private readonly ILogger<RunCommand> _logger;
    
    public class Settings : CommandSettings
    {
        [CommandOption("--source-uri")]
        [Description("Source data URI (supports kafka://topic)")]
        public string SourceUri { get; init; } = "";
        
        [CommandOption("--target-uri")]
        [Description("Target data URI (supports kafka://topic)")]
        public string TargetUri { get; init; } = "";
        
        [CommandOption("--source-format")]
        [Description("Source format")]
        [DefaultValue(DataFormat.Parquet)]
        public DataFormat SourceFormat { get; init; } = DataFormat.Parquet;
        
        [CommandOption("--target-format")]
        [Description("Target format")]
        [DefaultValue(DataFormat.Parquet)]
        public DataFormat TargetFormat { get; init; } = DataFormat.Parquet;
        
        [CommandOption("--streaming")]
        [Description("Enable streaming mode")]
        public bool Streaming { get; init; }
        
        [CommandOption("--batch-size")]
        [Description("Batch size for streaming")]
        [DefaultValue(100_000)]
        public int BatchSize { get; init; } = 100_000;
        
        [CommandOption("--cache")]
        [Description("Enable local caching")]
        public bool EnableCache { get; init; }
        
        [CommandOption("--cache-dir")]
        [Description("Cache directory")]
        [DefaultValue("/tmp/lakepipe_cache")]
        public string CacheDir { get; init; } = "/tmp/lakepipe_cache";
        
        [CommandOption("--cache-size")]
        [Description("Max cache size")]
        [DefaultValue("10GB")]
        public string CacheSize { get; init; } = "10GB";
        
        [CommandOption("--kafka-servers")]
        [Description("Kafka bootstrap servers (comma-separated)")]
        public string? KafkaServers { get; init; }
        
        [CommandOption("--kafka-group")]
        [Description("Kafka consumer group")]
        public string? KafkaGroup { get; init; }
        
        [CommandOption("--config")]
        [Description("Configuration file")]
        public string? ConfigFile { get; init; }
        
        [CommandOption("-v|--verbose")]
        [Description("Verbose output")]
        public bool Verbose { get; init; }
    }
    
    public RunCommand(IPipelineBuilder pipelineBuilder, ILogger<RunCommand> logger)
    {
        _pipelineBuilder = pipelineBuilder;
        _logger = logger;
    }
    
    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        // Build configuration
        var configBuilder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables("LAKEPIPE_")
            .AddCommandLine(Environment.GetCommandLineArgs());
        
        if (!string.IsNullOrEmpty(settings.ConfigFile))
        {
            configBuilder.AddJsonFile(settings.ConfigFile, optional: false);
        }
        
        var configuration = configBuilder.Build();
        var pipelineConfig = new PipelineConfigBuilder(configuration).Build(config =>
        {
            // Apply CLI overrides
            config = config with
            {
                Source = config.Source with
                {
                    Uri = settings.SourceUri,
                    Format = settings.SourceFormat
                },
                Sink = config.Sink with
                {
                    Uri = settings.TargetUri,
                    Format = settings.TargetFormat
                },
                Streaming = config.Streaming with
                {
                    Enabled = settings.Streaming,
                    BatchSize = settings.BatchSize
                },
                Cache = config.Cache with
                {
                    Enabled = settings.EnableCache,
                    CacheDirectory = settings.CacheDir,
                    MaxSize = settings.CacheSize
                }
            };
            
            if (!string.IsNullOrEmpty(settings.KafkaServers))
            {
                var kafkaConfig = new KafkaConfig
                {
                    BootstrapServers = settings.KafkaServers.Split(',').ToList(),
                    GroupId = settings.KafkaGroup
                };
                
                if (config.Source.Format == DataFormat.Kafka)
                    config = config with { Source = config.Source with { Kafka = kafkaConfig } };
                if (config.Sink.Format == DataFormat.Kafka)
                    config = config with { Sink = config.Sink with { Kafka = kafkaConfig } };
            }
        });
        
        // Create and execute pipeline with progress display
        var pipeline = _pipelineBuilder.Build(pipelineConfig);
        
        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .AutoClear(false)
            .HideCompleted(false)
            .Columns(new ProgressColumn[]
            {
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new SpinnerColumn(),
            })
            .StartAsync(async ctx =>
            {
                var task = ctx.AddTask("[green]Processing pipeline[/]");
                
                await foreach (var result in pipeline.ExecuteAsync())
                {
                    task.Increment(1);
                    
                    if (settings.Verbose)
                    {
                        AnsiConsole.MarkupLine($"[grey]Processed: {result.Metadata}[/]");
                    }
                }
            });
        
        AnsiConsole.MarkupLine("[green]✓[/] Pipeline completed successfully");
        return 0;
    }
}

// Cache management command
public class CacheCommand : Command<CacheCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [CommandArgument(0, "<action>")]
        [Description("Cache action (status|clear|cleanup)")]
        public CacheAction Action { get; init; }
        
        [CommandOption("--cache-dir")]
        [Description("Cache directory")]
        [DefaultValue("/tmp/lakepipe_cache")]
        public string CacheDir { get; init; } = "/tmp/lakepipe_cache";
    }
    
    public enum CacheAction
    {
        Status,
        Clear,
        Cleanup
    }
    
    public override int Execute(CommandContext context, Settings settings)
    {
        var cacheConfig = new CacheConfig
        {
            Enabled = true,
            CacheDirectory = settings.CacheDir,
            MaxSize = "10GB"
        };
        
        var cache = new HybridCache(cacheConfig, NullLogger<HybridCache>.Instance);
        
        switch (settings.Action)
        {
            case CacheAction.Status:
                DisplayCacheStatus(cache);
                break;
                
            case CacheAction.Clear:
                ClearCache(cache);
                break;
                
            case CacheAction.Cleanup:
                var result = cache.CleanupCacheAsync().GetAwaiter().GetResult();
                AnsiConsole.MarkupLine($"[green]✓[/] Cleaned up {result.DeletedFiles} files");
                break;
        }
        
        return 0;
    }
    
    private void DisplayCacheStatus(HybridCache cache)
    {
        var table = new Table();
        table.AddColumn("Property");
        table.AddColumn("Value");
        
        // Get cache statistics
        var stats = cache.GetStatisticsAsync().GetAwaiter().GetResult();
        
        table.AddRow("Total Size", stats.TotalSize.Bytes().Humanize());
        table.AddRow("File Count", stats.FileCount.ToString());
        table.AddRow("Memory Usage", stats.MemoryUsage.Bytes().Humanize());
        table.AddRow("Hit Rate", $"{stats.HitRate:P2}");
        
        AnsiConsole.Write(table);
    }
}
```

## 6. Advanced Streaming Patterns

### 6.1 Windowing and Time-Based Processing

```csharp
namespace Lakepipe.Streaming.Windows;

/// <summary>
/// Time-based windowing for streaming aggregations
/// </summary>
public class WindowingProcessor<TKey, TValue> : IProcessor
{
    private readonly WindowConfig _config;
    private readonly ITimeProvider _timeProvider;
    private readonly ConcurrentDictionary<WindowKey, WindowState> _windows;
    
    public WindowingProcessor(WindowConfig config, ITimeProvider timeProvider)
    {
        _config = config;
        _timeProvider = timeProvider;
        _windows = new ConcurrentDictionary<WindowKey, WindowState>();
    }
    
    public async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Timer for window triggers
        using var windowTimer = new PeriodicTimer(_config.TriggerInterval);
        
        // Process input stream and window data concurrently
        var processTask = ProcessInputStreamAsync(inputStream, cancellationToken);
        var triggerTask = TriggerWindowsAsync(windowTimer, cancellationToken);
        
        // Merge results from both tasks
        await foreach (var result in Merge(processTask, triggerTask)
            .WithCancellation(cancellationToken))
        {
            yield return result;
        }
    }
    
    private async IAsyncEnumerable<DataPart> ProcessInputStreamAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            var batch = part.Data as ArrowRecordBatch;
            if (batch == null) continue;
            
            // Extract timestamp column
            var timestampArray = batch.Column("timestamp") as TimestampArray;
            if (timestampArray == null) continue;
            
            // Process each record into appropriate windows
            for (int i = 0; i < batch.Length; i++)
            {
                var timestamp = timestampArray.GetValue(i);
                var windows = GetWindowsForTimestamp(timestamp);
                
                foreach (var window in windows)
                {
                    await window.AddRecordAsync(batch, i);
                }
            }
        }
    }
    
    private IEnumerable<WindowState> GetWindowsForTimestamp(DateTimeOffset timestamp)
    {
        return _config.WindowType switch
        {
            WindowType.Tumbling => new[] { GetOrCreateTumblingWindow(timestamp) },
            WindowType.Sliding => GetSlidingWindows(timestamp),
            WindowType.Session => new[] { GetOrCreateSessionWindow(timestamp) },
            _ => Array.Empty<WindowState>()
        };
    }
}

/// <summary>
/// Stateful stream processing with checkpointing
/// </summary>
public class StatefulProcessor<TState> : IProcessor where TState : class, new()
{
    private readonly IStateStore<TState> _stateStore;
    private readonly ICheckpointManager _checkpointManager;
    private readonly TimeSpan _checkpointInterval;
    private TState _state;
    private long _processedCount;
    
    public StatefulProcessor(
        IStateStore<TState> stateStore,
        ICheckpointManager checkpointManager,
        TimeSpan checkpointInterval)
    {
        _stateStore = stateStore;
        _checkpointManager = checkpointManager;
        _checkpointInterval = checkpointInterval;
        _state = new TState();
    }
    
    public async Task InitializeAsync()
    {
        // Restore state from last checkpoint
        var checkpoint = await _checkpointManager.GetLatestCheckpointAsync();
        if (checkpoint != null)
        {
            _state = await _stateStore.LoadStateAsync(checkpoint.StateId);
            _processedCount = checkpoint.ProcessedCount;
        }
    }
    
    public async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var checkpointTimer = new PeriodicTimer(_checkpointInterval);
        var lastCheckpoint = DateTime.UtcNow;
        
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            // Process with state
            var result = await ProcessWithStateAsync(part, _state);
            _processedCount += part.Metadata.MessageCount;
            
            yield return result;
            
            // Periodic checkpointing
            if (await checkpointTimer.WaitForNextTickAsync(cancellationToken))
            {
                await CreateCheckpointAsync();
            }
        }
        
        // Final checkpoint
        await CreateCheckpointAsync();
    }
    
    private async Task CreateCheckpointAsync()
    {
        var stateId = Guid.NewGuid().ToString();
        await _stateStore.SaveStateAsync(stateId, _state);
        
        await _checkpointManager.SaveCheckpointAsync(new Checkpoint
        {
            StateId = stateId,
            ProcessedCount = _processedCount,
            Timestamp = DateTime.UtcNow
        });
    }
}
```

### 6.2 Advanced Backpressure and Flow Control

```csharp
namespace Lakepipe.Streaming.FlowControl;

/// <summary>
/// Adaptive backpressure controller with dynamic throttling
/// </summary>
public class AdaptiveBackpressureController
{
    private readonly BackpressureConfig _config;
    private readonly IMetricsCollector _metrics;
    private readonly SemaphoreSlim _throttle;
    private volatile int _currentLimit;
    private readonly Timer _adaptTimer;
    
    public AdaptiveBackpressureController(
        BackpressureConfig config,
        IMetricsCollector metrics)
    {
        _config = config;
        _metrics = metrics;
        _currentLimit = config.InitialLimit;
        _throttle = new SemaphoreSlim(_currentLimit, config.MaxLimit);
        
        // Adapt limits based on system metrics
        _adaptTimer = new Timer(
            _ => AdaptLimits(),
            null,
            config.AdaptInterval,
            config.AdaptInterval);
    }
    
    public async Task<T> ExecuteWithBackpressureAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        await _throttle.WaitAsync(cancellationToken);
        
        try
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await operation();
            
            _metrics.RecordOperationLatency(stopwatch.Elapsed);
            return result;
        }
        finally
        {
            _throttle.Release();
        }
    }
    
    private void AdaptLimits()
    {
        var metrics = _metrics.GetCurrentMetrics();
        
        // Calculate pressure indicators
        var cpuPressure = metrics.CpuUsage / 100.0;
        var memoryPressure = metrics.MemoryUsage / metrics.TotalMemory;
        var latencyPressure = metrics.P99Latency.TotalMilliseconds / _config.TargetLatencyMs;
        
        var maxPressure = Math.Max(cpuPressure, Math.Max(memoryPressure, latencyPressure));
        
        if (maxPressure > _config.HighPressureThreshold)
        {
            // Decrease limit
            var newLimit = Math.Max(
                _config.MinLimit,
                (int)(_currentLimit * _config.DecreaseRatio));
            
            AdjustLimit(newLimit);
        }
        else if (maxPressure < _config.LowPressureThreshold)
        {
            // Increase limit
            var newLimit = Math.Min(
                _config.MaxLimit,
                (int)(_currentLimit * _config.IncreaseRatio));
            
            AdjustLimit(newLimit);
        }
    }
    
    private void AdjustLimit(int newLimit)
    {
        var delta = newLimit - _currentLimit;
        
        if (delta > 0)
        {
            // Increase semaphore capacity
            for (int i = 0; i < delta; i++)
            {
                _throttle.Release();
            }
        }
        else if (delta < 0)
        {
            // Decrease by waiting for slots
            Task.Run(async () =>
            {
                for (int i = 0; i < -delta; i++)
                {
                    await _throttle.WaitAsync();
                }
            });
        }
        
        _currentLimit = newLimit;
        _metrics.RecordBackpressureLimit(newLimit);
    }
}

/// <summary>
/// Circuit breaker for stream processing resilience
/// </summary>
public class StreamCircuitBreaker
{
    private readonly CircuitBreakerConfig _config;
    private readonly ILogger _logger;
    private CircuitState _state = CircuitState.Closed;
    private int _failureCount;
    private DateTime _lastFailure;
    private readonly SemaphoreSlim _stateLock;
    
    public async Task<Result<T>> ExecuteAsync<T>(
        Func<Task<T>> operation,
        string operationName,
        CancellationToken cancellationToken = default)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            if (_state == CircuitState.Open)
            {
                if (DateTime.UtcNow - _lastFailure > _config.OpenDuration)
                {
                    _state = CircuitState.HalfOpen;
                    _logger.LogInformation("Circuit breaker half-open for {Operation}", operationName);
                }
                else
                {
                    return Result<T>.Failure("Circuit breaker is open");
                }
            }
        }
        finally
        {
            _stateLock.Release();
        }
        
        try
        {
            var result = await operation();
            await OnSuccessAsync();
            return Result<T>.Success(result);
        }
        catch (Exception ex)
        {
            await OnFailureAsync(ex, operationName);
            return Result<T>.Failure($"Operation failed: {ex.Message}");
        }
    }
    
    private async Task OnSuccessAsync()
    {
        await _stateLock.WaitAsync();
        try
        {
            if (_state == CircuitState.HalfOpen)
            {
                _state = CircuitState.Closed;
                _failureCount = 0;
                _logger.LogInformation("Circuit breaker closed");
            }
        }
        finally
        {
            _stateLock.Release();
        }
    }
    
    private async Task OnFailureAsync(Exception ex, string operationName)
    {
        await _stateLock.WaitAsync();
        try
        {
            _failureCount++;
            _lastFailure = DateTime.UtcNow;
            
            if (_failureCount >= _config.FailureThreshold)
            {
                _state = CircuitState.Open;
                _logger.LogError(ex, "Circuit breaker opened for {Operation} after {Count} failures",
                    operationName, _failureCount);
            }
        }
        finally
        {
            _stateLock.Release();
        }
    }
}
```

### 6.3 Stream Processing Best Practices

1. **CPU Affinity Guidelines**
   - Dedicate cores to stages based on workload characteristics
   - Keep source/sink on separate NUMA nodes from transforms
   - Use hyperthreading cores for I/O-bound operations
   - Reserve physical cores for CPU-intensive transforms

2. **Memory Management**
   - Use `ArrayPool<T>` for temporary buffers
   - Implement sliding window buffers with circular arrays
   - Pre-allocate Arrow builders with expected capacity
   - Use memory-mapped files for large intermediate results

3. **Backpressure Strategies**
   - Implement multi-level backpressure (local and global)
   - Use bounded channels with `Wait` mode for natural flow control
   - Monitor queue depths and adjust dynamically
   - Implement circuit breakers for downstream protection

4. **Fault Tolerance**
   - Checkpoint state at regular intervals
   - Use idempotent operations where possible
   - Implement dead letter queues for poison messages
   - Design for at-least-once delivery with deduplication

5. **Monitoring and Observability**
   - Track per-stage latencies and throughput
   - Monitor CPU cache hit rates with hardware counters
   - Export metrics to OpenTelemetry
   - Implement distributed tracing for debugging

## 7. Implementation Plan

### Phase 1: Core Infrastructure (Weeks 1-2)
- [ ] Set up .NET 10 solution structure
- [ ] Implement base processor interfaces and Result types
- [ ] Create configuration system with Microsoft.Extensions.Configuration
- [ ] Set up Serilog with structured logging
- [ ] Implement resource monitoring with diagnostics
- [ ] Create CLI foundation with Spectre.Console

### Phase 2: Local Cache System (Weeks 3-4)
- [ ] Implement HybridCache with FASTER backend
- [ ] Add memory cache tier with size limits
- [ ] Create cache validation and TTL management
- [ ] Implement cache-aware processors
- [ ] Add cache management utilities and CLI
- [ ] Performance benchmarking

### Phase 3: Data Sources (Weeks 5-6)
- [ ] Implement Parquet source with Parquet.NET
- [ ] Implement S3 source with AWSSDK.S3
- [ ] Implement CSV source with CsvHelper
- [ ] Add Arrow IPC stream source
- [ ] Implement DuckDB source with DuckDB.NET
- [ ] Add Iceberg source support

### Phase 4: Kafka Integration (Weeks 7-8)
- [ ] Implement Kafka source with Confluent.Kafka
- [ ] Implement Kafka sink processor
- [ ] Add JSON/Avro/Protobuf serialization
- [ ] Create Kafka utilities and health checks
- [ ] Add Kafka CLI commands
- [ ] High-throughput optimization

### Phase 5: Data Sinks (Weeks 9-10)
- [ ] Implement Parquet sink
- [ ] Implement S3 sink with multipart upload
- [ ] Add Iceberg sink with transactions
- [ ] Add Delta Lake sink
- [ ] Enhance Kafka sink with partitioning

### Phase 6: Transformation Engine (Weeks 11-12)
- [ ] Implement Arrow compute transformations
- [ ] Implement DuckDB SQL processor
- [ ] Add user-defined transformations
- [ ] Create transform composition
- [ ] Zero-allocation optimizations

### Phase 7: Stream Processing & Integration (Weeks 13-14)
- [ ] Implement Channel-based streaming
- [ ] Add concurrent execution with dataflow
- [ ] Implement backpressure handling
- [ ] Create pipeline operators
- [ ] Integrate caching with streaming

### Phase 8: Testing & Documentation (Weeks 15-16)
- [ ] Implement comprehensive test suite
- [ ] Add performance benchmarks
- [ ] Create API documentation
- [ ] Add example pipelines
- [ ] Performance profiling and optimization

## 7. Performance Targets

### 7.1 Cache Performance
- **Cache Hit Ratio**: >95% for immutable datasets
- **Cache Lookup**: <5ms memory, <50ms disk (FASTER)
- **Zero-Copy**: Direct memory mapping for large files
- **Concurrent Access**: Lock-free reads with FASTER

### 7.2 Streaming Performance with TPL Dataflow
- **Throughput**: 500K+ messages/second with CPU affinity
- **Latency**: <10ms end-to-end with pinned cores
- **Backpressure**: Automatic flow control with bounded channels
- **CPU Cache Efficiency**: 95%+ L1/L2 cache hits with affinity
- **Pipeline Efficiency**: Near-linear scaling with core count

### 7.3 Kafka Specific Performance
- **Consumer Throughput**: 1M+ messages/second across partitions
- **Producer Batching**: 32KB batches with <100ms linger
- **Offset Management**: Async commits with <50ms overhead
- **Rebalancing**: <5 second consumer group rebalancing
- **Memory Buffering**: 1GB in-flight messages with bounded channels

### 7.4 Overall System Performance
- **Allocation Rate**: <1KB per MB processed
- **GC Pressure**: Gen2 collections <1 per hour
- **CPU Efficiency**: 95%+ CPU utilization with affinity
- **Memory Efficiency**: Constant memory usage under streaming load
- **Core Utilization**: Dedicated cores for source/transform/sink stages
- **NUMA Awareness**: Optional NUMA-aware memory allocation

### 7.5 Continuous Streaming Targets
- **Uptime**: 99.99% availability for infinite streams
- **Recovery Time**: <1 second from transient failures
- **State Management**: Checkpoint/restore in <500ms
- **Dynamic Scaling**: Add/remove processors without restart
- **Monitoring Overhead**: <1% performance impact

This .NET implementation leverages TPL Dataflow for advanced streaming scenarios, CPU affinity for optimal cache locality, and bounded channels for backpressure handling, enabling true infinite stream processing with predictable performance characteristics.