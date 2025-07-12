using Lakepipe.Configuration.Models;
using Lakepipe.Core.Streams;
using Microsoft.Extensions.Configuration;

namespace Lakepipe.Configuration.Builders;

/// <summary>
/// Builder for creating pipeline configuration from various sources.
/// </summary>
public class PipelineConfigBuilder
{
    private readonly IConfiguration _configuration;
    private readonly string _sectionName;

    /// <summary>
    /// Creates a new pipeline configuration builder.
    /// </summary>
    /// <param name="configuration">The configuration source.</param>
    /// <param name="sectionName">The configuration section name.</param>
    public PipelineConfigBuilder(IConfiguration configuration, string sectionName = "Lakepipe")
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _sectionName = sectionName;
    }

    /// <summary>
    /// Builds the pipeline configuration with optional overrides.
    /// </summary>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The built pipeline configuration.</returns>
    public PipelineConfig Build(Action<PipelineConfig>? configure = null)
    {
        // Start with defaults
        var config = new PipelineConfig();

        // Bind from configuration section if it exists
        var section = _configuration.GetSection(_sectionName);
        if (section.Exists())
        {
            section.Bind(config);
        }

        // Apply environment variable overrides
        config = ApplyEnvironmentOverrides(config);

        // Apply programmatic overrides
        configure?.Invoke(config);

        // Validate configuration
        ValidateConfiguration(config);

        return config;
    }

    /// <summary>
    /// Creates a configuration builder with standard sources.
    /// </summary>
    public static IConfigurationBuilder CreateDefaultBuilder(string? basePath = null)
    {
        var builder = new ConfigurationBuilder();

        if (!string.IsNullOrEmpty(basePath))
        {
            builder.SetBasePath(basePath);
        }

        return builder
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables("LAKEPIPE_");
    }

    private PipelineConfig ApplyEnvironmentOverrides(PipelineConfig config)
    {
        // Cache configuration from environment
        var cacheEnabled = _configuration.GetValue<bool?>("LAKEPIPE_CACHE_ENABLED");
        var cacheDir = _configuration.GetValue<string>("LAKEPIPE_CACHE_DIR");
        var cacheMaxSize = _configuration.GetValue<string>("LAKEPIPE_CACHE_MAX_SIZE");
        var cacheCompression = _configuration.GetValue<string>("LAKEPIPE_CACHE_COMPRESSION");
        var cacheTtlDays = _configuration.GetValue<int?>("LAKEPIPE_CACHE_TTL_DAYS");
        var cacheImmutableOnly = _configuration.GetValue<bool?>("LAKEPIPE_CACHE_IMMUTABLE_ONLY");
        var cacheIncludePatterns = _configuration.GetValue<string>("LAKEPIPE_CACHE_INCLUDE_PATTERNS");
        var cacheExcludePatterns = _configuration.GetValue<string>("LAKEPIPE_CACHE_EXCLUDE_PATTERNS");

        if (cacheEnabled.HasValue || !string.IsNullOrEmpty(cacheDir) || !string.IsNullOrEmpty(cacheMaxSize))
        {
            config = config with
            {
                Cache = config.Cache with
                {
                    Enabled = cacheEnabled ?? config.Cache.Enabled,
                    CacheDirectory = cacheDir ?? config.Cache.CacheDirectory,
                    MaxSize = cacheMaxSize ?? config.Cache.MaxSize,
                    Compression = ParseEnum<Lakepipe.Core.Streams.CompressionType>(cacheCompression) ?? config.Cache.Compression,
                    TtlDays = cacheTtlDays ?? config.Cache.TtlDays,
                    ImmutableOnly = cacheImmutableOnly ?? config.Cache.ImmutableOnly,
                    IncludePatterns = ParseList(cacheIncludePatterns) ?? config.Cache.IncludePatterns,
                    ExcludePatterns = ParseList(cacheExcludePatterns) ?? config.Cache.ExcludePatterns
                }
            };
        }

        // Source configuration from environment
        var sourceUri = _configuration.GetValue<string>("LAKEPIPE_SOURCE_URI");
        var sourceFormat = _configuration.GetValue<string>("LAKEPIPE_SOURCE_FORMAT");
        var sourceCompression = _configuration.GetValue<string>("LAKEPIPE_SOURCE_COMPRESSION");

        if (!string.IsNullOrEmpty(sourceUri) || !string.IsNullOrEmpty(sourceFormat))
        {
            config = config with
            {
                Source = config.Source with
                {
                    Uri = sourceUri ?? config.Source.Uri,
                    Format = ParseEnum<DataFormat>(sourceFormat) ?? config.Source.Format,
                    Compression = ParseEnum<Lakepipe.Core.Streams.CompressionType>(sourceCompression),
                    Cache = config.Cache // Use global cache config
                }
            };
        }

        // Kafka configuration from environment
        var kafkaServers = _configuration.GetValue<string>("LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS");
        var kafkaTopic = _configuration.GetValue<string>("LAKEPIPE_KAFKA_SOURCE_TOPIC");
        var kafkaGroup = _configuration.GetValue<string>("LAKEPIPE_KAFKA_GROUP_ID");

        if (!string.IsNullOrEmpty(kafkaServers) && config.Source.Format == DataFormat.Kafka)
        {
            config = config with
            {
                Source = config.Source with
                {
                    Kafka = new KafkaConfig
                    {
                        BootstrapServers = ParseList(kafkaServers) ?? new List<string> { "localhost:9092" },
                        Topic = kafkaTopic ?? "",
                        GroupId = kafkaGroup
                    }
                }
            };
        }

        // Sink configuration from environment
        var sinkUri = _configuration.GetValue<string>("LAKEPIPE_SINK_URI");
        var sinkFormat = _configuration.GetValue<string>("LAKEPIPE_SINK_FORMAT");
        var sinkCompression = _configuration.GetValue<string>("LAKEPIPE_SINK_COMPRESSION");
        var sinkPartitionBy = _configuration.GetValue<string>("LAKEPIPE_SINK_PARTITION_BY");

        if (!string.IsNullOrEmpty(sinkUri) || !string.IsNullOrEmpty(sinkFormat))
        {
            config = config with
            {
                Sink = config.Sink with
                {
                    Uri = sinkUri ?? config.Sink.Uri,
                    Format = ParseEnum<DataFormat>(sinkFormat) ?? config.Sink.Format,
                    Compression = ParseEnum<Lakepipe.Core.Streams.CompressionType>(sinkCompression),
                    PartitionBy = ParseList(sinkPartitionBy)
                }
            };
        }

        // Streaming configuration from environment
        var streamingBatchSize = _configuration.GetValue<int?>("LAKEPIPE_STREAMING_BATCH_SIZE");
        var streamingMaxMemory = _configuration.GetValue<string>("LAKEPIPE_STREAMING_MAX_MEMORY");
        var streamingConcurrentTasks = _configuration.GetValue<int?>("LAKEPIPE_STREAMING_CONCURRENT_TASKS");

        if (streamingBatchSize.HasValue || !string.IsNullOrEmpty(streamingMaxMemory))
        {
            config = config with
            {
                Streaming = config.Streaming with
                {
                    SinkBatchSize = streamingBatchSize ?? config.Streaming.SinkBatchSize,
                    MaxMemoryPerPipelineMB = ParseMemorySize(streamingMaxMemory) ?? config.Streaming.MaxMemoryPerPipelineMB,
                    DefaultParallelism = streamingConcurrentTasks ?? config.Streaming.DefaultParallelism
                }
            };
        }

        return config;
    }

    private static T? ParseEnum<T>(string? value) where T : struct, Enum
    {
        if (string.IsNullOrEmpty(value))
            return null;

        return Enum.TryParse<T>(value, ignoreCase: true, out var result) ? result : null;
    }

    private static List<string>? ParseList(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return null;

        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }

    private static int? ParseMemorySize(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return null;

        value = value.ToUpperInvariant();
        
        if (value.EndsWith("GB"))
        {
            if (int.TryParse(value[..^2], out var gb))
                return gb * 1024;
        }
        else if (value.EndsWith("MB"))
        {
            if (int.TryParse(value[..^2], out var mb))
                return mb;
        }

        return null;
    }

    private void ValidateConfiguration(PipelineConfig config)
    {
        var errors = new List<string>();

        // Validate source
        if (string.IsNullOrEmpty(config.Source.Uri))
        {
            errors.Add("Source URI is required");
        }

        // Validate sink
        if (string.IsNullOrEmpty(config.Sink.Uri))
        {
            errors.Add("Sink URI is required");
        }

        // Validate Kafka config if needed
        if (config.Source.Format == DataFormat.Kafka && config.Source.Kafka != null)
        {
            if (!config.Source.Kafka.BootstrapServers.Any())
            {
                errors.Add("Kafka bootstrap servers are required");
            }
            if (string.IsNullOrEmpty(config.Source.Kafka.Topic))
            {
                errors.Add("Kafka topic is required");
            }
        }

        // Validate streaming config
        if (config.Streaming.EnableCpuAffinity)
        {
            var totalCores = Environment.ProcessorCount;
            var allAffinityCores = config.Streaming.SourceCpuAffinity
                .Concat(config.Streaming.SinkCpuAffinity)
                .Concat(config.Streaming.TransformCpuAffinity?.SelectMany(x => x) ?? Array.Empty<int>())
                .Distinct()
                .ToList();

            if (allAffinityCores.Any(core => core >= totalCores))
            {
                errors.Add($"CPU affinity cores must be less than total cores ({totalCores})");
            }
        }

        if (errors.Any())
        {
            throw new InvalidOperationException($"Configuration validation failed:\n{string.Join("\n", errors)}");
        }
    }
}