using System.ComponentModel.DataAnnotations;
using Lakepipe.Configuration.Models;

namespace Lakepipe.Configuration.Validation;

/// <summary>
/// Validates pipeline configuration.
/// </summary>
public static class ConfigurationValidator
{
    /// <summary>
    /// Validates the entire pipeline configuration.
    /// </summary>
    public static ValidationResult Validate(PipelineConfig config)
    {
        var errors = new List<ValidationError>();

        // Validate using data annotations
        var context = new ValidationContext(config);
        var results = new List<System.ComponentModel.DataAnnotations.ValidationResult>();
        
        if (!Validator.TryValidateObject(config, context, results, true))
        {
            errors.AddRange(results.Select(r => new ValidationError(r.ErrorMessage ?? "Unknown error", r.MemberNames)));
        }

        // Custom validation
        errors.AddRange(ValidateSource(config.Source));
        errors.AddRange(ValidateSink(config.Sink));
        errors.AddRange(ValidateCache(config.Cache));
        errors.AddRange(ValidateStreaming(config.Streaming));
        errors.AddRange(ValidateKafka(config.Source.Kafka, "Source"));
        errors.AddRange(ValidateKafka(config.Sink.Kafka, "Sink"));

        return new ValidationResult(errors);
    }

    private static IEnumerable<ValidationError> ValidateSource(SourceConfig source)
    {
        if (string.IsNullOrWhiteSpace(source.Uri))
        {
            yield return new ValidationError("Source URI is required", new[] { nameof(source.Uri) });
        }
        else if (!IsValidUri(source.Uri))
        {
            yield return new ValidationError($"Invalid source URI: {source.Uri}", new[] { nameof(source.Uri) });
        }
    }

    private static IEnumerable<ValidationError> ValidateSink(SinkConfig sink)
    {
        if (string.IsNullOrWhiteSpace(sink.Uri))
        {
            yield return new ValidationError("Sink URI is required", new[] { nameof(sink.Uri) });
        }
        else if (!IsValidUri(sink.Uri))
        {
            yield return new ValidationError($"Invalid sink URI: {sink.Uri}", new[] { nameof(sink.Uri) });
        }

        if (sink.PartitionBy != null && sink.PartitionBy.Any(string.IsNullOrWhiteSpace))
        {
            yield return new ValidationError("Partition columns cannot be empty", new[] { nameof(sink.PartitionBy) });
        }
    }

    private static IEnumerable<ValidationError> ValidateCache(CacheConfig cache)
    {
        if (cache.Enabled)
        {
            if (string.IsNullOrWhiteSpace(cache.CacheDirectory))
            {
                yield return new ValidationError("Cache directory is required when cache is enabled", 
                    new[] { nameof(cache.CacheDirectory) });
            }

            if (!IsValidSize(cache.MaxSize))
            {
                yield return new ValidationError($"Invalid cache size: {cache.MaxSize}", 
                    new[] { nameof(cache.MaxSize) });
            }

            if (cache.TtlDays.HasValue && cache.TtlDays.Value <= 0)
            {
                yield return new ValidationError("Cache TTL must be positive", 
                    new[] { nameof(cache.TtlDays) });
            }

            if (cache.MemoryCachePercentage is <= 0 or > 100)
            {
                yield return new ValidationError("Memory cache percentage must be between 1 and 100", 
                    new[] { nameof(cache.MemoryCachePercentage) });
            }
        }
    }

    private static IEnumerable<ValidationError> ValidateStreaming(StreamingConfig streaming)
    {
        if (streaming.EnableCpuAffinity)
        {
            var totalCores = Environment.ProcessorCount;
            
            foreach (var core in streaming.SourceCpuAffinity)
            {
                if (core < 0 || core >= totalCores)
                {
                    yield return new ValidationError($"Invalid CPU core {core}. Must be between 0 and {totalCores - 1}", 
                        new[] { nameof(streaming.SourceCpuAffinity) });
                }
            }

            foreach (var core in streaming.SinkCpuAffinity)
            {
                if (core < 0 || core >= totalCores)
                {
                    yield return new ValidationError($"Invalid CPU core {core}. Must be between 0 and {totalCores - 1}", 
                        new[] { nameof(streaming.SinkCpuAffinity) });
                }
            }

            if (streaming.TransformCpuAffinity != null)
            {
                foreach (var cores in streaming.TransformCpuAffinity)
                {
                    foreach (var core in cores)
                    {
                        if (core < 0 || core >= totalCores)
                        {
                            yield return new ValidationError($"Invalid CPU core {core}. Must be between 0 and {totalCores - 1}", 
                                new[] { nameof(streaming.TransformCpuAffinity) });
                        }
                    }
                }
            }
        }

        if (streaming.MaxMemoryPerPipelineMB <= 0)
        {
            yield return new ValidationError("Max memory per pipeline must be positive", 
                new[] { nameof(streaming.MaxMemoryPerPipelineMB) });
        }

        if (streaming.MaxMessagesInFlight <= 0)
        {
            yield return new ValidationError("Max messages in flight must be positive", 
                new[] { nameof(streaming.MaxMessagesInFlight) });
        }
    }

    private static IEnumerable<ValidationError> ValidateKafka(KafkaConfig? kafka, string prefix)
    {
        if (kafka == null)
            yield break;

        if (!kafka.BootstrapServers.Any() || kafka.BootstrapServers.All(string.IsNullOrWhiteSpace))
        {
            yield return new ValidationError($"{prefix} Kafka bootstrap servers are required", 
                new[] { $"{prefix}.{nameof(kafka.BootstrapServers)}" });
        }

        if (string.IsNullOrWhiteSpace(kafka.Topic))
        {
            yield return new ValidationError($"{prefix} Kafka topic is required", 
                new[] { $"{prefix}.{nameof(kafka.Topic)}" });
        }

        if (kafka.BatchSize <= 0)
        {
            yield return new ValidationError($"{prefix} Kafka batch size must be positive", 
                new[] { $"{prefix}.{nameof(kafka.BatchSize)}" });
        }

        if (kafka.MaxPollRecords <= 0)
        {
            yield return new ValidationError($"{prefix} Kafka max poll records must be positive", 
                new[] { $"{prefix}.{nameof(kafka.MaxPollRecords)}" });
        }
    }

    private static bool IsValidUri(string uri)
    {
        if (string.IsNullOrWhiteSpace(uri))
            return false;

        // Check for supported URI schemes
        var supportedSchemes = new[] { "file://", "s3://", "kafka://", "http://", "https://" };
        return supportedSchemes.Any(scheme => uri.StartsWith(scheme, StringComparison.OrdinalIgnoreCase)) ||
               Path.IsPathRooted(uri);
    }

    private static bool IsValidSize(string size)
    {
        if (string.IsNullOrWhiteSpace(size))
            return false;

        size = size.ToUpperInvariant();
        
        if (size.EndsWith("GB") || size.EndsWith("MB") || size.EndsWith("KB") || size.EndsWith("TB"))
        {
            var numPart = size[..^2];
            return double.TryParse(numPart, out var value) && value > 0;
        }

        return false;
    }
}

/// <summary>
/// Represents a validation error.
/// </summary>
public record ValidationError(string Message, IEnumerable<string> MemberNames);

/// <summary>
/// Represents the result of configuration validation.
/// </summary>
public class ValidationResult
{
    public IReadOnlyList<ValidationError> Errors { get; }
    public bool IsValid => !Errors.Any();

    public ValidationResult(IEnumerable<ValidationError> errors)
    {
        Errors = errors.ToList();
    }

    public void ThrowIfInvalid()
    {
        if (!IsValid)
        {
            var message = string.Join("\n", Errors.Select(e => $"- {e.Message} ({string.Join(", ", e.MemberNames)})"));
            throw new ValidationException($"Configuration validation failed:\n{message}");
        }
    }
}