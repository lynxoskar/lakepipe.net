using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Lakepipe.Core.Processors;
using Lakepipe.Core.Streams;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Cache;

/// <summary>
/// Processor wrapper that adds caching capabilities to any processor.
/// </summary>
public class CachedProcessor : ProcessorBase
{
    private readonly IProcessor _inner;
    private readonly IHybridCache _cache;
    private readonly ICacheKeyGenerator _keyGenerator;
    private readonly TimeSpan? _cacheExpiration;
    private readonly bool _cacheOnlySuccessful;
    private long _cacheHits;
    private long _cacheMisses;

    public CachedProcessor(
        IProcessor inner,
        IHybridCache cache,
        ILogger logger,
        ICacheKeyGenerator? keyGenerator = null,
        TimeSpan? cacheExpiration = null,
        bool cacheOnlySuccessful = true) : base(logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _keyGenerator = keyGenerator ?? new DefaultCacheKeyGenerator();
        _cacheExpiration = cacheExpiration;
        _cacheOnlySuccessful = cacheOnlySuccessful;
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var input in inputStream.WithCancellation(cancellationToken))
        {
            var cacheKey = _keyGenerator.GenerateKey(_inner.GetType().Name, input);
            
            // Try to get from cache
            var cacheResult = await _cache.GetAsync<CachedDataPart>(cacheKey, cancellationToken);
            
            if (cacheResult.IsSuccess)
            {
                Interlocked.Increment(ref _cacheHits);
                Logger.LogDebug("Cache hit for key {CacheKey}", cacheKey);
                
                // Update metadata to indicate cache hit
                var cachedPart = cacheResult.Value.DataPart with
                {
                    Metadata = cacheResult.Value.DataPart.Metadata with
                    {
                        CacheHit = true
                    }
                };
                
                yield return cachedPart;
                continue;
            }
            
            Interlocked.Increment(ref _cacheMisses);
            Logger.LogDebug("Cache miss for key {CacheKey}", cacheKey);
            
            // Process through inner processor
            var processed = false;
            await foreach (var result in _inner.ProcessAsync(SingleItemStream(input), cancellationToken))
            {
                processed = true;
                
                // Cache the result if configured to do so
                if (!_cacheOnlySuccessful || IsSuccessful(result))
                {
                    var cachedItem = new CachedDataPart { DataPart = result };
                    await _cache.SetAsync(cacheKey, cachedItem, _cacheExpiration, cancellationToken);
                    Logger.LogDebug("Cached result for key {CacheKey}", cacheKey);
                }
                
                yield return result;
            }
            
            // If no output was produced and we're caching unsuccessful results
            if (!processed && !_cacheOnlySuccessful)
            {
                // Store the input as-is to indicate no output was produced
                var cachedItem = new CachedDataPart { DataPart = input };
                await _cache.SetAsync(cacheKey, cachedItem, _cacheExpiration, cancellationToken);
            }
        }
        
        // Log final statistics
        var stats = _cache.GetStatistics();
        Logger.LogInformation(
            "CachedProcessor statistics - Hits: {Hits}, Misses: {Misses}, " +
            "Hit ratio: {HitRatio:P2}, Cache stats: {@CacheStats}",
            _cacheHits, _cacheMisses,
            _cacheHits + _cacheMisses > 0 ? (double)_cacheHits / (_cacheHits + _cacheMisses) : 0,
            stats);
    }

    private static async IAsyncEnumerable<DataPart> SingleItemStream(DataPart item)
    {
        yield return item;
        await Task.CompletedTask;
    }

    private static bool IsSuccessful(DataPart dataPart)
    {
        // Consider a data part successful if it has data and no error indication
        return dataPart.Data != null;
    }
}

/// <summary>
/// Interface for generating cache keys.
/// </summary>
public interface ICacheKeyGenerator
{
    string GenerateKey(string processorName, DataPart dataPart);
}

/// <summary>
/// Default cache key generator using content hash.
/// </summary>
public class DefaultCacheKeyGenerator : ICacheKeyGenerator
{
    private readonly JsonSerializerOptions _jsonOptions;

    public DefaultCacheKeyGenerator()
    {
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false
        };
    }

    public string GenerateKey(string processorName, DataPart dataPart)
    {
        // Create a key based on processor name and data content hash
        var contentHash = ComputeHash(dataPart);
        return $"lakepipe:{processorName}:{contentHash}";
    }

    private string ComputeHash(DataPart dataPart)
    {
        // Serialize relevant parts of the data for hashing
        var hashInput = new
        {
            dataPart.Metadata.Source,
            dataPart.Metadata.StreamPosition,
            dataPart.Metadata.Topic,
            dataPart.Metadata.PartitionOffsets,
            dataPart.Metadata.MessageCount,
            DataType = dataPart.Data?.GetType().FullName
        };
        
        var json = JsonSerializer.Serialize(hashInput, _jsonOptions);
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(json));
        return Convert.ToBase64String(hashBytes);
    }
}

/// <summary>
/// Wrapper for cached DataPart.
/// </summary>
public class CachedDataPart
{
    public DataPart DataPart { get; init; } = default!;
    public DateTime CachedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Extension methods for adding caching to processors.
/// </summary>
public static class CachedProcessorExtensions
{
    /// <summary>
    /// Wraps a processor with caching capabilities.
    /// </summary>
    public static IProcessor WithCaching(
        this IProcessor processor,
        IHybridCache cache,
        ILogger logger,
        TimeSpan? expiration = null,
        ICacheKeyGenerator? keyGenerator = null,
        bool cacheOnlySuccessful = true)
    {
        return new CachedProcessor(processor, cache, logger, keyGenerator, expiration, cacheOnlySuccessful);
    }
}