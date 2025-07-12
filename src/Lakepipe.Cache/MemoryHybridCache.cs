using System.Collections.Concurrent;
using System.Text.Json;
using Lakepipe.Configuration.Models;
using Lakepipe.Core;
using Lakepipe.Core.Results;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Cache;

/// <summary>
/// Memory-based hybrid cache implementation with disk persistence.
/// </summary>
public class MemoryHybridCache : IHybridCache
{
    private readonly ILogger<MemoryHybridCache> _logger;
    private readonly CacheConfig _config;
    private readonly IMemoryCache _memoryCache;
    private readonly ConcurrentDictionary<string, DateTime> _keyTracker;
    private readonly string _persistencePath;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly Timer _persistenceTimer;
    private long _hits;
    private long _misses;
    private DateTime _lastAccess;
    private bool _disposed;

    public MemoryHybridCache(ILogger<MemoryHybridCache> logger, CacheConfig config)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        
        var cacheOptions = new MemoryCacheOptions
        {
            SizeLimit = ParseSize(_config.MaxSize) / 1024 // Convert to KB for memory cache
        };
        
        _memoryCache = new MemoryCache(cacheOptions);
        _keyTracker = new ConcurrentDictionary<string, DateTime>();
        _persistencePath = Path.Combine(_config.CacheDirectory, "cache_data.json");
        
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false
        };
        
        Directory.CreateDirectory(_config.CacheDirectory);
        
        // Load persisted data
        LoadFromDisk();
        
        // Set up periodic persistence
        _persistenceTimer = new Timer(
            _ => PersistToDisk(),
            null,
            TimeSpan.FromMinutes(5),
            TimeSpan.FromMinutes(5));
        
        _logger.LogInformation("Memory hybrid cache initialized with {Size} max size at {Path}", 
            _config.MaxSize, _config.CacheDirectory);
    }

    public Task<Result<T>> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            _lastAccess = DateTime.UtcNow;
            
            if (_memoryCache.TryGetValue<CacheEntry>(key, out var entry) && entry != null)
            {
                // Check expiration
                if (entry.ExpiresAt.HasValue && entry.ExpiresAt.Value < DateTime.UtcNow)
                {
                    _memoryCache.Remove(key);
                    _keyTracker.TryRemove(key, out _);
                    Interlocked.Increment(ref _misses);
                    return Task.FromResult(Result<T>.Failure($"Key '{key}' has expired"));
                }
                
                var value = JsonSerializer.Deserialize<T>(entry.Data, _jsonOptions);
                if (value == null)
                    return Task.FromResult(Result<T>.Failure("Deserialized value was null"));
                
                Interlocked.Increment(ref _hits);
                _logger.LogDebug("Cache hit for key {Key}", key);
                return Task.FromResult(Result<T>.Success(value));
            }
            
            Interlocked.Increment(ref _misses);
            _logger.LogDebug("Cache miss for key {Key}", key);
            return Task.FromResult(Result<T>.Failure($"Key '{key}' not found in cache"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting key {Key} from cache", key);
            return Task.FromResult(Result<T>.Failure($"Cache get error: {ex.Message}"));
        }
    }

    public Task<Result<Unit>> SetAsync<T>(string key, T value, TimeSpan? expiration = null, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            _lastAccess = DateTime.UtcNow;
            
            var json = JsonSerializer.Serialize(value, _jsonOptions);
            var entry = new CacheEntry
            {
                Data = json,
                CreatedAt = DateTime.UtcNow,
                ExpiresAt = expiration.HasValue ? DateTime.UtcNow.Add(expiration.Value) : null
            };
            
            var cacheEntryOptions = new MemoryCacheEntryOptions
            {
                Size = json.Length / 1024 // Size in KB
            };
            
            if (expiration.HasValue)
            {
                cacheEntryOptions.AbsoluteExpirationRelativeToNow = expiration;
            }
            
            _memoryCache.Set(key, entry, cacheEntryOptions);
            _keyTracker[key] = DateTime.UtcNow;
            
            _logger.LogDebug("Set key {Key} in cache", key);
            return Task.FromResult(Result<Unit>.Success(Unit.Value));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error setting key {Key} in cache", key);
            return Task.FromResult(Result<Unit>.Failure($"Cache set error: {ex.Message}"));
        }
    }

    public Task<Result<bool>> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            _lastAccess = DateTime.UtcNow;
            
            _memoryCache.Remove(key);
            var removed = _keyTracker.TryRemove(key, out _);
            
            _logger.LogDebug("Removed key {Key} from cache", key);
            return Task.FromResult(Result<bool>.Success(removed));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error removing key {Key} from cache", key);
            return Task.FromResult(Result<bool>.Failure($"Cache remove error: {ex.Message}"));
        }
    }

    public Task<Result<bool>> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            _lastAccess = DateTime.UtcNow;
            var exists = _memoryCache.TryGetValue(key, out _);
            return Task.FromResult(Result<bool>.Success(exists));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if key {Key} exists in cache", key);
            return Task.FromResult(Result<bool>.Failure($"Cache exists error: {ex.Message}"));
        }
    }

    public async Task<Result<T>> GetOrSetAsync<T>(string key, Func<Task<T>> factory, TimeSpan? expiration = null, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var getResult = await GetAsync<T>(key, cancellationToken);
            if (getResult.IsSuccess)
                return getResult;
            
            var value = await factory();
            var setResult = await SetAsync(key, value, expiration, cancellationToken);
            
            return setResult.IsSuccess 
                ? Result<T>.Success(value) 
                : Result<T>.Failure(setResult.Error ?? "Failed to set value");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in GetOrSetAsync for key {Key}", key);
            return Result<T>.Failure($"Cache GetOrSet error: {ex.Message}");
        }
    }

    public Task<Result<Unit>> FlushAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            PersistToDisk();
            _logger.LogInformation("Cache flushed to disk");
            return Task.FromResult(Result<Unit>.Success(Unit.Value));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing cache");
            return Task.FromResult(Result<Unit>.Failure($"Cache flush error: {ex.Message}"));
        }
    }

    public CacheStatistics GetStatistics()
    {
        return new CacheStatistics
        {
            Hits = _hits,
            Misses = _misses,
            MemorySize = _memoryCache.GetCurrentStatistics()?.CurrentEstimatedSize ?? 0,
            DiskSize = File.Exists(_persistencePath) ? new FileInfo(_persistencePath).Length : 0,
            ItemCount = _keyTracker.Count,
            LastAccess = _lastAccess
        };
    }

    private void LoadFromDisk()
    {
        try
        {
            if (!File.Exists(_persistencePath))
                return;
            
            var json = File.ReadAllText(_persistencePath);
            var data = JsonSerializer.Deserialize<Dictionary<string, CacheEntry>>(json, _jsonOptions);
            
            if (data != null)
            {
                foreach (var kvp in data)
                {
                    if (!kvp.Value.ExpiresAt.HasValue || kvp.Value.ExpiresAt.Value > DateTime.UtcNow)
                    {
                        var options = new MemoryCacheEntryOptions
                        {
                            Size = kvp.Value.Data.Length / 1024
                        };
                        
                        if (kvp.Value.ExpiresAt.HasValue)
                        {
                            options.AbsoluteExpiration = kvp.Value.ExpiresAt.Value;
                        }
                        
                        _memoryCache.Set(kvp.Key, kvp.Value, options);
                        _keyTracker[kvp.Key] = kvp.Value.CreatedAt;
                    }
                }
                
                _logger.LogInformation("Loaded {Count} items from disk cache", data.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading cache from disk");
        }
    }

    private void PersistToDisk()
    {
        try
        {
            var data = new Dictionary<string, CacheEntry>();
            
            foreach (var key in _keyTracker.Keys)
            {
                if (_memoryCache.TryGetValue<CacheEntry>(key, out var entry) && entry != null)
                {
                    data[key] = entry;
                }
            }
            
            var json = JsonSerializer.Serialize(data, _jsonOptions);
            File.WriteAllText(_persistencePath, json);
            
            _logger.LogDebug("Persisted {Count} items to disk", data.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error persisting cache to disk");
        }
    }

    private static long ParseSize(string size)
    {
        size = size.ToUpperInvariant().Trim();
        
        if (size.EndsWith("GB"))
            return long.Parse(size[..^2]) * 1024L * 1024L * 1024L;
        if (size.EndsWith("MB"))
            return long.Parse(size[..^2]) * 1024L * 1024L;
        if (size.EndsWith("KB"))
            return long.Parse(size[..^2]) * 1024L;
        if (size.EndsWith("TB"))
            return long.Parse(size[..^2]) * 1024L * 1024L * 1024L * 1024L;
        
        return long.Parse(size);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        try
        {
            _persistenceTimer?.Dispose();
            PersistToDisk();
            _memoryCache?.Dispose();
            _logger.LogInformation("Memory hybrid cache disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing cache");
        }
        finally
        {
            _disposed = true;
        }
    }

    private class CacheEntry
    {
        public string Data { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public DateTime? ExpiresAt { get; set; }
    }
}