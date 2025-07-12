using Lakepipe.Core;
using Lakepipe.Core.Results;

namespace Lakepipe.Cache;

/// <summary>
/// Interface for hybrid memory/disk cache operations.
/// </summary>
public interface IHybridCache : IDisposable
{
    /// <summary>
    /// Gets a value from the cache asynchronously.
    /// </summary>
    Task<Result<T>> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class;
    
    /// <summary>
    /// Sets a value in the cache asynchronously.
    /// </summary>
    Task<Result<Unit>> SetAsync<T>(string key, T value, TimeSpan? expiration = null, CancellationToken cancellationToken = default) where T : class;
    
    /// <summary>
    /// Removes a value from the cache asynchronously.
    /// </summary>
    Task<Result<bool>> RemoveAsync(string key, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Checks if a key exists in the cache.
    /// </summary>
    Task<Result<bool>> ExistsAsync(string key, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets or sets a value in the cache with a factory function.
    /// </summary>
    Task<Result<T>> GetOrSetAsync<T>(string key, Func<Task<T>> factory, TimeSpan? expiration = null, CancellationToken cancellationToken = default) where T : class;
    
    /// <summary>
    /// Flushes all data to disk.
    /// </summary>
    Task<Result<Unit>> FlushAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets cache statistics.
    /// </summary>
    CacheStatistics GetStatistics();
}

/// <summary>
/// Cache statistics.
/// </summary>
public record CacheStatistics
{
    public long Hits { get; init; }
    public long Misses { get; init; }
    public long TotalRequests => Hits + Misses;
    public double HitRatio => TotalRequests > 0 ? (double)Hits / TotalRequests : 0;
    public long MemorySize { get; init; }
    public long DiskSize { get; init; }
    public long ItemCount { get; init; }
    public DateTime LastAccess { get; init; }
}