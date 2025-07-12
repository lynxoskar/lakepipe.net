using Lakepipe.Configuration.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Cache;

/// <summary>
/// Service collection extensions for cache registration.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds hybrid cache services to the service collection.
    /// </summary>
    public static IServiceCollection AddLakepipeCache(
        this IServiceCollection services,
        CacheConfig? config = null)
    {
        config ??= new CacheConfig();
        
        services.AddSingleton(config);
        services.AddSingleton<IHybridCache>(provider =>
        {
            var logger = provider.GetRequiredService<ILogger<MemoryHybridCache>>();
            return new MemoryHybridCache(logger, config);
        });
        
        // Register default cache key generator
        services.AddSingleton<ICacheKeyGenerator, DefaultCacheKeyGenerator>();
        
        return services;
    }
    
    /// <summary>
    /// Adds hybrid cache services with custom configuration.
    /// </summary>
    public static IServiceCollection AddLakepipeCache(
        this IServiceCollection services,
        Action<CacheConfig> configureOptions)
    {
        var config = new CacheConfig();
        configureOptions(config);
        return services.AddLakepipeCache(config);
    }
}