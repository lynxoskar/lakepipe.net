using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Lakepipe.Configuration.Models;

namespace Lakepipe.Configuration.Logging;

/// <summary>
/// Extension methods for configuring Serilog logging.
/// </summary>
public static class LoggingExtensions
{
    /// <summary>
    /// Configures Serilog with the specified log configuration.
    /// </summary>
    public static LoggerConfiguration ConfigureSerilog(this LoggerConfiguration loggerConfiguration, LogConfig config)
    {
        // Set minimum level
        var level = ParseLogLevel(config.Level);
        loggerConfiguration.MinimumLevel.Is(level);
        
        // Add enrichers
        loggerConfiguration
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .Enrich.WithProcessId()
            .Enrich.WithEnvironmentName()
            .Enrich.WithMachineName();

        // Console sink
        if (config.Console)
        {
            if (config.Structured)
            {
                loggerConfiguration.WriteTo.Console(
                    outputTemplate: config.Format,
                    restrictedToMinimumLevel: level);
            }
            else
            {
                loggerConfiguration.WriteTo.Console(
                    outputTemplate: config.Format,
                    restrictedToMinimumLevel: level);
            }
        }

        // File sink
        if (!string.IsNullOrEmpty(config.FilePath))
        {
            loggerConfiguration.WriteTo.File(
                path: config.FilePath,
                restrictedToMinimumLevel: level,
                outputTemplate: config.Format,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: 31,
                fileSizeLimitBytes: 1_073_741_824); // 1GB
        }

        return loggerConfiguration;
    }

    /// <summary>
    /// Creates a Serilog logger from configuration.
    /// </summary>
    public static Serilog.Core.Logger CreateLogger(LogConfig config)
    {
        var loggerConfig = new LoggerConfiguration();
        return loggerConfig.ConfigureSerilog(config).CreateLogger();
    }

    /// <summary>
    /// Converts a logger factory to use Serilog.
    /// </summary>
    public static ILoggingBuilder AddSerilog(this ILoggingBuilder builder, LogConfig config)
    {
        var logger = CreateLogger(config);
        builder.AddSerilog(logger, dispose: true);
        return builder;
    }

    private static LogEventLevel ParseLogLevel(string level)
    {
        return level?.ToUpperInvariant() switch
        {
            "VERBOSE" => LogEventLevel.Verbose,
            "DEBUG" => LogEventLevel.Debug,
            "INFORMATION" or "INFO" => LogEventLevel.Information,
            "WARNING" or "WARN" => LogEventLevel.Warning,
            "ERROR" => LogEventLevel.Error,
            "FATAL" => LogEventLevel.Fatal,
            _ => LogEventLevel.Information
        };
    }
}