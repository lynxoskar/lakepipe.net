using System.Diagnostics;
using Serilog.Context;
using Lakepipe.Core.Streams;

namespace Lakepipe.Core.Logging;

/// <summary>
/// Provides structured logging context for pipeline operations.
/// </summary>
public static class LogContextExtensions
{
    /// <summary>
    /// Adds pipeline context to the log scope.
    /// </summary>
    public static IDisposable WithPipelineContext(string pipelineName, string pipelineId)
    {
        return LogContext.PushProperty("Pipeline", new { Name = pipelineName, Id = pipelineId });
    }

    /// <summary>
    /// Adds data part context to the log scope.
    /// </summary>
    public static IDisposable WithDataPartContext(DataPart dataPart)
    {
        var properties = new Dictionary<string, object?>
        {
            ["DataPartId"] = dataPart.Metadata.Id,
            ["Source"] = dataPart.Metadata.Source,
            ["MessageCount"] = dataPart.Metadata.MessageCount,
            ["StreamPosition"] = dataPart.Metadata.StreamPosition
        };

        if (dataPart.Metadata.Topic != null)
            properties["Topic"] = dataPart.Metadata.Topic;

        if (dataPart.Metadata.CacheHit)
            properties["CacheHit"] = true;

        return LogContext.PushProperty("DataPart", properties);
    }

    /// <summary>
    /// Adds operation context with timing.
    /// </summary>
    public static OperationContext BeginOperation(string operationName, string? operationId = null)
    {
        return new OperationContext(operationName, operationId ?? Guid.NewGuid().ToString());
    }

    /// <summary>
    /// Adds processor context to the log scope.
    /// </summary>
    public static IDisposable WithProcessorContext(string processorType, string processorName)
    {
        return LogContext.PushProperty("Processor", new { Type = processorType, Name = processorName });
    }

    /// <summary>
    /// Adds performance metrics to the log scope.
    /// </summary>
    public static IDisposable WithPerformanceMetrics(long itemsProcessed, long bytesProcessed, TimeSpan elapsed)
    {
        var metrics = new
        {
            ItemsProcessed = itemsProcessed,
            BytesProcessed = bytesProcessed,
            ElapsedMs = elapsed.TotalMilliseconds,
            ItemsPerSecond = elapsed.TotalSeconds > 0 ? itemsProcessed / elapsed.TotalSeconds : 0,
            BytesPerSecond = elapsed.TotalSeconds > 0 ? bytesProcessed / elapsed.TotalSeconds : 0
        };

        return LogContext.PushProperty("Performance", metrics);
    }

    /// <summary>
    /// Adds error context to the log scope.
    /// </summary>
    public static IDisposable WithErrorContext(Exception exception, string? context = null)
    {
        var errorInfo = new
        {
            ErrorType = exception.GetType().Name,
            ErrorMessage = exception.Message,
            Context = context,
            StackTrace = exception.StackTrace
        };

        return LogContext.PushProperty("Error", errorInfo);
    }
}

/// <summary>
/// Represents an operation context that tracks timing and success/failure.
/// </summary>
public sealed class OperationContext : IDisposable
{
    private readonly string _operationName;
    private readonly string _operationId;
    private readonly Stopwatch _stopwatch;
    private readonly IDisposable _logContext;
    private bool _disposed;

    internal OperationContext(string operationName, string operationId)
    {
        _operationName = operationName;
        _operationId = operationId;
        _stopwatch = Stopwatch.StartNew();
        _logContext = LogContext.PushProperty("Operation", new 
        { 
            Name = operationName, 
            Id = operationId,
            StartTime = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Marks the operation as completed successfully.
    /// </summary>
    public void Complete(string? result = null)
    {
        if (_disposed) return;

        var elapsed = _stopwatch.Elapsed;
        using (LogContext.PushProperty("OperationResult", new
        {
            Name = _operationName,
            Id = _operationId,
            Success = true,
            ElapsedMs = elapsed.TotalMilliseconds,
            Result = result
        }))
        {
            Serilog.Log.Information("Operation {OperationName} completed successfully in {ElapsedMs}ms",
                _operationName, elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Marks the operation as failed.
    /// </summary>
    public void Fail(Exception exception)
    {
        if (_disposed) return;

        var elapsed = _stopwatch.Elapsed;
        using (LogContext.PushProperty("OperationResult", new
        {
            Name = _operationName,
            Id = _operationId,
            Success = false,
            ElapsedMs = elapsed.TotalMilliseconds,
            ErrorType = exception.GetType().Name,
            ErrorMessage = exception.Message
        }))
        {
            Serilog.Log.Error(exception, "Operation {OperationName} failed after {ElapsedMs}ms",
                _operationName, elapsed.TotalMilliseconds);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _disposed = true;
        _stopwatch.Stop();
        _logContext.Dispose();
        
        // Log if operation was not explicitly completed or failed
        if (_stopwatch.IsRunning)
        {
            Serilog.Log.Warning("Operation {OperationName} disposed without explicit completion after {ElapsedMs}ms",
                _operationName, _stopwatch.Elapsed.TotalMilliseconds);
        }
    }
}