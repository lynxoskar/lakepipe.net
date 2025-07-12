using System.Diagnostics;
using System.Runtime.CompilerServices;
using Lakepipe.Core.Logging;
using Lakepipe.Core.Streams;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Core.Processors;

/// <summary>
/// Processor that adds structured logging to the pipeline.
/// </summary>
public class LoggingProcessor : ProcessorBase
{
    private readonly IProcessor _inner;
    private readonly string _processorName;
    private readonly bool _logDataParts;
    private readonly bool _logPerformance;
    private long _totalItemsProcessed;
    private long _totalBytesProcessed;
    private readonly Stopwatch _totalStopwatch;

    public LoggingProcessor(
        IProcessor inner,
        ILogger logger,
        string? processorName = null,
        bool logDataParts = true,
        bool logPerformance = true) : base(logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _processorName = processorName ?? inner.GetType().Name;
        _logDataParts = logDataParts;
        _logPerformance = logPerformance;
        _totalStopwatch = new Stopwatch();
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var processorContext = LogContextExtensions.WithProcessorContext(_inner.GetType().Name, _processorName);
        
        Logger.LogInformation("Starting processor {ProcessorName}", _processorName);
        _totalStopwatch.Start();

        var batchStopwatch = Stopwatch.StartNew();
        var batchItemCount = 0;
        var batchByteCount = 0L;
        var lastLogTime = DateTime.UtcNow;

        await foreach (var part in _inner.ProcessAsync(inputStream, cancellationToken))
        {
            if (_logDataParts)
            {
                using (LogContextExtensions.WithDataPartContext(part))
                {
                    Logger.LogDebug("Processed data part {DataPartId} with {MessageCount} messages",
                        part.Metadata.Id, part.Metadata.MessageCount);
                }
            }

            // Track metrics
            batchItemCount++;
            _totalItemsProcessed++;
            
            // Estimate bytes (this is a rough estimate)
            var estimatedBytes = EstimateDataPartSize(part);
            batchByteCount += estimatedBytes;
            _totalBytesProcessed += estimatedBytes;

            // Log performance periodically
            if (_logPerformance && (DateTime.UtcNow - lastLogTime).TotalSeconds >= 10)
            {
                LogPerformanceMetrics(batchItemCount, batchByteCount, batchStopwatch.Elapsed);
                
                // Reset batch metrics
                batchItemCount = 0;
                batchByteCount = 0;
                batchStopwatch.Restart();
                lastLogTime = DateTime.UtcNow;
            }

            yield return part;
        }
        
        _totalStopwatch.Stop();
        
        // Log final metrics
        if (_logPerformance)
        {
            LogFinalMetrics();
        }
        
        Logger.LogInformation("Completed processor {ProcessorName} after {ElapsedMs}ms",
            _processorName, _totalStopwatch.ElapsedMilliseconds);
    }

    private void LogPerformanceMetrics(int itemCount, long byteCount, TimeSpan elapsed)
    {
        using (LogContextExtensions.WithPerformanceMetrics(itemCount, byteCount, elapsed))
        {
            var itemsPerSecond = elapsed.TotalSeconds > 0 ? itemCount / elapsed.TotalSeconds : 0;
            var mbPerSecond = elapsed.TotalSeconds > 0 ? (byteCount / (1024.0 * 1024.0)) / elapsed.TotalSeconds : 0;

            Logger.LogInformation(
                "Processor {ProcessorName} performance: {ItemsPerSecond:F2} items/s, {MBPerSecond:F2} MB/s",
                _processorName, itemsPerSecond, mbPerSecond);
        }
    }

    private void LogFinalMetrics()
    {
        using (LogContextExtensions.WithPerformanceMetrics(
            _totalItemsProcessed, 
            _totalBytesProcessed, 
            _totalStopwatch.Elapsed))
        {
            var totalMB = _totalBytesProcessed / (1024.0 * 1024.0);
            var avgItemsPerSecond = _totalStopwatch.Elapsed.TotalSeconds > 0 
                ? _totalItemsProcessed / _totalStopwatch.Elapsed.TotalSeconds 
                : 0;
            var avgMBPerSecond = _totalStopwatch.Elapsed.TotalSeconds > 0 
                ? totalMB / _totalStopwatch.Elapsed.TotalSeconds 
                : 0;

            Logger.LogInformation(
                "Processor {ProcessorName} final metrics: {TotalItems} items, {TotalMB:F2} MB, " +
                "{AvgItemsPerSecond:F2} items/s, {AvgMBPerSecond:F2} MB/s, {TotalElapsedMs}ms",
                _processorName, _totalItemsProcessed, totalMB, 
                avgItemsPerSecond, avgMBPerSecond, _totalStopwatch.ElapsedMilliseconds);
        }
    }

    private static long EstimateDataPartSize(DataPart part)
    {
        // This is a rough estimate - in a real implementation you would
        // calculate actual size based on the data type
        return part.Metadata.MessageCount * 1024; // Assume 1KB per message average
    }
}

/// <summary>
/// Extension methods for adding logging to processors.
/// </summary>
public static class LoggingProcessorExtensions
{
    /// <summary>
    /// Wraps a processor with structured logging.
    /// </summary>
    public static IProcessor WithLogging(
        this IProcessor processor,
        ILogger logger,
        string? name = null,
        bool logDataParts = true,
        bool logPerformance = true)
    {
        return new LoggingProcessor(processor, logger, name, logDataParts, logPerformance);
    }

    /// <summary>
    /// Wraps a processor with operation logging.
    /// </summary>
    public static IProcessor WithOperationLogging(
        this IProcessor processor,
        string operationName,
        ILogger logger)
    {
        return new OperationLoggingProcessor(processor, operationName, logger);
    }
}

/// <summary>
/// Processor that logs operations with timing.
/// </summary>
internal class OperationLoggingProcessor : ProcessorBase
{
    private readonly IProcessor _inner;
    private readonly string _operationName;

    public OperationLoggingProcessor(IProcessor inner, string operationName, ILogger logger) : base(logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _operationName = operationName ?? throw new ArgumentNullException(nameof(operationName));
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var operation = LogContextExtensions.BeginOperation(_operationName);
        
        var itemCount = 0;
        
        await foreach (var part in _inner.ProcessAsync(inputStream, cancellationToken))
        {
            itemCount++;
            yield return part;
        }
        
        operation.Complete($"Processed {itemCount} items");
    }
}