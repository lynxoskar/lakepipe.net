using Lakepipe.Core.Streams;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Core.Processors;

/// <summary>
/// Extension methods for processor composition.
/// </summary>
public static class ProcessorExtensions
{
    /// <summary>
    /// Converts a processor to a composable processor.
    /// </summary>
    public static IComposableProcessor AsComposable(this IProcessor processor, ILogger logger)
    {
        if (processor is IComposableProcessor composable)
            return composable;

        return new ComposableProcessor(
            (stream, ct) => processor.ProcessAsync(stream, ct),
            logger);
    }

    /// <summary>
    /// Chains two processors together.
    /// </summary>
    public static IComposableProcessor Then(this IProcessor left, IProcessor right, ILogger? logger = null)
    {
        logger ??= Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        return left.AsComposable(logger).Then(right);
    }

    /// <summary>
    /// Creates a processor from a simple transformation function.
    /// </summary>
    public static IProcessor FromFunc<TInput, TOutput>(
        Func<TInput, Task<TOutput>> transformFunc,
        ILogger logger)
        where TInput : notnull
        where TOutput : notnull
    {
        return new FuncProcessor<TInput, TOutput>(transformFunc, logger);
    }

    /// <summary>
    /// Creates a processor that filters data parts based on a predicate.
    /// </summary>
    public static IProcessor Filter(Func<DataPart, bool> predicate, ILogger logger)
    {
        return new FilterProcessor(predicate, logger);
    }

    /// <summary>
    /// Creates a processor that batches data parts.
    /// </summary>
    public static IProcessor Batch(int batchSize, TimeSpan? timeout, ILogger logger)
    {
        return new BatchProcessor(batchSize, timeout, logger);
    }

    /// <summary>
    /// Adds retry logic to a processor.
    /// </summary>
    public static IProcessor WithRetry(this IProcessor processor, int maxRetries, TimeSpan delay, ILogger logger)
    {
        return new RetryProcessor(processor, maxRetries, delay, logger);
    }

    /// <summary>
    /// Adds timeout to a processor.
    /// </summary>
    public static IProcessor WithTimeout(this IProcessor processor, TimeSpan timeout, ILogger logger)
    {
        return new TimeoutProcessor(processor, timeout, logger);
    }
}

/// <summary>
/// Processor that filters data parts.
/// </summary>
internal class FilterProcessor : ProcessorBase
{
    private readonly Func<DataPart, bool> _predicate;

    public FilterProcessor(Func<DataPart, bool> predicate, ILogger logger) : base(logger)
    {
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (_predicate(item))
                yield return item;
        }
    }
}

/// <summary>
/// Processor created from a transformation function.
/// </summary>
internal class FuncProcessor<TInput, TOutput> : ProcessorBase<TInput, TOutput>
    where TInput : notnull
    where TOutput : notnull
{
    private readonly Func<TInput, Task<TOutput>> _transformFunc;

    public FuncProcessor(Func<TInput, Task<TOutput>> transformFunc, ILogger logger) : base(logger)
    {
        _transformFunc = transformFunc ?? throw new ArgumentNullException(nameof(transformFunc));
    }

    public override Task<TOutput> ProcessSingleAsync(TInput input, CancellationToken cancellationToken = default)
    {
        return _transformFunc(input);
    }
}

/// <summary>
/// Processor that batches data parts.
/// </summary>
internal class BatchProcessor : ProcessorBase
{
    private readonly int _batchSize;
    private readonly TimeSpan? _timeout;

    public BatchProcessor(int batchSize, TimeSpan? timeout, ILogger logger) : base(logger)
    {
        _batchSize = batchSize;
        _timeout = timeout;
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = new List<DataPart>(_batchSize);
        using var timer = _timeout.HasValue ? new PeriodicTimer(_timeout.Value) : null;

        var timerTask = timer?.WaitForNextTickAsync(cancellationToken).AsTask();

        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            batch.Add(part);

            if (batch.Count >= _batchSize)
            {
                yield return CreateBatchedPart(batch);
                batch.Clear();
                timerTask = timer?.WaitForNextTickAsync(cancellationToken).AsTask();
            }
            else if (timerTask != null)
            {
                var inputTask = Task.FromResult(true);
                var completedTask = await Task.WhenAny(inputTask, timerTask);

                if (completedTask == timerTask && batch.Count > 0)
                {
                    yield return CreateBatchedPart(batch);
                    batch.Clear();
                    timerTask = timer!.WaitForNextTickAsync(cancellationToken).AsTask();
                }
            }
        }

        if (batch.Count > 0)
        {
            yield return CreateBatchedPart(batch);
        }
    }

    private DataPart CreateBatchedPart(List<DataPart> batch)
    {
        return new DataPart
        {
            Data = batch.ToArray(),
            Metadata = new DataPartMetadata
            {
                Source = "BatchProcessor",
                MessageCount = batch.Count,
                Properties = new Dictionary<string, object>
                {
                    ["BatchSize"] = batch.Count,
                    ["FirstItemId"] = batch[0].Metadata.Id,
                    ["LastItemId"] = batch[^1].Metadata.Id
                }
            },
            SourceInfo = batch[0].SourceInfo,
            Schema = batch[0].Schema,
            CacheMetadata = batch[0].CacheMetadata
        };
    }
}

/// <summary>
/// Processor that adds retry logic.
/// </summary>
internal class RetryProcessor : ProcessorBase
{
    private readonly IProcessor _inner;
    private readonly int _maxRetries;
    private readonly TimeSpan _delay;

    public RetryProcessor(IProcessor inner, int maxRetries, TimeSpan delay, ILogger logger) : base(logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _maxRetries = maxRetries;
        _delay = delay;
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            var retries = 0;
            var success = false;
            DataPart? result = null;

            while (retries <= _maxRetries && !success)
            {
                try
                {
                    await foreach (var processedPart in _inner.ProcessAsync(
                        SingleItemStream(part), cancellationToken))
                    {
                        result = processedPart;
                        success = true;
                        break;
                    }
                }
                catch (Exception ex)
                {
                    retries++;
                    if (retries > _maxRetries)
                    {
                        Logger.LogError(ex, "Max retries exceeded for data part {PartId}", part.Metadata.Id);
                        throw;
                    }

                    Logger.LogWarning(ex, "Retry {Retry}/{MaxRetries} for data part {PartId}",
                        retries, _maxRetries, part.Metadata.Id);
                    
                    await Task.Delay(_delay, cancellationToken);
                }
            }

            if (result != null)
            {
                yield return result;
            }
        }
    }
    
    #pragma warning disable CS1998 // Async method lacks 'await' operators
    private static async IAsyncEnumerable<T> SingleItemStream<T>(T item)
    {
        yield return item;
    }
    #pragma warning restore CS1998
}

/// <summary>
/// Processor that adds timeout.
/// </summary>
internal class TimeoutProcessor : ProcessorBase
{
    private readonly IProcessor _inner;
    private readonly TimeSpan _timeout;

    public TimeoutProcessor(IProcessor inner, TimeSpan timeout, ILogger logger) : base(logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _timeout = timeout;
    }

    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_timeout);

            DataPart? result = null;
            
            try
            {
                await foreach (var processedPart in _inner.ProcessAsync(
                    SingleItemStream(part), cts.Token))
                {
                    result = processedPart;
                    break;
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                Logger.LogWarning("Processing timeout for data part {PartId}", part.Metadata.Id);
                throw new TimeoutException($"Processing timeout after {_timeout.TotalSeconds} seconds");
            }

            if (result != null)
            {
                yield return result;
            }
        }
    }
    
    #pragma warning disable CS1998 // Async method lacks 'await' operators
    private static async IAsyncEnumerable<T> SingleItemStream<T>(T item)
    {
        yield return item;
    }
    #pragma warning restore CS1998
}