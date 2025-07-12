using System.Runtime.CompilerServices;
using Lakepipe.Core.Streams;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Core.Processors;

/// <summary>
/// Base implementation for processors.
/// </summary>
public abstract class ProcessorBase : IProcessor
{
    protected readonly ILogger Logger;

    protected ProcessorBase(ILogger logger)
    {
        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public abstract IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream, 
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Helper method to process stream with error handling.
    /// </summary>
    protected async IAsyncEnumerable<DataPart> ProcessStreamWithErrorHandling(
        IAsyncEnumerable<DataPart> inputStream,
        Func<DataPart, CancellationToken, ValueTask<DataPart?>> processFunc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var part in inputStream.WithCancellation(cancellationToken))
        {
            DataPart? result = null;
            
            try
            {
                result = await processFunc(part, cancellationToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error processing data part {PartId}", part.Metadata.Id);
                
                // Create error result
                result = part with
                {
                    Metadata = part.Metadata with
                    {
                        Properties = new Dictionary<string, object>(part.Metadata.Properties)
                        {
                            ["ProcessingError"] = ex.Message,
                            ["ProcessingErrorType"] = ex.GetType().Name
                        }
                    }
                };
            }

            if (result != null)
            {
                yield return result;
            }
        }
    }
}

/// <summary>
/// Base implementation for typed processors.
/// </summary>
public abstract class ProcessorBase<TInput, TOutput> : ProcessorBase, IProcessor<TInput, TOutput>
    where TInput : notnull
    where TOutput : notnull
{
    protected ProcessorBase(ILogger logger) : base(logger)
    {
    }

    /// <inheritdoc/>
    public abstract Task<TOutput> ProcessSingleAsync(TInput input, CancellationToken cancellationToken = default);

    /// <inheritdoc/>
    public override async IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var part in ProcessStreamWithErrorHandling(inputStream, ProcessDataPartAsync, cancellationToken))
        {
            yield return part;
        }
    }

    private async ValueTask<DataPart?> ProcessDataPartAsync(DataPart part, CancellationToken cancellationToken)
    {
        if (part.Data is not TInput input)
        {
            Logger.LogWarning("Data part {PartId} has unexpected type {Type}, expected {ExpectedType}",
                part.Metadata.Id, part.Data?.GetType().Name ?? "null", typeof(TInput).Name);
            return part;
        }

        var output = await ProcessSingleAsync(input, cancellationToken);
        
        return part with { Data = output };
    }
}

/// <summary>
/// Composable processor that supports functional composition.
/// </summary>
public class ComposableProcessor : ProcessorBase, IComposableProcessor
{
    private readonly Func<IAsyncEnumerable<DataPart>, CancellationToken, IAsyncEnumerable<DataPart>> _processFunc;

    public ComposableProcessor(
        Func<IAsyncEnumerable<DataPart>, CancellationToken, IAsyncEnumerable<DataPart>> processFunc,
        ILogger logger) : base(logger)
    {
        _processFunc = processFunc ?? throw new ArgumentNullException(nameof(processFunc));
    }

    public override IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream, 
        CancellationToken cancellationToken = default)
    {
        return _processFunc(inputStream, cancellationToken);
    }

    public IComposableProcessor Then(IProcessor next)
    {
        return new ComposableProcessor(
            (stream, ct) => next.ProcessAsync(_processFunc(stream, ct), ct),
            Logger);
    }

    public IComposableProcessor Parallel(IProcessor other)
    {
        return new ComposableProcessor(
            (stream, ct) => ParallelProcessAsync(stream, ct, other),
            Logger);
    }

    public IComposableProcessor When(Func<DataPart, bool> predicate, IProcessor processor)
    {
        return new ComposableProcessor(
            (stream, ct) => ConditionalProcessAsync(stream, ct, predicate, processor),
            Logger);
    }

    private async IAsyncEnumerable<DataPart> ParallelProcessAsync(
        IAsyncEnumerable<DataPart> stream,
        [EnumeratorCancellation] CancellationToken ct,
        IProcessor other)
    {
        var channel1 = System.Threading.Channels.Channel.CreateUnbounded<DataPart>();
        var channel2 = System.Threading.Channels.Channel.CreateUnbounded<DataPart>();

        // Split stream
        var splitTask = Task.Run(async () =>
        {
            await foreach (var item in stream.WithCancellation(ct))
            {
                await channel1.Writer.WriteAsync(item, ct);
                await channel2.Writer.WriteAsync(item, ct);
            }
            channel1.Writer.Complete();
            channel2.Writer.Complete();
        }, ct);

        // Process in parallel
        var stream1 = _processFunc(channel1.Reader.ReadAllAsync(ct), ct);
        var stream2 = other.ProcessAsync(channel2.Reader.ReadAllAsync(ct), ct);

        // Merge results
        await foreach (var item in MergeStreams(stream1, stream2, ct))
        {
            yield return item;
        }

        await splitTask;
    }

    private async IAsyncEnumerable<DataPart> ConditionalProcessAsync(
        IAsyncEnumerable<DataPart> stream,
        [EnumeratorCancellation] CancellationToken ct,
        Func<DataPart, bool> predicate,
        IProcessor processor)
    {
        await foreach (var item in _processFunc(stream, ct).WithCancellation(ct))
        {
            if (predicate(item))
            {
                await foreach (var processed in processor.ProcessAsync(SingleItemStream(item), ct))
                {
                    yield return processed;
                }
            }
            else
            {
                yield return item;
            }
        }
    }

    #pragma warning disable CS1998 // Async method lacks 'await' operators
    private static async IAsyncEnumerable<T> SingleItemStream<T>(T item)
    {
        yield return item;
    }
    #pragma warning restore CS1998

    private static async IAsyncEnumerable<T> MergeStreams<T>(
        IAsyncEnumerable<T> stream1,
        IAsyncEnumerable<T> stream2,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = System.Threading.Channels.Channel.CreateUnbounded<T>();

        var task1 = Task.Run(async () =>
        {
            await foreach (var item in stream1.WithCancellation(cancellationToken))
            {
                await channel.Writer.WriteAsync(item, cancellationToken);
            }
        }, cancellationToken);

        var task2 = Task.Run(async () =>
        {
            await foreach (var item in stream2.WithCancellation(cancellationToken))
            {
                await channel.Writer.WriteAsync(item, cancellationToken);
            }
        }, cancellationToken);

        _ = Task.WhenAll(task1, task2).ContinueWith(_ => channel.Writer.Complete(), TaskScheduler.Default);

        await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }
}