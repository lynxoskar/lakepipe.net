using Lakepipe.Core.Streams;

namespace Lakepipe.Core.Processors;

/// <summary>
/// Base interface for all data processors in the pipeline.
/// </summary>
public interface IProcessor
{
    /// <summary>
    /// Processes an async stream of data parts.
    /// </summary>
    /// <param name="inputStream">The input stream of data parts.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An async enumerable of processed data parts.</returns>
    IAsyncEnumerable<DataPart> ProcessAsync(
        IAsyncEnumerable<DataPart> inputStream,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Generic processor interface with specific input and output types.
/// </summary>
public interface IProcessor<TInput, TOutput> : IProcessor
    where TInput : notnull
    where TOutput : notnull
{
    /// <summary>
    /// Processes a single input item.
    /// </summary>
    Task<TOutput> ProcessSingleAsync(TInput input, CancellationToken cancellationToken = default);
}

/// <summary>
/// Interface for processors that can be composed using functional operators.
/// </summary>
public interface IComposableProcessor : IProcessor
{
    /// <summary>
    /// Composes this processor with another processor.
    /// </summary>
    IComposableProcessor Then(IProcessor next);

    /// <summary>
    /// Runs processors in parallel and merges results.
    /// </summary>
    IComposableProcessor Parallel(IProcessor other);

    /// <summary>
    /// Conditionally applies a processor based on a predicate.
    /// </summary>
    IComposableProcessor When(Func<DataPart, bool> predicate, IProcessor processor);
}

/// <summary>
/// Interface for processors that support streaming with backpressure.
/// </summary>
public interface IStreamingProcessor : IProcessor
{
    /// <summary>
    /// Pauses processing for backpressure handling.
    /// </summary>
    Task PauseAsync();

    /// <summary>
    /// Resumes processing after backpressure relief.
    /// </summary>
    Task ResumeAsync();

    /// <summary>
    /// Gets the current processing metrics.
    /// </summary>
    ProcessingMetrics GetMetrics();
}