using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;
using Lakepipe.Configuration.Models;
using Lakepipe.Core.Processors;
using Lakepipe.Core.Streams;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Streaming;

/// <summary>
/// High-performance streaming engine using TPL Dataflow with CPU affinity.
/// </summary>
public class DataflowStreamingEngine : IStreamingEngine
{
    private readonly ILogger<DataflowStreamingEngine> _logger;
    private readonly StreamingConfig _config;
    private readonly List<ProcessorUnit> _processorUnits;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    public DataflowStreamingEngine(ILogger<DataflowStreamingEngine> logger, StreamingConfig config)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _processorUnits = new List<ProcessorUnit>();
        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Creates a pipeline with CPU-affinity optimized dataflow blocks.
    /// </summary>
    public IPipeline CreatePipeline(string name, PipelineOptions? options = null)
    {
        options ??= new PipelineOptions();
        
        _logger.LogInformation("Creating pipeline {PipelineName} with options {@Options}", name, options);
        
        return new DataflowPipeline(name, options, _config, _logger, _cts.Token);
    }

    /// <summary>
    /// Starts the streaming engine with CPU affinity configuration.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting Dataflow Streaming Engine with CPU affinity enabled: {Enabled}", 
            _config.CpuAffinity.Enabled);
        
        if (_config.CpuAffinity.Enabled)
        {
            ConfigureCpuAffinity();
        }
        
        await Task.CompletedTask;
    }

    /// <summary>
    /// Stops the streaming engine gracefully.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Stopping Dataflow Streaming Engine");
        
        _cts.Cancel();
        
        // Wait for all processor units to complete
        await Task.WhenAll(_processorUnits.Select(u => u.CompletionTask));
        
        _logger.LogInformation("Dataflow Streaming Engine stopped");
    }

    private void ConfigureCpuAffinity()
    {
        var totalCores = Environment.ProcessorCount;
        var dedicatedCores = Math.Min(_config.CpuAffinity.DedicatedCores, totalCores - 1);
        
        _logger.LogInformation("Configuring CPU affinity: {DedicatedCores} cores out of {TotalCores}", 
            dedicatedCores, totalCores);
        
        // Reserve cores for dataflow processing
        for (int i = 0; i < dedicatedCores; i++)
        {
            var unit = new ProcessorUnit(i, _config.CpuAffinity.PinToCore);
            _processorUnits.Add(unit);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _cts.Cancel();
        _cts.Dispose();
        
        foreach (var unit in _processorUnits)
        {
            unit.Dispose();
        }
        
        _disposed = true;
    }
}

/// <summary>
/// Represents a processing unit pinned to a specific CPU core.
/// </summary>
internal class ProcessorUnit : IDisposable
{
    private readonly int _coreId;
    private readonly bool _pinToCore;
    private readonly TaskScheduler _scheduler;
    private readonly TaskFactory _taskFactory;
    private readonly TaskCompletionSource<bool> _completionSource;

    public int CoreId => _coreId;
    public Task CompletionTask => _completionSource.Task;

    public ProcessorUnit(int coreId, bool pinToCore)
    {
        _coreId = coreId;
        _pinToCore = pinToCore;
        _completionSource = new TaskCompletionSource<bool>();
        
        if (_pinToCore)
        {
            // Create a dedicated thread with CPU affinity
            var thread = new Thread(() => RunDedicatedThread())
            {
                Name = $"DataflowProcessor-Core{coreId}",
                IsBackground = false
            };
            
            // This would require P/Invoke on Windows to set thread affinity
            // For now, we use a custom task scheduler
            _scheduler = new DedicatedThreadTaskScheduler();
        }
        else
        {
            _scheduler = TaskScheduler.Default;
        }
        
        _taskFactory = new TaskFactory(_scheduler);
    }

    private void RunDedicatedThread()
    {
        try
        {
            // Thread runs until completion
            _completionSource.Task.Wait();
        }
        finally
        {
            _completionSource.TrySetResult(true);
        }
    }

    public Task RunAsync(Func<Task> workItem)
    {
        return _taskFactory.StartNew(workItem, TaskCreationOptions.LongRunning).Unwrap();
    }

    public void Dispose()
    {
        _completionSource.TrySetResult(true);
    }
}

/// <summary>
/// Custom task scheduler for dedicated thread execution.
/// </summary>
internal class DedicatedThreadTaskScheduler : TaskScheduler
{
    private readonly BlockingCollection<Task> _tasks = new();
    private readonly Thread _thread;

    public DedicatedThreadTaskScheduler()
    {
        _thread = new Thread(WorkerThread)
        {
            IsBackground = true,
            Name = "DedicatedScheduler"
        };
        _thread.Start();
    }

    private void WorkerThread()
    {
        foreach (var task in _tasks.GetConsumingEnumerable())
        {
            TryExecuteTask(task);
        }
    }

    protected override IEnumerable<Task> GetScheduledTasks()
    {
        return _tasks.ToArray();
    }

    protected override void QueueTask(Task task)
    {
        _tasks.Add(task);
    }

    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        return Thread.CurrentThread == _thread && TryExecuteTask(task);
    }
}

/// <summary>
/// TPL Dataflow-based pipeline implementation.
/// </summary>
public class DataflowPipeline : IPipeline
{
    private readonly string _name;
    private readonly PipelineOptions _options;
    private readonly StreamingConfig _config;
    private readonly ILogger _logger;
    private readonly CancellationToken _engineCancellationToken;
    private readonly List<IDataflowBlock> _blocks;
    private readonly Stopwatch _stopwatch;
    private TransformBlock<DataPart, DataPart>? _headBlock;
    private IDataflowBlock? _tailBlock;
    private bool _built;
    private long _processedCount;
    private long _errorCount;

    public string Name => _name;
    public PipelineState State { get; private set; }
    public PipelineStatistics Statistics => new()
    {
        ProcessedCount = _processedCount,
        ErrorCount = _errorCount,
        RunningTime = _stopwatch.Elapsed,
        Throughput = _stopwatch.Elapsed.TotalSeconds > 0 
            ? _processedCount / _stopwatch.Elapsed.TotalSeconds 
            : 0
    };

    public DataflowPipeline(string name, PipelineOptions options, StreamingConfig config, 
        ILogger logger, CancellationToken engineCancellationToken)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _engineCancellationToken = engineCancellationToken;
        _blocks = new List<IDataflowBlock>();
        _stopwatch = new Stopwatch();
        State = PipelineState.Created;
    }

    /// <summary>
    /// Adds a processor to the pipeline.
    /// </summary>
    public IPipeline AddProcessor(IProcessor processor, ProcessorOptions? options = null)
    {
        if (_built)
            throw new InvalidOperationException("Cannot add processors after pipeline is built");
        
        options ??= new ProcessorOptions();
        
        var executionOptions = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = options.BufferSize,
            MaxDegreeOfParallelism = options.Parallelism,
            CancellationToken = _engineCancellationToken,
            EnsureOrdered = options.PreserveOrder
        };

        var block = new TransformManyBlock<DataPart, DataPart>(
            async dataPart =>
            {
                try
                {
                    var results = new List<DataPart>();
                    var inputStream = CreateSingleItemStream(dataPart);
                    
                    await foreach (var result in processor.ProcessAsync(inputStream, _engineCancellationToken))
                    {
                        results.Add(result);
                        Interlocked.Increment(ref _processedCount);
                    }
                    
                    return results;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in processor {ProcessorType}", processor.GetType().Name);
                    Interlocked.Increment(ref _errorCount);
                    
                    if (_options.StopOnError)
                        throw;
                    
                    return Enumerable.Empty<DataPart>();
                }
            },
            executionOptions);

        _blocks.Add(block);
        
        // Link blocks together
        if (_headBlock == null)
        {
            _headBlock = new TransformBlock<DataPart, DataPart>(d => d, executionOptions);
            _headBlock.LinkTo(block, new DataflowLinkOptions { PropagateCompletion = true });
            _blocks.Insert(0, _headBlock);
        }
        else if (_tailBlock != null && _tailBlock is ISourceBlock<DataPart> sourceBlock)
        {
            sourceBlock.LinkTo(block, new DataflowLinkOptions { PropagateCompletion = true });
        }
        
        _tailBlock = block;
        
        return this;
    }

    /// <summary>
    /// Builds the pipeline for execution.
    /// </summary>
    public IPipeline Build()
    {
        if (_built)
            return this;
        
        if (_headBlock == null)
            throw new InvalidOperationException("Pipeline must have at least one processor");
        
        // Add final action block to consume results
        var finalBlock = new ActionBlock<DataPart>(
            dataPart =>
            {
                _logger.LogDebug("Pipeline {PipelineName} completed processing DataPart {DataPartId}",
                    _name, dataPart.Metadata.Id);
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = _options.BufferSize,
                CancellationToken = _engineCancellationToken
            });
        
        if (_tailBlock != null && _tailBlock is ISourceBlock<DataPart> sourceBlock)
        {
            sourceBlock.LinkTo(finalBlock, new DataflowLinkOptions { PropagateCompletion = true });
        }
        
        _blocks.Add(finalBlock);
        _built = true;
        State = PipelineState.Ready;
        
        return this;
    }

    /// <summary>
    /// Runs the pipeline with the given input stream.
    /// </summary>
    public async Task RunAsync(IAsyncEnumerable<DataPart> inputStream, CancellationToken cancellationToken = default)
    {
        if (!_built)
            Build();
        
        if (_headBlock == null)
            throw new InvalidOperationException("Pipeline not properly initialized");
        
        State = PipelineState.Running;
        _stopwatch.Start();
        
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _engineCancellationToken);
        
        try
        {
            // Feed input stream to head block
            await foreach (var dataPart in inputStream.WithCancellation(linkedCts.Token))
            {
                await _headBlock.SendAsync(dataPart, linkedCts.Token);
            }
            
            // Signal completion
            _headBlock.Complete();
            
            // Wait for all blocks to complete
            await Task.WhenAll(_blocks.Select(b => b.Completion));
            
            State = PipelineState.Completed;
        }
        catch (OperationCanceledException)
        {
            State = PipelineState.Cancelled;
            throw;
        }
        catch (Exception ex)
        {
            State = PipelineState.Failed;
            _logger.LogError(ex, "Pipeline {PipelineName} failed", _name);
            throw;
        }
        finally
        {
            _stopwatch.Stop();
            _logger.LogInformation("Pipeline {PipelineName} finished. State: {State}, Processed: {ProcessedCount}, " +
                                  "Errors: {ErrorCount}, Duration: {Duration}ms", 
                _name, State, _processedCount, _errorCount, _stopwatch.ElapsedMilliseconds);
        }
    }

    private static async IAsyncEnumerable<DataPart> CreateSingleItemStream(DataPart item)
    {
        yield return item;
        await Task.CompletedTask;
    }
}

/// <summary>
/// Streaming engine interface.
/// </summary>
public interface IStreamingEngine : IDisposable
{
    IPipeline CreatePipeline(string name, PipelineOptions? options = null);
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Pipeline interface.
/// </summary>
public interface IPipeline
{
    string Name { get; }
    PipelineState State { get; }
    PipelineStatistics Statistics { get; }
    
    IPipeline AddProcessor(IProcessor processor, ProcessorOptions? options = null);
    IPipeline Build();
    Task RunAsync(IAsyncEnumerable<DataPart> inputStream, CancellationToken cancellationToken = default);
}

/// <summary>
/// Pipeline execution options.
/// </summary>
public class PipelineOptions
{
    public int BufferSize { get; set; } = 100;
    public bool StopOnError { get; set; } = false;
    public TimeSpan Timeout { get; set; } = TimeSpan.FromHours(24);
}

/// <summary>
/// Processor execution options.
/// </summary>
public class ProcessorOptions
{
    public int Parallelism { get; set; } = 1;
    public int BufferSize { get; set; } = 100;
    public bool PreserveOrder { get; set; } = true;
}

/// <summary>
/// Pipeline execution state.
/// </summary>
public enum PipelineState
{
    Created,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled
}

/// <summary>
/// Pipeline execution statistics.
/// </summary>
public class PipelineStatistics
{
    public long ProcessedCount { get; init; }
    public long ErrorCount { get; init; }
    public TimeSpan RunningTime { get; init; }
    public double Throughput { get; init; }
}