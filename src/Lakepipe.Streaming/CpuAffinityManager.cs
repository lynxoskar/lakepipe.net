using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Streaming;

/// <summary>
/// Manages CPU affinity for high-performance stream processing.
/// </summary>
public static class CpuAffinityManager
{
    private static readonly ILogger Logger = LoggerFactory.Create(builder => { })
        .CreateLogger(typeof(CpuAffinityManager));

    /// <summary>
    /// Sets the current thread's affinity to a specific CPU core.
    /// </summary>
    public static bool SetThreadAffinity(int coreId)
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return SetWindowsThreadAffinity(coreId);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return SetLinuxThreadAffinity(coreId);
            }
            else
            {
                Logger.LogWarning("CPU affinity not supported on this platform");
                return false;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to set thread affinity to core {CoreId}", coreId);
            return false;
        }
    }

    private static bool SetWindowsThreadAffinity(int coreId)
    {
        var mask = (IntPtr)(1L << coreId);
        var currentThread = GetCurrentThread();
        var previousMask = SetThreadAffinityMask(currentThread, mask);
        
        if (previousMask == IntPtr.Zero)
        {
            Logger.LogError("Failed to set Windows thread affinity. Error: {Error}", 
                Marshal.GetLastWin32Error());
            return false;
        }
        
        Logger.LogInformation("Set thread affinity to core {CoreId} on Windows", coreId);
        return true;
    }

    private static bool SetLinuxThreadAffinity(int coreId)
    {
        var cpuSet = new CpuSet();
        CPU_ZERO(ref cpuSet);
        CPU_SET(coreId, ref cpuSet);
        
        var result = sched_setaffinity(0, Marshal.SizeOf<CpuSet>(), ref cpuSet);
        
        if (result != 0)
        {
            Logger.LogError("Failed to set Linux thread affinity. Error: {Error}", 
                Marshal.GetLastWin32Error());
            return false;
        }
        
        Logger.LogInformation("Set thread affinity to core {CoreId} on Linux", coreId);
        return true;
    }

    #region Windows P/Invoke

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentThread();

    [DllImport("kernel32.dll")]
    private static extern IntPtr SetThreadAffinityMask(IntPtr hThread, IntPtr dwThreadAffinityMask);

    #endregion

    #region Linux P/Invoke

    private const int CPU_SETSIZE = 1024;
    private const int NCPUBITS = 8 * sizeof(ulong);

    [StructLayout(LayoutKind.Sequential)]
    private struct CpuSet
    {
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = CPU_SETSIZE / NCPUBITS)]
        public ulong[] bits;

        public CpuSet()
        {
            bits = new ulong[CPU_SETSIZE / NCPUBITS];
        }
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int sched_setaffinity(int pid, int cpusetsize, ref CpuSet mask);

    private static void CPU_ZERO(ref CpuSet set)
    {
        for (int i = 0; i < set.bits.Length; i++)
        {
            set.bits[i] = 0;
        }
    }

    private static void CPU_SET(int cpu, ref CpuSet set)
    {
        int idx = cpu / NCPUBITS;
        int bit = cpu % NCPUBITS;
        if (idx < set.bits.Length)
        {
            set.bits[idx] |= (1UL << bit);
        }
    }

    #endregion
}

/// <summary>
/// Thread pool with CPU affinity support.
/// </summary>
public class AffinityThreadPool : IDisposable
{
    private readonly int[] _coreIds;
    private readonly BlockingCollection<WorkItem> _workQueue;
    private readonly Thread[] _threads;
    private readonly CancellationTokenSource _cts;
    private readonly ILogger _logger;
    private bool _disposed;

    public AffinityThreadPool(int[] coreIds, ILogger logger)
    {
        _coreIds = coreIds ?? throw new ArgumentNullException(nameof(coreIds));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _workQueue = new BlockingCollection<WorkItem>();
        _threads = new Thread[coreIds.Length];
        _cts = new CancellationTokenSource();
        
        InitializeThreads();
    }

    private void InitializeThreads()
    {
        for (int i = 0; i < _coreIds.Length; i++)
        {
            var coreId = _coreIds[i];
            var thread = new Thread(() => WorkerThread(coreId))
            {
                Name = $"AffinityWorker-Core{coreId}",
                IsBackground = true
            };
            
            _threads[i] = thread;
            thread.Start();
        }
    }

    private void WorkerThread(int coreId)
    {
        // Set CPU affinity for this thread
        CpuAffinityManager.SetThreadAffinity(coreId);
        
        _logger.LogInformation("Worker thread started on core {CoreId}", coreId);
        
        try
        {
            foreach (var workItem in _workQueue.GetConsumingEnumerable(_cts.Token))
            {
                try
                {
                    workItem.Execute();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error executing work item on core {CoreId}", coreId);
                    workItem.SetException(ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        
        _logger.LogInformation("Worker thread on core {CoreId} stopped", coreId);
    }

    public Task RunAsync(Func<Task> work)
    {
        var workItem = new NonGenericWorkItem(work);
        _workQueue.Add(workItem);
        return workItem.Task;
    }

    public Task<T> RunAsync<T>(Func<Task<T>> work)
    {
        var workItem = new WorkItem<T>(work);
        _workQueue.Add(workItem);
        return workItem.Task;
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        _cts.Cancel();
        _workQueue.CompleteAdding();
        
        // Wait for all threads to complete
        foreach (var thread in _threads)
        {
            thread.Join(TimeSpan.FromSeconds(5));
        }
        
        _workQueue.Dispose();
        _cts.Dispose();
        _disposed = true;
    }

    private abstract class WorkItem
    {
        public abstract void Execute();
        public abstract void SetException(Exception ex);
    }

    private class WorkItem<T> : WorkItem
    {
        private readonly Func<Task<T>> _work;
        private readonly TaskCompletionSource<T> _tcs;

        public Task<T> Task => _tcs.Task;

        public WorkItem(Func<Task<T>> work)
        {
            _work = work;
            _tcs = new TaskCompletionSource<T>();
        }

        public override async void Execute()
        {
            try
            {
                var result = await _work();
                _tcs.SetResult(result);
            }
            catch (Exception ex)
            {
                _tcs.SetException(ex);
            }
        }

        public override void SetException(Exception ex)
        {
            _tcs.SetException(ex);
        }
    }

    private class NonGenericWorkItem : WorkItem<object?>
    {
        public NonGenericWorkItem(Func<Task> work) : base(async () =>
        {
            await work();
            return null;
        })
        {
        }
    }
}