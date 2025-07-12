using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Lakepipe.Streaming.Buffers;

/// <summary>
/// High-performance buffer pool optimized for streaming workloads.
/// </summary>
public sealed class StreamingBufferPool : IDisposable
{
    private readonly ArrayPool<byte> _arrayPool;
    private readonly int _defaultBufferSize;
    private readonly int _maxBufferSize;
    private readonly ILogger<StreamingBufferPool> _logger;
    private long _totalAllocated;
    private long _totalReturned;
    private long _currentlyRented;

    public StreamingBufferPool(ILogger<StreamingBufferPool> logger, int defaultBufferSize = 65536, int maxBufferSize = 1048576)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _defaultBufferSize = defaultBufferSize;
        _maxBufferSize = maxBufferSize;
        
        // Create a custom array pool with specific characteristics for streaming
        _arrayPool = ArrayPool<byte>.Create(maxBufferSize, 50);
    }

    /// <summary>
    /// Rents a buffer from the pool.
    /// </summary>
    public RentedBuffer RentBuffer(int minimumSize = -1)
    {
        if (minimumSize <= 0)
            minimumSize = _defaultBufferSize;
        
        if (minimumSize > _maxBufferSize)
            throw new ArgumentException($"Requested buffer size {minimumSize} exceeds maximum {_maxBufferSize}");
        
        var buffer = _arrayPool.Rent(minimumSize);
        Interlocked.Increment(ref _currentlyRented);
        Interlocked.Add(ref _totalAllocated, buffer.Length);
        
        return new RentedBuffer(this, buffer, minimumSize);
    }

    /// <summary>
    /// Returns a buffer to the pool.
    /// </summary>
    internal void ReturnBuffer(byte[] buffer, bool clearBuffer = false)
    {
        _arrayPool.Return(buffer, clearBuffer);
        Interlocked.Decrement(ref _currentlyRented);
        Interlocked.Add(ref _totalReturned, buffer.Length);
    }

    /// <summary>
    /// Gets current pool statistics.
    /// </summary>
    public BufferPoolStatistics GetStatistics()
    {
        return new BufferPoolStatistics
        {
            TotalAllocated = _totalAllocated,
            TotalReturned = _totalReturned,
            CurrentlyRented = _currentlyRented,
            EfficiencyRatio = _totalAllocated > 0 ? (double)_totalReturned / _totalAllocated : 0
        };
    }

    public void Dispose()
    {
        if (_currentlyRented > 0)
        {
            _logger.LogWarning("Disposing buffer pool with {Count} buffers still rented", _currentlyRented);
        }
    }
}

/// <summary>
/// Represents a rented buffer that automatically returns to the pool when disposed.
/// </summary>
public readonly struct RentedBuffer : IDisposable
{
    private readonly StreamingBufferPool _pool;
    private readonly byte[] _buffer;
    private readonly int _requestedSize;

    internal RentedBuffer(StreamingBufferPool pool, byte[] buffer, int requestedSize)
    {
        _pool = pool;
        _buffer = buffer;
        _requestedSize = requestedSize;
    }

    public byte[] Buffer => _buffer;
    public int Length => _requestedSize;
    public Memory<byte> Memory => _buffer.AsMemory(0, _requestedSize);
    public Span<byte> Span => _buffer.AsSpan(0, _requestedSize);

    public void Dispose()
    {
        _pool?.ReturnBuffer(_buffer);
    }
}

/// <summary>
/// Buffer pool statistics.
/// </summary>
public record BufferPoolStatistics
{
    public long TotalAllocated { get; init; }
    public long TotalReturned { get; init; }
    public long CurrentlyRented { get; init; }
    public double EfficiencyRatio { get; init; }
}

/// <summary>
/// Memory-efficient streaming buffer for processing large data streams.
/// </summary>
public sealed class StreamingBuffer : IDisposable
{
    private readonly StreamingBufferPool _pool;
    private readonly List<RentedBuffer> _segments;
    private readonly int _segmentSize;
    private RentedBuffer _currentSegment;
    private int _currentPosition;
    private long _totalLength;
    private bool _disposed;

    public StreamingBuffer(StreamingBufferPool pool, int segmentSize = 65536)
    {
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        _segmentSize = segmentSize;
        _segments = new List<RentedBuffer>();
        _currentSegment = _pool.RentBuffer(_segmentSize);
        _currentPosition = 0;
        _totalLength = 0;
    }

    public long Length => _totalLength;
    public int SegmentCount => _segments.Count + 1;

    /// <summary>
    /// Writes data to the streaming buffer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> data)
    {
        while (data.Length > 0)
        {
            var availableSpace = _segmentSize - _currentPosition;
            
            if (availableSpace == 0)
            {
                // Current segment is full, get a new one
                _segments.Add(_currentSegment);
                _currentSegment = _pool.RentBuffer(_segmentSize);
                _currentPosition = 0;
                availableSpace = _segmentSize;
            }
            
            var bytesToWrite = Math.Min(data.Length, availableSpace);
            data.Slice(0, bytesToWrite).CopyTo(_currentSegment.Span.Slice(_currentPosition));
            
            _currentPosition += bytesToWrite;
            _totalLength += bytesToWrite;
            data = data.Slice(bytesToWrite);
        }
    }

    /// <summary>
    /// Creates a memory stream view of the buffer contents.
    /// </summary>
    public ReadOnlyMemoryStream CreateReadStream()
    {
        var memories = new List<ReadOnlyMemory<byte>>();
        
        // Add all completed segments
        foreach (var segment in _segments)
        {
            memories.Add(segment.Memory);
        }
        
        // Add current segment if it has data
        if (_currentPosition > 0)
        {
            memories.Add(_currentSegment.Memory.Slice(0, _currentPosition));
        }
        
        return new ReadOnlyMemoryStream(memories);
    }

    /// <summary>
    /// Resets the buffer for reuse.
    /// </summary>
    public void Reset()
    {
        // Return all segments except the current one
        foreach (var segment in _segments)
        {
            segment.Dispose();
        }
        _segments.Clear();
        
        _currentPosition = 0;
        _totalLength = 0;
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        foreach (var segment in _segments)
        {
            segment.Dispose();
        }
        
        _currentSegment.Dispose();
        _disposed = true;
    }
}

/// <summary>
/// Read-only stream over multiple memory segments.
/// </summary>
public class ReadOnlyMemoryStream : Stream
{
    private readonly IReadOnlyList<ReadOnlyMemory<byte>> _memories;
    private int _currentSegmentIndex;
    private int _currentSegmentPosition;
    private readonly long _length;
    private long _position;

    public ReadOnlyMemoryStream(IReadOnlyList<ReadOnlyMemory<byte>> memories)
    {
        _memories = memories ?? throw new ArgumentNullException(nameof(memories));
        _length = memories.Sum(m => m.Length);
        _position = 0;
        _currentSegmentIndex = 0;
        _currentSegmentPosition = 0;
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _length;
    
    public override long Position
    {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (buffer == null) throw new ArgumentNullException(nameof(buffer));
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (offset + count > buffer.Length) throw new ArgumentException("Buffer too small");
        
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        var totalRead = 0;
        
        while (buffer.Length > 0 && _currentSegmentIndex < _memories.Count)
        {
            var currentSegment = _memories[_currentSegmentIndex].Span;
            var availableInSegment = currentSegment.Length - _currentSegmentPosition;
            
            if (availableInSegment == 0)
            {
                _currentSegmentIndex++;
                _currentSegmentPosition = 0;
                continue;
            }
            
            var toRead = Math.Min(buffer.Length, availableInSegment);
            currentSegment.Slice(_currentSegmentPosition, toRead).CopyTo(buffer);
            
            buffer = buffer.Slice(toRead);
            _currentSegmentPosition += toRead;
            _position += toRead;
            totalRead += toRead;
        }
        
        return totalRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var newPosition = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentException("Invalid seek origin")
        };
        
        if (newPosition < 0 || newPosition > _length)
            throw new ArgumentOutOfRangeException(nameof(offset));
        
        _position = newPosition;
        
        // Find the correct segment and position
        _currentSegmentIndex = 0;
        _currentSegmentPosition = 0;
        var remainingPosition = newPosition;
        
        while (_currentSegmentIndex < _memories.Count && remainingPosition > 0)
        {
            var segmentLength = _memories[_currentSegmentIndex].Length;
            if (remainingPosition >= segmentLength)
            {
                remainingPosition -= segmentLength;
                _currentSegmentIndex++;
            }
            else
            {
                _currentSegmentPosition = (int)remainingPosition;
                break;
            }
        }
        
        return _position;
    }

    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}