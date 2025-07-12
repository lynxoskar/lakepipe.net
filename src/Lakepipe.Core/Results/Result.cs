namespace Lakepipe.Core.Results;

/// <summary>
/// Represents the result of an operation that can either succeed or fail.
/// </summary>
/// <typeparam name="T">The type of the success value.</typeparam>
public readonly struct Result<T>
{
    private readonly T? _value;
    private readonly string? _error;
    private readonly bool _isSuccess;

    private Result(T? value, string? error, bool isSuccess)
    {
        _value = value;
        _error = error;
        _isSuccess = isSuccess;
    }

    /// <summary>
    /// Gets a value indicating whether the result is a success.
    /// </summary>
    public bool IsSuccess => _isSuccess;

    /// <summary>
    /// Gets a value indicating whether the result is a failure.
    /// </summary>
    public bool IsFailure => !_isSuccess;

    /// <summary>
    /// Gets the success value. Throws if the result is a failure.
    /// </summary>
    public T Value => _isSuccess ? _value! : throw new InvalidOperationException($"Cannot access value of a failed result. Error: {_error}");

    /// <summary>
    /// Gets the error message. Returns null if the result is a success.
    /// </summary>
    public string? Error => _error;

    /// <summary>
    /// Creates a successful result.
    /// </summary>
    public static Result<T> Success(T value) => new(value, null, true);

    /// <summary>
    /// Creates a failed result.
    /// </summary>
    public static Result<T> Failure(string error) => new(default, error, false);

    /// <summary>
    /// Maps the success value to a new type.
    /// </summary>
    public Result<TNew> Map<TNew>(Func<T, TNew> mapper)
    {
        return _isSuccess 
            ? Result<TNew>.Success(mapper(_value!)) 
            : Result<TNew>.Failure(_error!);
    }

    /// <summary>
    /// Binds the result to a function that returns a new result.
    /// </summary>
    public Result<TNew> Bind<TNew>(Func<T, Result<TNew>> binder)
    {
        return _isSuccess 
            ? binder(_value!) 
            : Result<TNew>.Failure(_error!);
    }

    /// <summary>
    /// Executes an action if the result is a success.
    /// </summary>
    public Result<T> OnSuccess(Action<T> action)
    {
        if (_isSuccess)
            action(_value!);
        return this;
    }

    /// <summary>
    /// Executes an action if the result is a failure.
    /// </summary>
    public Result<T> OnFailure(Action<string> action)
    {
        if (!_isSuccess)
            action(_error!);
        return this;
    }

    /// <summary>
    /// Matches the result against success and failure cases.
    /// </summary>
    public TResult Match<TResult>(Func<T, TResult> onSuccess, Func<string, TResult> onFailure)
    {
        return _isSuccess ? onSuccess(_value!) : onFailure(_error!);
    }

    public static implicit operator bool(Result<T> result) => result._isSuccess;
}

/// <summary>
/// Represents the result of an operation without a return value.
/// </summary>
public readonly struct Result
{
    private readonly string? _error;
    private readonly bool _isSuccess;

    private Result(string? error, bool isSuccess)
    {
        _error = error;
        _isSuccess = isSuccess;
    }

    public bool IsSuccess => _isSuccess;
    public bool IsFailure => !_isSuccess;
    public string? Error => _error;

    public static Result Success() => new(null, true);
    public static Result Failure(string error) => new(error, false);

    public static implicit operator bool(Result result) => result._isSuccess;
}