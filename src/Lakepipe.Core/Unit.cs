namespace Lakepipe.Core;

/// <summary>
/// Represents a unit value (similar to void but as a type).
/// </summary>
public readonly struct Unit : IEquatable<Unit>
{
    /// <summary>
    /// Gets the single Unit value.
    /// </summary>
    public static Unit Value { get; } = new();

    public bool Equals(Unit other) => true;

    public override bool Equals(object? obj) => obj is Unit;

    public override int GetHashCode() => 0;

    public override string ToString() => "()";

    public static bool operator ==(Unit left, Unit right) => true;

    public static bool operator !=(Unit left, Unit right) => false;
}