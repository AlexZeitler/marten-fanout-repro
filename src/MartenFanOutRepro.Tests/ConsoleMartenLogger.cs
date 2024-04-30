using System.Collections;
using System.Diagnostics;
using Marten;
using Marten.Services;
using Npgsql;

namespace MartenFanOutRepro.Tests;

public class ConsoleMartenLogger: IMartenLogger, IMartenSessionLogger
{
    private Stopwatch? _stopwatch;

    public IMartenSessionLogger StartSession(IQuerySession session)
    {
        return this;
    }

    public void SchemaChange(string sql)
    {
        Console.WriteLine("Executing DDL change:");
        Console.WriteLine(sql);
        Console.WriteLine();
    }

    public void LogSuccess(NpgsqlCommand command)
    {
        Console.WriteLine(command.CommandText);
        foreach (var p in command.Parameters.OfType<NpgsqlParameter>())
            Console.WriteLine($"  {p.ParameterName}: {GetParameterValue(p)}");
    }

    public void LogSuccess(NpgsqlBatch batch)
    {
        foreach (var command in batch.BatchCommands)
        {
            Console.WriteLine(command.CommandText);
            foreach (var p in command.Parameters.OfType<NpgsqlParameter>())
                Console.WriteLine($"  {p.ParameterName}: {GetParameterValue(p)}");
        }
    }

    private static object? GetParameterValue(NpgsqlParameter p)
    {
        if (p.Value is IList enumerable)
        {
            var result = "";
            for (var i = 0; i < Math.Min(enumerable.Count, 5); i++)
            {
                result += $"[{i}] {enumerable[i]}; ";
            }
            if (enumerable.Count > 5) result += $" + {enumerable.Count - 5} more";
            return result;
        }
        return p.Value;
    }

    public void LogFailure(NpgsqlCommand command, Exception ex)
    {
        Console.WriteLine("Postgresql command failed!");
        Console.WriteLine(command.CommandText);
        foreach (var p in command.Parameters.OfType<NpgsqlParameter>())
            Console.WriteLine($"  {p.ParameterName}: {p.Value}");
        Console.WriteLine(ex);
    }

    public void LogFailure(NpgsqlBatch batch, Exception ex)
    {
        Console.WriteLine("Postgresql command failed!");
        foreach (var command in batch.BatchCommands)
        {
            Console.WriteLine(command.CommandText);
            foreach (var p in command.Parameters.OfType<NpgsqlParameter>())
                Console.WriteLine($"  {p.ParameterName}: {p.Value}");
        }

        Console.WriteLine(ex);
    }

    public void LogFailure(Exception ex, string message)
    {
        Console.WriteLine("Failure: " + message);
        Console.WriteLine(ex.ToString());
    }

    public void RecordSavedChanges(IDocumentSession session, IChangeSet commit)
    {
        _stopwatch?.Stop();

        var lastCommit = commit;
        Console.WriteLine(
            $"Persisted {lastCommit.Updated.Count()} updates in {_stopwatch?.ElapsedMilliseconds ?? 0} ms, {lastCommit.Inserted.Count()} inserts, and {lastCommit.Deleted.Count()} deletions");
    }

    public void OnBeforeExecute(NpgsqlCommand command)
    {
        _stopwatch = new Stopwatch();
        _stopwatch.Start();
    }

    public void OnBeforeExecute(NpgsqlBatch batch)
    {
        _stopwatch = new Stopwatch();
        _stopwatch.Start();
    }
}
