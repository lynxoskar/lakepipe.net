# 🚀 Lakepipe

[![License MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE) [![GitHub stars](https://img.shields.io/github/stars/lynxoskar/lakepipe.net?style=social)](https://github.com/lynxoskar/lakepipe.net/stargazers)

**Lakepipe** is a high-performance, extensible data-pipeline engine written in C# / .NET.  It lets you declaratively stitch together streaming or batch dataflows that read from many different sources, transform the data using best-in-class engines like Apache Arrow or DuckDB, and deliver the results to a variety of sinks – all with a single JSON/YAML configuration file.

> ✨  Whether you need to materialise a Kafka topic to Parquet, run real-time analytics on Arrow streams, or fan-out data to S3 – Lakepipe aims to make it **fast**, **observable**, and **fun**.

---

## Key features

* 🔌 **Universal sources** – Kafka (streaming & batch), local files, Parquet, Apache Arrow streams, Amazon S3, DuckDB and more.
* ♻️ **Pluggable transforms** – out-of-the-box support for:
  * Apache Arrow Compute kernels
  * DuckDB SQL queries
  * Your own custom processors
* ⚡ **Blazing speed** – built on TPL Dataflow with fine-grained back-pressure, batching, parallelism and optional CPU-affinity pinning.
* 🗄️ **Hybrid caching** – in-memory ⚡ + disk-backed FASTER cache for repeatable reads.
* 📊 **First-class observability** – Serilog-based structured logging and an extensible metrics collector (OpenTelemetry-ready).
* 🔋 **Batteries-included CLI** – run, validate, list and test pipelines from the terminal.

---

## Quick start

### 1.  Install prerequisites

* [.NET 8 SDK](https://dotnet.microsoft.com/download) or newer
* (Optional) Docker for running local Kafka, DuckDB etc.

### 2.  Clone & restore

```bash
# clone the repository
git clone https://github.com/your-org/lakepipe.git
cd lakepipe
# restore NuGet packages
dotnet restore
```

### 3.  Run the **simple** example

```bash
# execute the CLI, pointing it at the example pipeline configuration
# this will read from a Kafka topic and write a Parquet file

dotnet run --project src/Lakepipe.Cli -- run examples/simple_pipeline.json
```

When it finishes you should see an `output.parquet` file in `/tmp`.


---

## Anatomy of a pipeline

A pipeline is described by a single JSON (or YAML) document.  Here is a minimal example (taken from `examples/simple_pipeline.json`):

```jsonc
{
  "Log": {
    "Level": "Information",
    "Console": true
  },
  "Source": {
    "Uri": "kafka://localhost:9092/input-topic",
    "Format": "Kafka",
    "Kafka": {
      "BootstrapServers": ["localhost:9092"],
      "Topic": "input-topic",
      "GroupId": "my-consumer-group",
      "AutoOffsetReset": "Earliest",
      "Serialization": "Json"
    }
  },
  "Transform": {
    "Engine": "Arrow",
    "Operations": [
      {
        "Type": "AddColumn",
        "Config": {
          "columnName": "processed_at",
          "value": "2025-01-01T00:00:00Z"
        }
      }
    ]
  },
  "Sink": {
    "Uri": "file:///tmp/output.parquet",
    "Format": "Parquet"
  }
}
```

**Sections**

* **Log** – controls log level and sinks (console, files, etc.)
* **Source** – where data comes from; the URI scheme determines the connector (`kafka://`, `file://`, `s3://`, ...).
* **Transform** – zero or more operations executed in order.  Pick the engine (`Arrow`, `DuckDb`, `Custom`) and list individual operations.
* **Sink** – where the processed data goes (`file://`, `kafka://`, ...).
* **Streaming** *(optional)* – fine-tune batch sizes, parallelism, CPU pinning, back-pressure and more.

---

## CLI reference

```bash
lakepipe run <config-file> [options]      # Execute a pipeline
lakepipe validate <config-file>           # Semantic & schema validation
lakepipe list                             # List built-in sources / sinks / transforms
lakepipe test kafka --bootstrap-servers   # Connectivity tester helpers
```

Use the `--help` flag on any command for the full set of options.

---

## Extending Lakepipe

Want to add a new connector or transformation engine?  Implement the relevant interfaces (`ISource`, `ISink`, `ITransformProcessor`) in *your* assembly and point Lakepipe at it by referencing your project or dropping the DLL next to the CLI.  The system was designed from the ground up to be modular and testable.

---

## Contributing

1.  Fork the repo and create your feature branch (`git checkout -b my-new-feature`).
2.  Commit your changes (`git commit -am 'Add some feature'`).
3.  Push to the branch (`git push origin my-new-feature`).
4.  Create a new Pull Request.

Bug reports and ⭐ stars are equally welcome!

---

## License

Lakepipe is released under the [MIT License](LICENSE).  Commercial support is available on request. 