# boe-mounter

Example application built with the [Spool framework](https://github.com/spool-framework) that demonstrates how to use a **Mounter** to consume BOE (Boletín Oficial del Estado) publication records from a data lake, download their associated files (PDF, HTML, XML), and persist them back to a partitioned data lake organized by publication date.

## What it does

1. Reads `GenericRecord` entries from a partitioned data lake source under `D:/spool/datalake`, scoped to today's BOE partition.
2. For each record, downloads the three file formats published by the BOE (PDF, HTML, XML) via HTTP.
3. Detects the content type of each downloaded payload automatically.
4. Writes the files back to the data lake partitioned by **publication date** (year / month / day) and **file type**, instead of the ingestion date.
5. Emits mount events to the configured event bus after each successful write.

The polling policy is set to `ONCE`, so the process runs a single pass and exits — suitable for scheduled/batch execution (e.g. a daily cron job).

## Architecture overview

```
Data Lake (source)
   └─ year=YYYY/month=MM/day=DD/source=boe
           │
           ▼
   ToBinaryFileMountAggregator          ← downloads PDF + HTML + XML per record
           │
           ▼
   ContentType auto-detection
           │
           ▼
Data Lake (target: RAW_FILE_SYSTEM)
   └─ year=YYYY/month=MM/day=DD/fileType=<pdf|html|xml>
```

## Key Spool concepts shown

| Concept | Where |
|---|---|
| `SpoolNode` | `Application` — entry point that wires and starts the runtime |
| `Mounter` | `Application#initializeMounter()` — polling-based data mover |
| `MountAggregator` | `ToBinaryFileMountAggregator` — transforms records into binary payloads |
| `PartitionKey` | `buildDatePartitionKeyFrom()` — dynamic partition derived from record data |
| `PollingPolicy.ONCE` | single-pass execution |
| `AlwaysClosedWindowPolicy` | processes the target partition as already closed (no incremental windowing) |
| `NoOpMountCheckpoint` | no checkpoint state — full re-run every execution |
| `PluginResolver` | resolves `PartitionedReaderProvider`, `EventBusProvider`, `DataMartWriterProvider` from the classpath via SPI |

## Project structure

```
boe-mounter/
├── src/main/java/software/example/spool/boe/
│   ├── Main.java                       # entry point
│   ├── Application.java                # Spool node wiring
│   ├── ToBinaryFileMountAggregator.java # downloads BOE files per record
│   └── HTTPUtils.java                  # thin HTTP client wrapper
└── pom.xml
```

## Prerequisites

- Java 21
- Maven 3.8+
- Spool framework runtime (`io.github.spool-framework:runtime:1.0.0-SNAPSHOT`) installed in your local Maven repository
- A populated data lake at `D:/spool/datalake` with BOE records ingested for today

## Build

```bash
mvn package
```

This produces a shaded JAR at `target/boe-mounter.jar` with all dependencies bundled.

## Run

```bash
java -jar target/boe-mounter.jar
```

The process reads the current day's BOE partition, downloads all referenced files, writes them to the data lake, and exits.

## Partition layout

### Source (input)

```
D:/spool/datalake/bronze/year=YYYY/month=MM/day=DD/source=boe/
```

Records are expected to contain a nested `payload` object with at least:

| Field | Description |
|---|---|
| `publish_date` | Publication date in `yyyyMMdd` format (e.g. `20260603`) |
| `url_pdf` | URL to the PDF version of the BOE item |
| `url_html` | URL to the HTML version |
| `url_xml` | URL to the XML version |

### Target (output)

```
D:/spool/datalake/silver/year=YYYY/month=MM/day=DD/fileType=<pdf|html|xml>/
```

The partition date is derived from `publish_date` in the record, not from today's date.

## Part of the Spool Labs collection

This repository is one of several example applications that demonstrate real-world usage of the Spool framework. Each example targets a different combination of Spool components (Ingester, Mounter, Processor, etc.) and infrastructure plugins.

---

> Built as part of a final degree project (TFT) showcasing the Spool data engineering framework.
