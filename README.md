# SyncLite DBReader — Smart Database ETL / Replication / Migration Tool

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite DBReader?

**SyncLite DBReader** is a web-based tool for setting up scalable, incremental, many-to-many database ETL, replication, and migration pipelines. It reads data from source databases (through JDBC, incremental queries, or native CDC/log-based replication where supported) and feeds the data into the SyncLite pipeline, which [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) then delivers to one or more destination databases, data warehouses, or data lakes.

Unlike heavy ETL platforms, SyncLite DBReader is lightweight, configuration-driven, and deployable in minutes.

```
Source DB(s)  ──▶  SyncLite DBReader  ──▶  Staging Storage  ──▶  SyncLite Consolidator  ──▶  Destination(s)
```

## Key Features

- **Incremental / delta replication** — processes only changed rows since the last run using user-defined watermark columns or native CDC
- **Log-based CDC** — where supported by the source database, captures changes at the binary log level for near-zero-latency replication
- **Many source → many destination** — one DBReader job can replicate from multiple source databases into multiple destinations simultaneously
- **Schema inference** — automatically maps source schema to destination; supports custom overrides
- **Table / column filtering** — include or exclude specific tables and columns
- **Scheduling** — run on-demand or on a cron schedule via [SyncLite Job Monitor](https://github.com/syncliteio/synclite-job-monitor)
- **Web UI** — full job configuration, progress tracking, and error monitoring from a browser
- **Data migration** — one-time full-load migration with a single job configuration

## Source Databases Supported

| Category | Databases |
|---|---|
| Relational | PostgreSQL, MySQL, MariaDB, Microsoft SQL Server, Oracle Database, IBM DB2 |
| Embedded | SQLite, DuckDB, Apache Derby, H2, HyperSQL |
| Analytics | ClickHouse |
| Files | CSV files, Apache Parquet |

## Quick Start

1. Deploy the SyncLite platform (see [platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)).
2. Open http://localhost:8080/synclite-dbreader
3. Open http://localhost:8080/synclite-consolidator and configure a destination
4. In DBReader, click **Configure Job**, fill in your source JDBC connection details, select tables, and start the job.
5. Monitor replication progress in the DBReader dashboard and destination data in the Consolidator UI.

## Build

```bash
cd synclite-dbreader/root
mvn -Drevision=oss clean install
```

Built WAR: `root/web/target/synclite-dbreader-oss.war`

## Related Components

| Component | Role |
|---|---|
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Receives and delivers the replicated data to destinations |
| [SyncLite Job Monitor](https://github.com/syncliteio/synclite-job-monitor) | Schedules and monitors DBReader jobs |
| [SyncLite Validator](https://github.com/syncliteio/synclite-validator) | End-to-end validation of replication pipelines |

## Documentation & Community

- Full documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- Smart database ETL solution: https://www.synclite.io/solutions/smart-database-etl
- Website: https://www.synclite.io
- Community: https://github.com/syncliteio/SyncLite/issues

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)
