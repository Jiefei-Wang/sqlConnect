# sqlConnect

`sqlConnect` provides a small S4 wrapper around a `DBI` connection to make table discovery and access more convenient.

## Features

- Detects active schema and available schemas
- Lists tables with backend-aware metadata queries
- Supports Oracle, PostgreSQL, SQL Server, MySQL, SQLite, and Snowflake
- Accesses tables with `$` and `[[` syntax
- Optional table-name caching via `cache_names`

## Installation

From GitHub:

```r
devtools::install_github("Jiefei-Wang/sqlConnect")
```



## Quick Start

```r
library(DBI)
library(RSQLite)
library(sqlConnect)

# Dummy in-memory database
con <- dbConnect(RSQLite::SQLite(), ":memory:")
dbExecute(con, "CREATE TABLE patients (id INTEGER, name TEXT)")
dbExecute(con, "CREATE TABLE visits (id INTEGER, patient_id INTEGER, visit_date TEXT)")
dbExecute(con, "INSERT INTO patients VALUES (1, 'Alice'), (2, 'Bob')")

# Build accessor object
x <- sql_tables(con, cache_names = TRUE)

# Metadata
active_schema(x)
schemas(x)
names(x)

# Table access
patients <- x$patients
visits <- x[["visits"]]
```

## Caching Behavior

- `cache_names = TRUE`: table names are cached at construction and reused.
- `cache_names = FALSE`: table names are fetched on each `names(x)` call.

## License

MIT
