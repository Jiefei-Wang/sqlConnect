.sqlconnect_schema_queries <- function(backend) {
  switch(
    backend,
    oracle = c("SELECT username AS schema_name FROM all_users ORDER BY username"),
    postgres = c(
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema') AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name"
    ),
    sqlserver = c("SELECT name AS schema_name FROM sys.schemas ORDER BY name"),
    mysql = c("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"),
    sqlite = c("PRAGMA database_list"),
    snowflake = c("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"),
    c("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
  )
}

.sqlconnect_current_schema_queries <- function(backend) {
  switch(
    backend,
    oracle = c("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS schema_name FROM dual"),
    postgres = c("SELECT current_schema() AS schema_name"),
    sqlserver = c("SELECT SCHEMA_NAME() AS schema_name"),
    mysql = c("SELECT DATABASE() AS schema_name"),
    sqlite = character(0),
    snowflake = c("SELECT CURRENT_SCHEMA() AS schema_name"),
    c("SELECT current_schema() AS schema_name")
  )
}

.sqlconnect_table_queries <- function(backend, schema = "") {
  schema_q <- .sqlconnect_quote_literal(schema)
  schema_u <- toupper(schema_q)
  has_schema <- nzchar(schema_q)

  where_clause <- function(condition) {
    if (nzchar(condition)) {
      glue(" WHERE {condition}")
    } else {
      ""
    }
  }

  switch(
    backend,
    oracle = {
      table_source <- if (has_schema) "all_tables" else "user_tables"
      view_source <- if (has_schema) "all_views" else "user_views"
      object_source <- if (has_schema) "all_objects" else "user_objects"
      owner_filter <- if (has_schema) glue("owner = '{schema_u}'") else ""
      object_filter <- if (has_schema) {
        glue("{owner_filter} AND object_type IN ('TABLE','VIEW')")
      } else {
        "object_type IN ('TABLE','VIEW')"
      }

      c(
        glue("SELECT table_name FROM {table_source}{where_clause(owner_filter)} ORDER BY table_name"),
        glue("SELECT view_name AS table_name FROM {view_source}{where_clause(owner_filter)} ORDER BY view_name"),
        glue("SELECT object_name AS table_name FROM {object_source}{where_clause(object_filter)} ORDER BY object_name")
      )
    },
    postgres = {
      schema_filter <- if (has_schema) {
        glue("table_schema = '{schema_q}'")
      } else {
        "table_schema NOT IN ('information_schema') AND table_schema NOT LIKE 'pg_%'"
      }
      c(
        glue(
          "SELECT table_name FROM information_schema.tables WHERE {schema_filter} AND table_type IN ('BASE TABLE','VIEW') ORDER BY table_name"
        )
      )
    },
    sqlserver = {
      schema_filter <- if (has_schema) glue("s.name = '{schema_q}'") else ""
      c(
        glue(
          "SELECT t.name AS table_name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id{where_clause(schema_filter)} ORDER BY t.name"
        ),
        glue(
          "SELECT v.name AS table_name FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id{where_clause(schema_filter)} ORDER BY v.name"
        )
      )
    },
    mysql = {
      schema_filter <- if (has_schema) glue("table_schema = '{schema_q}'") else "table_schema = DATABASE()"
      c(
        glue(
          "SELECT table_name FROM information_schema.tables WHERE {schema_filter} AND table_type IN ('BASE TABLE','VIEW') ORDER BY table_name"
        )
      )
    },
    sqlite = c("SELECT name AS table_name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"),
    snowflake = {
      schema_filter <- if (has_schema) {
        glue("table_schema = '{toupper(schema_q)}'")
      } else {
        "table_schema = CURRENT_SCHEMA()"
      }
      c(glue("SELECT table_name FROM information_schema.tables WHERE {schema_filter} ORDER BY table_name"))
    },
    {
      schema_filter <- if (has_schema) glue(" WHERE table_schema = '{schema_q}'") else ""
      c(glue("SELECT table_name FROM information_schema.tables{schema_filter} ORDER BY table_name"))
    }
  )
}

.sqlconnect_list_schemas <- function(con, backend) {
  vals <- .sqlconnect_run_queries(con, .sqlconnect_schema_queries(backend))

  # SQLite PRAGMA returns seq,name,file; schema name is second column, so fix if needed.
  if (backend == "sqlite" && length(vals) > 0L && all(grepl("^[0-9]+$", vals))) {
    out <- tryCatch(dbGetQuery(con, "PRAGMA database_list"), error = function(e) NULL)
    if (!is.null(out) && "name" %in% names(out)) {
      vals <- unique(as.character(out$name))
      vals <- vals[nzchar(vals)]
    }
  }

  vals
}

.sqlconnect_infer_active_schema <- function(con, backend) {
  current <- .sqlconnect_run_queries(con, .sqlconnect_current_schema_queries(backend))
  if (length(current) > 0L) {
    return(current[1L])
  }

  schemas <- .sqlconnect_list_schemas(con, backend)
  if (length(schemas) == 1L) {
    return(schemas[1L])
  }

  ""
}

.sqlconnect_list_tables <- function(con, schema = "", backend) {
  vals <- .sqlconnect_run_queries(con, .sqlconnect_table_queries(backend, schema))
  if (length(vals) > 0L) {
    return(vals)
  }

  # Unknown or restricted metadata: keep DBI as final compatibility fallback.
  out <- tryCatch(dbListTables(con), error = function(e) character(0))
  out <- as.character(out)
  if (!nzchar(schema)) {
    return(unique(out))
  }

  prefixed <- out[startsWith(out, paste0(schema, "."))]
  if (length(prefixed) > 0L) {
    return(unique(sub("^[^.]+\\.", "", prefixed)))
  }

  unique(out)
}
