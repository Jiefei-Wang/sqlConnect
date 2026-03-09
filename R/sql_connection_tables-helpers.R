`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

.sqlconnect_cache_key <- function(schema) {
  if (nzchar(schema)) schema else "<default>"
}

.sqlconnect_quote_literal <- function(x) {
  gsub("'", "''", as.character(x)[1], fixed = TRUE)
}

.sqlconnect_query_first_col <- function(con, sql) {
  out <- tryCatch(dbGetQuery(con, sql), error = function(e) NULL)
  if (is.null(out) || ncol(out) == 0L || nrow(out) == 0L) {
    return(character(0))
  }

  vals <- as.character(out[[1]])
  vals <- vals[nzchar(vals)]
  unique(vals)
}

.sqlconnect_detect_backend <- function(con) {
  cls_txt <- tolower(paste(class(con), collapse = " "))
  info <- tryCatch(dbGetInfo(con), error = function(e) list())
  info_txt <- tolower(paste(unname(unlist(info)), collapse = " "))
  txt <- paste(cls_txt, info_txt)

  if (grepl("oracle", txt)) return("oracle")
  if (grepl("postgres|postgresql", txt)) return("postgres")
  if (grepl("sql server|mssql|tds|odbc driver .*sql server", txt)) return("sqlserver")
  if (grepl("mysql|mariadb", txt)) return("mysql")
  if (grepl("sqlite", txt)) return("sqlite")
  if (grepl("snowflake", txt)) return("snowflake")

  "unknown"
}

.sqlconnect_run_queries <- function(con, queries) {
  for (sql in queries) {
    vals <- .sqlconnect_query_first_col(con, sql)
    if (length(vals) > 0L) {
      return(vals)
    }
  }
  character(0)
}

.sqlconnect_get_cached_tables <- function(x, schema) {
  key <- .sqlconnect_cache_key(schema)
  x@cache$tables[[key]]
}

.sqlconnect_set_cached_tables <- function(x, schema, tables) {
  key <- .sqlconnect_cache_key(schema)
  x@cache$tables[[key]] <- unique(as.character(tables))
  invisible(NULL)
}

.sqlconnect_backend <- function(x) {
  cached <- x@cache$backend %||% ""
  if (nzchar(cached)) {
    return(cached)
  }

  backend <- .sqlconnect_detect_backend(x@con)
  x@cache$backend <- backend
  backend
}

.sqlconnect_get_active_schema <- function(x) {
  if (nzchar(x@schema)) {
    return(x@schema)
  }

  if (nzchar(x@cache$active_schema %||% "")) {
    return(x@cache$active_schema)
  }

  backend <- .sqlconnect_backend(x)
  inferred <- .sqlconnect_infer_active_schema(x@con, backend)
  x@cache$active_schema <- inferred
  inferred
}
