#' Refresh cached schema and table metadata
#'
#' @param x A `SqlConnectionTables` object.
#' @param schema Optional schema override for this refresh.
#'
#' @return The updated `SqlConnectionTables` object.
#' @export
refresh_sql_tables <- function(x, schema = NULL) {
  stopifnot(is(x, "SqlConnectionTables"))

  backend <- .sqlconnect_backend(x)
  schema_value <- if (is.null(schema)) .sqlconnect_get_active_schema(x) else as.character(schema)[1]

  x@cache$schemas <- .sqlconnect_list_schemas(x@con, backend)
  x@cache$active_schema <- schema_value
  .sqlconnect_set_cached_tables(x, schema_value, .sqlconnect_list_tables(x@con, schema_value, backend))
  x
}

#' Create an S4 table accessor over a DBI connection
#'
#' @param con A valid DBI connection.
#' @param schema Optional schema name. If `NULL`, schema is inferred when possible.
#' @param cache_names Whether to cache table names. If `TRUE`, names are cached
#'   at construction; if `FALSE`, table names are fetched on each `names()` call.
#'
#' @return A `SqlConnectionTables` object.
#' @export
sql_tables <- function(con, schema = NULL, cache_names = TRUE) {
  if (!dbIsValid(con)) {
    stop("`con` must be a valid DBI connection.")
  }

  cache <- new.env(parent = emptyenv())
  cache$tables <- list()
  cache$schemas <- character(0)
  cache$active_schema <- if (is.null(schema)) "" else as.character(schema)[1]
  cache$backend <- ""
  cache$cache_names <- isTRUE(cache_names)

  x <- new(
    "SqlConnectionTables",
    con = con,
    schema = if (is.null(schema)) "" else as.character(schema)[1],
    cache = cache
  )

  if (isTRUE(cache_names)) {
    x <- refresh_sql_tables(x)
  }

  x
}
