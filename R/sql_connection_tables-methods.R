setMethod("names", "SqlConnectionTables", function(x) {
  schema <- .sqlconnect_get_active_schema(x)
  tables <- .sqlconnect_get_cached_tables(x, schema)
  should_cache <- isTRUE(x@cache$cache_names %||% TRUE)

  if (should_cache && (is.null(tables) || length(tables) == 0L)) {
    backend <- .sqlconnect_backend(x)
    tables <- .sqlconnect_list_tables(x@con, schema, backend)
    .sqlconnect_set_cached_tables(x, schema, tables)
  }

  if (!should_cache) {
    backend <- .sqlconnect_backend(x)
    tables <- .sqlconnect_list_tables(x@con, schema, backend)
  }

  tables
})

setMethod("$", "SqlConnectionTables", function(x, name) {
  x[[name]]
})

setMethod("[[", "SqlConnectionTables", function(x, i, j, ...) {
  tables <- names(x)
  i <- as.character(i)[1]

  if (!i %in% tables) {
    message(sprintf("Table `%s` is not in the cached table list.", i))
    return(NULL)
  }

  schema <- .sqlconnect_get_active_schema(x)
  if (nzchar(schema)) {
    tbl(x@con, Id(schema = schema, table = i))
  } else {
    tbl(x@con, i)
  }
})

setMethod(".DollarNames", "SqlConnectionTables", function(x, pattern = "") {
  tbls <- names(x)
  if (!nzchar(pattern)) {
    return(tbls)
  }
  grep(pattern, tbls, value = TRUE)
})

setMethod("show", "SqlConnectionTables", function(object) {
  schema_txt <- active_schema(object)
  if (!nzchar(schema_txt)) {
    schema_txt <- "<unresolved>"
  }
  cat(sprintf("SqlConnectionTables(schema = %s, backend = %s)\n", schema_txt, .sqlconnect_backend(object)))
  cat("Use names(x) for cached tables, schemas(x) for cached/discovered schemas.\n")
})

#' List schemas available to a `SqlConnectionTables` object
#'
#' @param x A `SqlConnectionTables` object.
#'
#' @return A character vector of schema names.
#' @export
setGeneric("schemas", function(x) standardGeneric("schemas"))

setMethod("schemas", "SqlConnectionTables", function(x) {
  current <- x@cache$schemas
  if (length(current) == 0L) {
    current <- .sqlconnect_list_schemas(x@con, .sqlconnect_backend(x))
    x@cache$schemas <- current
  }
  current
})

#' Get the active schema used by a `SqlConnectionTables` object
#'
#' @param x A `SqlConnectionTables` object.
#'
#' @return A single schema name, or `\"\"` when unresolved.
#' @export
setGeneric("active_schema", function(x) standardGeneric("active_schema"))

setMethod("active_schema", "SqlConnectionTables", function(x) {
  .sqlconnect_get_active_schema(x)
})
