setClass(
  "SqlConnectionTables",
  slots = c(
    con = "ANY",
    schema = "character",
    cache = "environment"
  )
)
