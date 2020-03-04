# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'


get_table_data <- function(tablename) {
  library(rJava)
  library(RJDBC)
  sql_query = paste("SELECT * FROM ",tablename)
  jdbcDriver <- JDBC(driverClass="org.apache.ignite.IgniteJdbcThinDriver", classPath="/opt/ignite-core-2.7.0.jar")
  jdbcConnection <- dbConnect(jdbcDriver, paste("jdbc:ignite:thin://10.34.121.188"), "ignite", "ignite")
  workflow_dataframe <- dbGetQuery(jdbcConnection, sql_query)
  workflow_dataframe
}
