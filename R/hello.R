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

put_table_data <- function(tablename) {
  library(rJava)
  library(RJDBC)
  jdbcDriver <- JDBC(driverClass="org.apache.ignite.IgniteJdbcThinDriver", classPath="/opt/ignite-core-2.7.0.jar")
  jdbcConnection <- dbConnect(jdbcDriver, paste("jdbc:ignite:thin://10.34.121.188"), "ignite", "ignite")
  workflow_dataframe <- dbGetQuery(jdbcConnection, sql_query)
}

read_hdfs <- function (filename) 
{
  library(sparklyr)
  
  sparkconf <- spark_config()
  sparkconf$`sparklyr.shell.driver-java-options` <- sprintf("-Djava.io.tmpdir=%s", "tmp")
  sparkconf$`sparklyr.shell.driver-memory` <- "4G"
  sparkconf$`sparklyr.shell.executor-memory` <- "30G"
  
  sc <- spark_connect(master = "local[*]",config = sparkconf)
  
  #read data from hdfs
  hdfs <- spark_read_csv(sc, name="df",delimiter =",",
                       path="hdfs://10.34.121.184:9000/",filename)
  print(hdfs)
}

write_hdfs <- function (data, filename) 
{
  library(sparklyr)
  sparkconf <- spark_config()
  sparkconf$`sparklyr.shell.driver-java-options` <- sprintf("-Djava.io.tmpdir=%s", 
                                                            "tmp")
  sparkconf$`sparklyr.shell.driver-memory` <- "4G"
  sparkconf$`sparklyr.shell.executor-memory` <- "30G"
  sc <- spark_connect(master = "local[*]", config = sparkconf)
  url <- "hdfs://10.34.121.184:9000/"
  filepath = paste(url,filename,sep="")
  spark_write_csv(data, path = filepath, header = TRUE, delimiter = ",")
  print("File successfully saved to hdfs")
}

