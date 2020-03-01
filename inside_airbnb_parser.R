library(tidyverse)
library(RCurl)
library(DBI)
library(data.table)

scrape_inside_airbnb <- function(){
  main_url <- getURL("http://insideairbnb.com/get-the-data.html")
  detailed_listings_urls <- regmatches(main_url, gregexpr('http[^"]*listings.csv.gz', main_url))[[1]]
  tmpSplit <- strsplit(detailed_listings_urls,"/")
  table <- tibble(
                  "url_country" = map_chr(tmpSplit,4),
                  "url_state" = map_chr(tmpSplit,5),
                  "url_city" = map_chr(tmpSplit,6),
                  "url_date" = map_chr(tmpSplit,7),
                  "detailed_listings_url" = detailed_listings_urls,
                  "summary_listings_url" = regmatches(main_url, gregexpr('http[^"]*visualisations/listings.csv', main_url))[[1]],
                  "calendar_url" = regmatches(main_url, gregexpr('http[^"]*calendar.csv.gz', main_url))[[1]],
                  "detailed_review_url" = regmatches(main_url, gregexpr('http[^"]*reviews.csv.gz', main_url))[[1]],
                  "summary_review_url" = regmatches(main_url, gregexpr('http[^"]*visualisations/reviews.csv', main_url))[[1]],
                  "neighborhood_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.csv', main_url))[[1]],
                  "neighborhood_geo_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.geojson', main_url))[[1]]
                  )
  table$url_date <- as.Date(table$url_date)
  return(table)
}

url_to_pgdb <- function(con, table_name, url, add_indices = NULL){
  copy_table <- paste0("COPY ", table_name, 
                  " FROM PROGRAM 'curl \"", 
                  url,
                  "\" | gzip -dac' HEADER CSV DELIMITER ','"
                  )
  dbExecute(con, copy_table)
  # table <- fread(as.character(url), encoding = 'UTF-8')
  # if (!is.null(add_indices)){
  #   table[,names(add_indices)] <- add_indices[names(add_indices)]
  # }
  # if (!dbExistsTable(con, table_name)){
  #   dbCreateTable(con, table_name, table)
  #   dbAppendTable(con, table_name, table)
  # } else {
  #   dbAppendTable(con, table_name, table)
  #   # dbWriteTable(con, "tmp_table", table)
  #   # #this is unsafe, should use dbBind() or sqlInterpolate() but they were failing
  #   # query <- paste0("INSERT INTO ", table_name, " SELECT * FROM tmp_table EXCEPT SELECT * FROM ", table_name)
  #   # dbExecute(con, query)
  #   # dbRemoveTable(con, "tmp_table")
  # }
}

build_pgdb <- function(con, table_names){
  file_urls <- scrape_inside_airbnb()
  url_cols <- grep("_url", names(file_urls))
  indices <- file_urls[-url_cols]
  for (i in 1:length(table_names)){
    drop_table <- paste0("DROP TABLE IF EXISTS ", table_names[[i]]) 
    dbExecute(con, drop_table)
    create_table <- paste0("CREATE TABLE ", 
                           table_names[[i]], 
                           " (
                           listing_id INT, 
                           date DATE, 
                           available BOOLEAN, 
                           price TEXT, 
                           adjusted_price TEXT, 
                           minimum_nights INT, 
                           maximum_nights INT)"
                           )
    dbExecute(con, create_table)
    for (j in 1:nrow(file_urls)){
      try(
        url_to_pgdb(con, 
                    table_name = table_names[[i]],
                    url = file_urls[j, paste0(table_names[[i]],"_url")],
                    add_indices = NULL) #indices[j,])
        )
      print(paste0("copied ", j, " of ", nrow(file_urls)))
    } 
  }
}


library(DBI)
con <- DBI::dbConnect(odbc::odbc(), Driver = "PostgreSQL ODBC Driver(ANSI)", 
                      Server = "localhost", Database = "inside_airbnb", UID = rstudioapi::askForPassword("Database user"), 
                      PWD = rstudioapi::askForPassword("Database password"), Port = 5432)
file_urls <- scrape_inside_airbnb()



data <- fread("http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2019-12-07/data/calendar.csv.gz")
calendar <- read.csv("C:/Users/Brian/Downloads/calendar/calendar.csv")
spark_read_jdbc(sc,
                name = "actor_jdbc",
                options = list(url = "jdbc:postgresql://localhost:5432/dvdrental", 
                                             user = rstudioapi::askForPassword("Database user"), 
                                             password = rstudioapi::askForPassword("Database password"), 
                                             dbtable = "actor"))