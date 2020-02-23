library(tidyverse)
library(RCurl)

scrape_inside_airbnb <- function(){
  main_url <- getURL("http://insideairbnb.com/get-the-data.html")
  detailed_listings_urls <- regmatches(main_url, gregexpr('http[^"]*listings.csv.gz', main_url))[[1]]
  tmpSplit <- strsplit(detailed_listings_urls,"/")
  table <- tibble(
                  "country" = map_chr(tmpSplit,4),
                  "state" = map_chr(tmpSplit,5),
                  "city" = map_chr(tmpSplit,6),
                  "date" = map_chr(tmpSplit,7),
                  "detailed_listings_url" = detailed_listings_urls,
                  "summary_listings_url" = regmatches(main_url, gregexpr('http[^"]*visualisations/listings.csv', main_url))[[1]],
                  "calendar_url" = regmatches(main_url, gregexpr('http[^"]*calendar.csv.gz', main_url))[[1]],
                  "detailed_review_url" = regmatches(main_url, gregexpr('http[^"]*reviews.csv.gz', main_url))[[1]],
                  "summary_review_url" = regmatches(main_url, gregexpr('http[^"]*visualisations/reviews.csv', main_url))[[1]],
                  "neighborhood_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.csv', main_url))[[1]],
                  "neighborhood_geo_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.geojson', main_url))[[1]]
                  )
  table$date <- as.Date(table$date)
  return(table)
}



write_tables(url_list){
  #read tables from the given url extensions and write them to disk   
}

file_url <- scrape_inside_airbnb()

library(data.table)
data <- fread("http://data.insideairbnb.com/united-states/or/portland/2019-10-16/data/listings.csv.gz")

calendar <- read.csv("C:/Users/Brian/Downloads/calendar/calendar.csv")

spark_read_jdbc(sc,
                name = "actor_jdbc",
                options = list(url = "jdbc:postgresql://localhost:5432/dvdrental", 
                                             user = rstudioapi::askForPassword("Database user"), 
                                             password = rstudioapi::askForPassword("Database password"), 
                                             dbtable = "actor"))