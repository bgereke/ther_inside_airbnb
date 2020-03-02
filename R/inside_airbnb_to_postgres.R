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
                  "neighbourhood_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.csv', main_url))[[1]],
                  "neighbourhood_geo_url" = regmatches(main_url, gregexpr('http[^"]*neighbourhoods.geojson', main_url))[[1]]
                  )
  table$url_date <- as.Date(table$url_date)
  return(table)
}

create_table <- function(con, table_name){
  drop_table <- paste0("DROP TABLE IF EXISTS ", table_name) 
  dbExecute(con, drop_table)
  if (table_name == "calendar"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      listing_id INT, 
      date TEXT, 
      available TEXT, 
      price MONEY, 
      adjusted_price MONEY, 
      minimum_nights INT, 
      maximum_nights INT)"
    )
  } else if (table_name == "detailed_listings"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      id INT, 
      listing_url TEXT, 
      scrape_id BIGINT, 
      last_scraped TEXT,
      name TEXT,
      summary TEXT,
      space TEXT,
      description TEXT,
      experiences_offered TEXT,
      neighbourhood_overview TEXT,
      notes TEXT,
      transit TEXT,
      access TEXT,
      interaction TEXT,
      house_rules TEXT,
      thumbnail_url TEXT,
      medium_url TEXT,
      picture_url TEXT,
      xl_picture_url TEXT,
      host_id INT,
      host_url TEXT,
      host_name TEXT,
      host_since TEXT,
      host_location TEXT,
      host_about TEXT,
      host_response_time TEXT,
      host_response_rate VARCHAR(4),
      host_acceptance_rate VARCHAR(4),
      host_is_superhost TEXT,
      host_thumbnail_url TEXT,
      host_picture_url TEXT,
      host_neighbourhood TEXT,
      host_listings_count INT,
      host_total_listings_count INT,
      host_verifications TEXT,
      host_has_profile_pic TEXT,
      host_identity_verified TEXT,
      street TEXT,
      neighbourhood TEXT,
      neighbourhood_cleansed TEXT,
      neighbourhood_group_cleansed TEXT,
      city TEXT,
      state TEXT,
      zipcode TEXT,
      market TEXT,
      smart_location TEXT,
      country_code CHAR(2),
      country TEXT,
      latitute NUMERIC,
      longitude NUMERIC,
      is_location_exact TEXT,
      property_type TEXT,
      room_type TEXT,
      accomodates NUMERIC,
      bathrooms NUMERIC,
      bedrooms NUMERIC,
      beds NUMERIC,
      bed_type TEXT,
      amenities TEXT,
      square_feet INT,
      price MONEY,
      weekly_price MONEY,
      monthly_price MONEY,
      security_deposit MONEY,
      cleaning_fee MONEY,
      guests_included INT,
      extra_people MONEY,
      minimum_nights INT,
      maximum_nights INT,
      minimum_minimum_nights INT,
      maximum_minimum_nights INT,
      minimum_maximum_nights INT,
      maximum_maximum_nights INT,
      minimum_nights_avg_ntm NUMERIC,
      maximum_nights_avg_ntm NUMERIC,
      calendar_updated TEXT,
      has_availability TEXT,
      availability_30 INT,
      availability_60 INT,
      availability_90 INT,
      availability_365 INT,
      calendar_last_scraped TEXT,
      number_of_reviews INT,
      number_of_reviews_ltm INT,
      first_review TEXT,
      last_review TEXT,
      review_scores_ratings INT,
      review_scores_accuracy INT,
      review_scores_cleanliness INT,
      review_scores_checkin INT,
      review_scores_communication INT,
      review_scores_location INT,
      review_scores_value INT,
      requires_license TEXT,
      license TEXT,
      jurisdiction_names TEXT,
      instant_bookable TEXT,
      is_business_travel_ready TEXT,
      cancellation_policy TEXT,
      require_guest_profile_picture TEXT,
      require_guest_phone_verification TEXT,
      calculated_host_listings INT,
      calculated_host_listings_count_entire_home INT,
      calculated_host_listings_count_private_rooms INT,
      calculated_host_listings_count_shared_rooms INT,
      reviews_per_month NUMERIC)
      "
    )
  } else if (table_name == "summary_listings"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      id INT, 
      name TEXT, 
      host_id INT, 
      host_name TEXT, 
      neighborhood_group TEXT, 
      neighbourhood TEXT, 
      latitutde NUMERIC,
      longitude NUMERIC,
      room_type TEXT,
      price INT,
      minimum_nights INT,
      number_of_reviews INT,
      last_review TEXT,
      reviews_per_month NUMERIC,
      calculated_host_listings_count INT,
      availability_365 INT)"
    )
  } else if (table_name == "detailed_review"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      listing_id INT, 
      id INT, 
      date TEXT, 
      reviewer_id INT, 
      reviewer_name TEXT, 
      comments TEXT)"
    )
  } else if (table_name == "summary_review"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      listing_id INT, 
      date TEXT)"
    )
  } else if (table_name == "neighbourhood"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      neighborhood_group TEXT,
      neighborhood TEXT)"
    )
  } else if (table_name == "neighbourhood_geo"){
    create_table <- paste0(
      "CREATE TABLE ", 
      table_name, 
      " (
      listing_id INT, 
      date TEXT, 
      available TEXT, 
      price TEXT, 
      adjusted_price TEXT, 
      minimum_nights INT, 
      maximum_nights INT)"
    )
  }
  dbExecute(con, create_table)
  return(TRUE)
}

url_to_pgdb <- function(con, city, table_name, url_df){
  url <- url_df[url_df$url_city == city, paste0(table_name, "_url")]
  if (grepl(".gz", url)){
    copy_table <- paste0("COPY ", table_name, 
                         " FROM PROGRAM 'curl \"", 
                         url,
                         "\" | gzip -dac' HEADER CSV DELIMITER ','"
    )
  } else {
    copy_table <- paste0("COPY ", table_name, 
                         " FROM PROGRAM 'curl \"", 
                         url,
                         "\"' HEADER CSV DELIMITER ','"
    )
  }
  num_rows_affected <- dbExecute(con, copy_table)
  return(num_rows_affected)
}

build_pgdb <- function(con, cities, table_names){
  file_urls <-
    scrape_inside_airbnb() %>%
    filter(url_city %in% cities) %>%
    select(url_country, url_state, url_city, url_date, unlist(paste0(table_names, "_url"))) %>%
    group_by(url_city) %>%
    filter(url_date == max(url_date))
  url_cols <- grep("_url", names(file_urls))
  indices <- file_urls[-url_cols]
  dbExecute(con, "SET CLIENT_ENCODING TO 'utf8'")
  tables_created <- 
    map(table_names, create_table, con = con)
  num_rows_affected <- 
    cross2(cities, table_names) %>% 
    purrr::transpose() %>%
    pmap(url_to_pgdb, con = con, url_df = file_urls)
  return(num_rows_affected)
}


library(DBI)
con <- DBI::dbConnect(odbc::odbc(), Driver = "PostgreSQL ODBC Driver(ANSI)", 
                      Server = "localhost", Database = "inside_airbnb", UID = rstudioapi::askForPassword("Database user"), 
                      PWD = rstudioapi::askForPassword("Database password"), Port = 5432)
file_urls <- scrape_inside_airbnb()
cities <- list("portland", "san-francisco")
table_names <- list("summary_listings", "calendar", "detailed_review", "detailed_listings", "summary_review", "neighbourhood")
build_pgdb(con, cities = cities, table_names = table_names)

calendar <- read.csv("C:/Users/Brian/Downloads/calendar/calendar.csv")
spark_read_jdbc(sc,
                name = "actor_jdbc",
                options = list(url = "jdbc:postgresql://localhost:5432/dvdrental", 
                                             user = rstudioapi::askForPassword("Database user"), 
                                             password = rstudioapi::askForPassword("Database password"), 
                                             dbtable = "actor"))