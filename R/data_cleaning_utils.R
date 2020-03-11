suppressMessages(library(tidyverse, quietly = TRUE, warn.conflicts = FALSE))

#' Remove junk columns from a data frame.
#'
#' Drops columns with mostly missing data or only a
#' single distinct value. Allows user to provide a 
#' list of additional columns to drop.
#'
#' @param df A data frame.
#' @param cols A list of column names to be dropped 
#' from the data frame.
#'
#' @return A data frame with junk columns dropped.
#'
#' @examples
#' junk_df <- data.frame(a = rep('a', times = 10),
#'                       b = 1:10,
#'                       c = c(rep(NA, times = 6),
#'                             1:4),
#'                       d = 3:13)
#' clean_df <- drop_junk(junk_df, cols = c('d'))
#' head(clean_df)
drop_junk <- function(df, cols = junk_cols){
  cols_to_drop <- c(
    colnames(df)[colSums(is.na(df))/nrow(df) > 0.5],
    colnames(df)[summarize_all(df, n_distinct) == 1],
    colnames(df)[grep("_url", colnames(df))],
    cols
  )
  df <- df %>% select(-cols_to_drop)
  return(df)
}

#' Detect date columns and convert them to Date variables.
#'
#' @param df A data frame with some columns that might 
#' contain dates. 
#' @param format A date format to detect (e.g., '\%Y-\%m-\%d') 
#'
#' @return A data frame with converted date columns
#'
#' @examples
#' char_df <- data.frame(date_col = rep('2020-01-15', times = 30))
#' date_df <- convert_dates(char_df, format = '%Y-%m-%d')
#' is.Date(date_df$date_col)
convert_dates <- function(df, format = date_format){
  is_date <- sapply(df, function(x) !all(is.na(as.Date(as.character(x),format=format))))
  df[,is_date] <- map(df[, is_date], as.Date)
  df[,is_date] <- sapply(df[, is_date], function(x) as.numeric(Sys.Date() - x))
  return(df)
}

#' Detect and convert boolean variables to factors.
#'
#' @param df A data frame with some columns containing only 
#' two distinct characters.
#' @param bools A list of two characters to detect.  
#'
#' @return A data frame with the boolean columns converted 
#' to factors.
#'
#' @examples
#' char_df <- data.frame(bool_col = rep(c('t','f'), times = 5))
#' factor_df <- convert_dates(char_df, c('t', 'f'))
#' is.factor(date_df$bool_col)
factorize_booleans <- function(df, bools = bool_chars){
  is_boolean <- sapply(df, function(x) sum(x %in% bools,na.rm = TRUE)) / colSums(!is.na(df)) == 1
  boolean_cols <- names(which(is_boolean))
  df[, boolean_cols] <- map(df[,boolean_cols], as_factor)
  return(df)
}

#' Clean columns with poorly coded city names and convert to factors.
#'
#' @param df A data frame of the 'detailed_listings' type with redundant 
#' columns for cities, or with too multiple distinct codes for the same 
#' city.
#' @param city_names A list of city names to detect. 
#' @param state_names A list of state names to detect.
#' @param col_to_keep Name of city column to keep.
#' @param cols_to_drop Names of city columns to drop.
#' @param relate_host_location If TRUE, adds a new column specifying 
#' if host lives in the same city as her/his listing. 
#'
#' @return A data frame with a single factor column for city. The levels
#' of the factor are determined by \code{\link{city_names}}. 
#'
#' @examples
#' dirty_df <- data.frame(city = c("Portland", "Portland, Or", "OR", "San Fran, CA"),
#'                       location = c("Or, Port", "Portland", "port", "ca"),
#'                       host_location = c("Portland", "NYC", "Mars", "717 Potter St."))
#' city_codes <- c("Portland", "San Francisco")
#' state_codes <- c("OR", "CA")
#' clean_df <- factorize_city(dirty_df, 
#'                            city_names = city_codes, 
#'                            state_names = state_codes, 
#'                            col_to_keep = 'city',
#'                            cols_to_drop = c('location'),
#'                            relate_host_location = TRUE)
#' head(clean_df)
factorize_city <- function(df, city_names, state_names, col_to_keep = city_keep, cols_to_drop = city_drop, relate_host_location = TRUE){
  if (relate_host_location){
    df$host_same_city <- as.character(df$host_location)
  }
  for (c in 1:length(city_names)){
    has_city <-  grepl(city_names[c], df[, col_to_keep]) 
    has_state <-  grepl(state_names[c], df[, col_to_keep]) 
    df[has_city | has_state, col_to_keep] <- city_names[c]
    if (relate_host_location){
      host_in_city <- grepl(city_names[c], df$host_location)
      host_in_state <- grepl(state_names[c], df$host_location)
      df$host_location[host_in_city | host_in_state] <- city_names[c]
    }
    
  }
  if (relate_host_location){
    df$host_same_city <- df$host_location == df[, col_to_keep]
    df$host_location <- as.factor(df$host_location)
    df$host_location <- as.factor(df$host_location)
  }
  df[, col_to_keep] <- as.factor(df[, col_to_keep])
  df <- df %>% select(-c(cols_to_drop))
  return(df)
}

#' Add factor columns indicating listing amenities.
#'
#' @param df A data frame like 'detailed_listings' with an amenities 
#' column where each record contains a list of amenities the listing has.
#' @param amenities A list of amenities to detect and convert to factors.
#' @param prop_filter The minimum poportion of either TRUE or FALSE 
#' required for an amenity to be included.
#'
#' @return A data frame with indicator columns for each amenity.
#'
#' @examples
#' dirty_df <- data.frame(amenities = c({tv,lamp},{tv},{lamp}))
#' am_list <- c('tv', 'lamp')
#' clean_df <- factorize_amenities(dirty_df, amenities = am_list)
factorize_amenities <- function(df, amenities = amenities_list, prop_filter = 0.1){
  for (a in 1:length(amenities)){
    tmp <- factor(grepl(amenities[a], df$amenities))
    prop_true <- sum(tmp == TRUE) / length(tmp)
    if (prop_true > prop_filter & prop_true < (1 - prop_filter)){
      df$tmp <- tmp
      names(df)[names(df) == "tmp"] <- paste0("listing_has_",amenities[a])
    }
  }
  df <- df %>% 
    select(
      -c(
        'amenities',
        colnames(df)[summarize_all(df, n_distinct) == 1]
        )
      )
  return(df)
}

#' Add factor columns indicating host_verifications.
#'
#' @param df A data frame like 'detailed_listings' with a host verifications 
#' column where each record contains a list of verifications the host has.
#' @param verifications A list of verifications to detect and convert to factors.
#' @param prop_filter The minimum poportion of either TRUE or FALSE 
#' required for a verification to be included.
#'
#' @return A data frame with indicator columns for each verification.
#' @export
#'
#' @examples
#' dirty_df <- data.frame(verifications = c({ID,picture},{ID},{picture}))
#' ver_list <- c('ID', 'picture')
#' clean_df <- factorize_verifications(dirty_df, verifications = ver)
factorize_host_verifications <- function(df, verifications = verifications_list, prop_filter = 0.1){
  for (v in 1:length(verifications)){
    tmp <- factor(grepl(verifications[v], df$host_verifications))
    prop_true <- sum(tmp == TRUE) / length(tmp)
    if (prop_true > prop_filter & prop_true < (1 - prop_filter)){
      df$tmp <- tmp
      names(df)[names(df) == "tmp"] <- paste0("host_has_",verifications[v])
    }
  }
  df <- df %>% 
    select(
      -c(
        'host_verifications',
        colnames(df)[summarize_all(df, n_distinct) == 1]
      )
    )
  return(df)
}

#' Convert columns with simple categorical variables to factors.
#'
#' @param df A data frame with some categorical columns that need to be factors.
#' @param factor_cols List of column names to covert.
#'
#' @return A data frame with the elected columns converted to factors.
#'
#' @examples
#' char_df <- data.frame(fac_col = rep(c("level_one","level_two", "level_three"), 
#'                                     each = 5))
#' fac_df <- factorize_simple_cols(char_df, factor_cols = c('fac_col'))
#' is.factor(fac_df$fac_col)
factorize_simple_cols <- function(df, factor_cols = simple_factor_list){
  df[, factor_cols] <- map(df[, factor_cols], as.factor)
  return(df)
}

#' Add a column to indicate if a host is licensed.
#'
#' @param df A data frame with a column of license types where NA
#' denotes the absense of a license.
#'
#' @return A data frame with a 'host_has_license' column where 
#' not NA is TRUE and NA is FALSE.
#'
#' @examples
#' lic_df <- data.frame(license = c('sfgert34',NA,'ST$%'))
#' fac_df <- factorize_license(lic_df)
#' is.factor(fac_df$host_has_license)
factorize_license <- function(df){
  df$host_has_license <- as.factor(!is.na(df$license))
  df$license <- NULL
  return(df)
}

#' Convert cancellation policy to 'flexible', 'moderate' or 'strict' and factorize.
#'
#' @param df A data frame with a 'cancellation_policy' column where entries have
#' different codes that all contain aither 'flexible', 'moderate', or 'strict'.
#'
#' @return A data frame with the 'cancellation_policy' column recoded and converted 
#' to factor.
#'
#' @examples
#' can_df <- data.frame(cancellation_policy = c('moderate','kinda strict', 'mostly flexible'))
#' fac_df <- factorize_cancellation_policy(can_df)
#' is.factor(fac_df$cancellation_policy)
#' levels(fac_df$cancellation_policy)
factorize_cancellation_policy <- function(df){
  to_flexible <- grepl("flexible",df$cancellation_policy)
  to_moderate <- grepl("moderate",df$cancellation_policy)
  to_strict <- grepl("strict",df$cancellation_policy)
  df$cancellation_policy[to_flexible] <- "flexible"
  df$cancellation_policy[to_moderate] <- "moderate"
  df$cancellation_policy[to_strict] <- "strict"
  df$cancellation_policy <- as.factor(df$cancellation_policy)
  return(df)
}

#' Convert host percentage rates columns to numeric.
#'
#' @param df A data frame with character columns containing percentage values.
#' @param rate_cols A list of column names of the percentage columns. 
#'
#' @return A data frame with the percent symbols reoved and converted to numeric.
#'
#' @examples
#' perc_df <- data.frame(rates <- c('10%', '2%', '100%'))
#' num_df <- host_rates_to_numeric(perc_df, rate_cols = c('rates'))
#' is.numeric(num_df$rates)
host_rates_to_numeric <- function(df, rate_cols = rate_list){
  df[, rate_cols] <- sapply(df[, rate_cols], function(x) sub("N/A",NA,x))
  df[, rate_cols] <- sapply(df[, rate_cols], function(x) as.numeric(sub("%","",x)))
  return(df)
}

#' Convert columns with text entries to numeric columns given by the length of the text.
#'
#' @param df A data frame with some columns where each record is a text description.
#' @param cols A list of column names specifying the text columns.
#'
#' @return A data frame with text columns converted to numeric.
#'
#' @examples
#' text_df = data.frame(text_col = c('short description', 'not very long description'))
#' num_df <- text_to_numeric(text_df, cols = c('text_col'))
#' is.numeric(num_df$text_col)
text_to_numeric <- function(df, cols = text_cols){
  df[, cols] <- map(df[, cols], str_length)
  return(df)
}

#' Process columns with missing values depending on column name.
#'
#' @param df A data frame like 'detailed_listings' that has columns with
#' missing values that need to be dealt with in specialized ways.
#' Supported column names are: 'host_response_rate', host_acceptance_rate',
#' 'first_review', 'last_review', and 'review_scores_*'.  
#' @param proportion_to_drop If a column has less than this proportion
#' of missing records, then missings records will be deleted.
#'
#' @return A data frame with processed missing values.
#'
#' @examples
#' To-do
process_missing <- function(df, proportion_to_drop = 0.05){
  #delete rows if feature has small number of missing values
  prop_missing <- colSums(is.na(df)) / nrow(df)
  low_na_cols <- names(df)[prop_missing < proportion_to_drop]
  df <- df[complete.cases(df[, low_na_cols]),]
  high_na_cols <- names(df)[prop_missing >= proportion_to_drop]
  for (c in 1:length(high_na_cols)){
    if (high_na_cols[c] %in% c("security_deposit", "cleaning_fee")){
      df[is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- 0
    } else if (high_na_cols[c] %in% c("host_response_rate", "host_acceptance_rate")){
      df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- cut(df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]], 
                                                                breaks = c(0, 50, 90, 99, 100))
      df[is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- "empty" 
      df[, high_na_cols[c]] <- factor(df[, high_na_cols[c]], 
                                      levels = as.character(unique(df[, high_na_cols[c]])))
    } else if(high_na_cols[c] %in% c("first_review", "last_review")){
      first_review <- min(df[, high_na_cols[c]], na.rm = TRUE)
      last_review <- max(df[, high_na_cols[c]], na.rm = TRUE)
      df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- cut(df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]], 
                                                                breaks = seq(first_review, last_review, length.out = 5))
      df[, high_na_cols[c]] <- as.character(df[, high_na_cols[c]])
      df[is.na( df[, high_na_cols[c]]), high_na_cols[c]] <- "empty"
      df[, high_na_cols[c]] <- factor(df[, high_na_cols[c]], 
                                      levels = unique(df[, high_na_cols[c]]))
    } else if(grepl("review_scores_",high_na_cols[c])){
      if (100 %in% df[, high_na_cols[c]]){
        df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- cut(df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]],
                                                                  breaks = c(70, 80, 90, 100))
      } else {
        df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- cut(df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]],
                                                                  breaks = c(7, 8, 9, 10))
      }
      
      df[is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- "empty" 
      df[, high_na_cols[c]] <- factor(df[, high_na_cols[c]], 
                                      levels = as.character(unique(df[, high_na_cols[c]])))
    }
  }
  return(df)
}

#' Refactor property types to either 'house', 'apartment', or 'other'.
#'
#' @param df A data frame with a 'property_type' column where many 
#' entries contain either 'house' or 'part'.
#'
#' @return A data frame where entries in the 'property_type' column 
#' containing 'house' are converted to 'house', entries containing
#' 'part' are converted to 'apartment', and all others are converted 
#' to 'other' and factorized.
#'
#' @examples
#' prop_df <- data.frame(property_type = c('big house', 'small house', 'Apart. build', 'igloo'))
#' fac_df <- refactor_property_types(prop_df)
#' is.factor(fac_df$property_type)
#' levels(fac_df$property_type)
refactor_property_types <- function(df){
  df$property_type <- as.character(df$property_type)
  is_house <- grepl("house", df$property_type)
  is_apt <- grepl("part", df$property_type)
  df$property_type[is_house] <- 'house'
  df$property_type[is_apt] <- 'apartment'
  df$property_type[!is_house & !is_apt] <- 'other'
  df$property_type <- as.factor(df$property_type)
  return(df)
} 

#' Refactor bed types to either 'Real Bed' or 'Fake Bed'
#'
#' @param df A data frame with a 'bed_type' column where many of the
#' entries are already 'Real Bed', but a few are random types of 
#' other beds.
#'
#' @return A data frame where the 'Real Bed' entries stay the same, 
#' and all other are converted to 'Fake Bed' and factorized.
#'
#' @examples 
#' bed_df <- data.frame(bed_type = c('Real Bed', 'hammock', 'couch'))
#' fac_df <- refactor_bed_type(bed_df)
#' is.factor(fac_df$bed_type)
#' levels(fac_df$bed_type)
refactor_bed_type <- function(df){
  df$bed_type <- as.character(df$bed_type)
  is_bed <- df$bed_type == "Real Bed"
  df$bed_type[!is_bed] <- "Fake Bed"
  df$bed_type <- as.factor(df$bed_type)
  return(df)
}


#default variables
junk_cols <- c('scrape_id', 'host_id',
               'host_name', 'host_neighbourhood', 
               'host_listings_count', 'calendar_last_scraped',
               'calendar_updated', 'neighbourhood_group_cleansed', 
               'country_code', 'reviews_per_month',
               'number_of_reviews_ltm', 'last_scraped')
# pop_cols <- c('latitute', 'longitude', 'zipcode')
rate_list <- c("host_response_rate", "host_acceptance_rate")
text_cols <- c("name", "summary", "space", "description", "neighbourhood_overview", "notes", "transit", "access", "interaction", "house_rules", "host_about")
date_format <- "%Y-%m-%d"
bool_chars <- c("t", "f")
simple_factor_list <- c("host_response_time", "property_type", "room_type", "bed_type")
city_keep <- "smart_location"
city_drop <- c('street', 'neighbourhood', 'city', 'market', 'jurisdiction_names', 'state')
verifications_list = c('email', 'phone', 'reviews', 'jumio', 'government_id','kba', 'work_email','facebook','identity_manual','offline_government_id','selfie','google')
amenities_list = c(
  '24-hour check-in',
  'Accessible-height bed',
  'Accessible-height toilet',
  'Air conditioning',
  'Air purifier',
  'Alfresco bathtub',
  'Amazon Echo',
  'Apple TV',
  'BBQ grill',
  'Baby bath',
  'Baby monitor',
  'Babysitter recommendations',
  'Balcony',
  'Bath towel',
  'Bathroom essentials',
  'Bathtub',
  'Bathtub with bath chair',
  'Beach essentials',
  'Beach view',
  'Beachfront',
  'Bed linens',
  'Bedroom comforts',
  'Bidet',
  'Body soap',
  'Breakfast',
  'Breakfast bar',
  'Breakfast df',
  'Building staff',
  'Buzzer/wireless intercom',
  'Cable TV',
  'Carbon monoxide detector',
  'Cat(s)',
  'Ceiling fan',
  'Ceiling hoist',
  'Central air conditioning',
  'Changing df',
  "Chef's kitchen",
  'Children’s books and toys',
  'Children’s dinnerware',
  'Cleaning before checkout',
  'Coffee maker',
  'Convection oven',
  'Cooking basics',
  'Crib',
  'DVD player',
  'Day bed',
  'Dining area',
  'Disabled parking spot',
  'Dishes and silverware',
  'Dishwasher',
  'Dog(s)',
  'Doorman',
  'Double oven',
  'Dryer',
  'EV charger',
  'Electric profiling bed',
  'Elevator',
  'En suite bathroom',
  'Espresso machine',
  'Essentials',
  'Ethernet connection',
  'Exercise equipment',
  'Extra pillows and blankets',
  'Family/kid friendly',
  'Fax machine',
  'Fire extinguisher',
  'Fire pit',
  'Fireplace guards',
  'Firm mattress',
  'First aid kit',
  'Fixed grab bars for shower',
  'Fixed grab bars for toilet',
  'Flat path to front door',
  'Formal dining area',
  'Free parking on premises',
  'Free street parking',
  'Full kitchen',
  'Game console',
  'Garden or backyard',
  'Gas oven',
  'Ground floor access',
  'Gym',
  'HBO GO',
  'Hair dryer',
  'Hammock',
  'Handheld shower head',
  'Hangers',
  'Heat lamps',
  'Heated floors',
  'Heated towel rack',
  'Heating',
  'High chair',
  'High-resolution computer monitor',
  'Host greets you',
  'Hot tub',
  'Hot water',
  'Hot water kettle',
  'Indoor fireplace',
  'Internet',
  'Iron',
  'Ironing Board',
  'Jetted tub',
  'Keypad',
  'Kitchen',
  'Kitchenette',
  'Lake access',
  'Laptop friendly workspace',
  'Lock on bedroom door',
  'Lockbox',
  'Long term stays allowed',
  'Luggage dropoff allowed',
  'Memory foam mattress',
  'Microwave',
  'Mini fridge',
  'Mobile hoist',
  'Mountain view',
  'Mudroom',
  'Murphy bed',
  'Netflix',
  'Office',
  'Other',
  'Other pet(s)',
  'Outdoor kitchen',
  'Outdoor parking',
  'Outdoor seating',
  'Outlet covers',
  'Oven',
  'Pack ’n Play/travel crib',
  'Paid parking off premises',
  'Paid parking on premises',
  'Patio or balcony',
  'Pets allowed',
  'Pets live on this property',
  'Pillow-top mattress',
  'Pocket wifi',
  'Pool',
  'Pool cover',
  'Pool with pool hoist',
  'Printer',
  'Private bathroom',
  'Private entrance',
  'Private gym',
  'Private hot tub',
  'Private living room',
  'Private pool',
  'Projector and screen',
  'Propane barbeque',
  'Rain shower',
  'Refrigerator',
  'Roll-in shower',
  'Room-darkening shades',
  'Safe',
  'Safety card',
  'Sauna',
  'Security system',
  'Self check-in',
  'Shampoo',
  'Shared gym',
  'Shared hot tub',
  'Shared pool',
  'Shower chair',
  'Single level home',
  'Ski-in/Ski-out',
  'Smart TV',
  'Smart lock',
  'Smoke detector',
  'Smoking allowed',
  'Soaking tub',
  'Sound system',
  'Stair gates',
  'Stand alone steam shower',
  'Standing valet',
  'Steam oven',
  'Step-free access',
  'Stove',
  'Suidf for events',
  'Sun loungers',
  'TV',
  'df corner guards',
  'Tennis court',
  'Terrace',
  'Toilet paper',
  'Touchless faucets',
  'Walk-in shower',
  'Warming drawer',
  'Washer',
  'Washer / Dryer',
  'Waterfront',
  'Well-lit path to entrance',
  'Wheelchair accessible',
  'Wide clearance to bed',
  'Wide clearance to shower',
  'Wide doorway',
  'Wide entryway',
  'Wide hallway clearance',
  'Wifi',
  'Window guards',
  'Wine cooler',
  'toilet'
)