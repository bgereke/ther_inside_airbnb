suppressMessages(library(tidyverse, quietly = TRUE, warn.conflicts = FALSE))

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

convert_dates <- function(df, format = date_format){
  is_date <- sapply(df, function(x) !all(is.na(as.Date(as.character(x),format=format))))
  df[,is_date] <- map(df[, is_date], as.Date)
  return(df)
}

factorize_booleans <- function(df, bools = bool_chars){
  is_boolean <- sapply(df, function(x) sum(x %in% bools,na.rm = TRUE)) / colSums(!is.na(df)) == 1
  boolean_cols <- names(which(is_boolean))
  df[, boolean_cols] <- map(df[,boolean_cols], as_factor)
  return(df)
}

factorize_city <- function(df, city_names, state_names, col_to_keep = city_keep, cols_to_drop = city_drop){
  for (c in 1:length(city_names)){
    has_city <-  grepl(city_names[c], df[, col_to_keep]) 
    has_state <-  grepl(state_names[c], df[, col_to_keep]) 
    df[has_city | has_state, col_to_keep] <- city_names[c]
  }
  df[, col_to_keep] <- as.factor(df[, col_to_keep])
  df <- df %>% select(-c(cols_to_drop))
  return(df)
}

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

factorize_simple_cols <- function(df, factor_cols = simple_factor_list){
  df[, factor_cols] <- map(df[, factor_cols], as.factor)
  return(df)
}

factorize_license <- function(df){
  df$host_has_license <- as.factor(!is.na(df$license))
  df$license <- NULL
  return(df)
}

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

host_rates_to_numeric <- function(df, rate_cols = rate_list){
  df[, rate_cols] <- sapply(df[, rate_cols], function(x) sub("N/A",NA,x))
  df[, rate_cols] <- sapply(df[, rate_cols], function(x) as.numeric(sub("%","",x)))
  return(df)
}

text_to_numeric <- function(df, cols = text_cols){
  df[, cols] <- map(df[, cols], str_length)
  return(df)
}

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
      df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- cut(df[!is.na(df[, high_na_cols[c]]), high_na_cols[c]], 
                                                                breaks = c(8, 9, 10))
      df[is.na(df[, high_na_cols[c]]), high_na_cols[c]] <- "empty" 
      df[, high_na_cols[c]] <- factor(df[, high_na_cols[c]], 
                                      levels = as.character(unique(df[, high_na_cols[c]])))
    }
  }
  return(df)
}


#default variables
junk_cols <- c('scrape_id', 'host_id',
               'host_name', 'host_neighbourhood', 
               'host_listings_count', 'calendar_last_scraped',
               'calendar_updated', 'neighbourhood_group_cleansed', 
               'country_code', 'reviews_per_month',
               'number_of_reviews_ltm')
pop_cols <- c('latitute', 'longitude', 'zipcode')
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