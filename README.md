# ther_inside_airbnb
*Makes http://insideairbnb.com/get-the-data.html queryable by building PostgreSQL database(s) on-the-fly. Scrapes data using regex and piped bash curl commands directly from postgres to minimize redundant data transfer and improve query performance.
*Provides library for cleaning and preprocessing scraped data (i.e., dealing w/ missing values, coding of numerical/categorical variables, etc.). All functions documented w/ examples using Roxygen2.
*Performs exploratory data analysis (i.e., variable distributions, correlations, etc.), temporal and spectral clustering of listing price dynamics using tadpole (pam + dtw), and predictive modeling of listing prices and occupancy using xgboost. 
*See R/ther_inside_airbnb.nb.html for use examples. 
