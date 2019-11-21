library(RCurl)

#scrape data file URLs from inside airbnb
mainURL <- getURL("http://insideairbnb.com/get-the-data.html")
datListingsURLs <- regmatches(mainURL, gregexpr('http[^"]*listings.csv.gz', mainURL))[[1]]
visListingsURLs <- regmatches(mainURL, gregexpr('http[^"]*visualisations/listings.csv', mainURL))[[1]]
calendarURLs <- regmatches(mainURL, gregexpr('http[^"]*calendar.csv.gz', mainURL))[[1]]
datReviewURLs <- regmatches(mainURL, gregexpr('http[^"]*reviews.csv.gz', mainURL))[[1]]
visReviewURLs <- regmatches(mainURL, gregexpr('http[^"]*visualisations/reviews.csv', mainURL))[[1]]
hoodURLs <- regmatches(mainURL, gregexpr('http[^"]*neighbourhoods.csv', mainURL))[[1]]
hoodGeoURLs <- regmatches(mainURL, gregexpr('http[^"]*neighbourhoods.geojson', mainURL))[[1]]

#parse URLs to dataframes
tmpSplit <- strsplit(datListingsURLs,"/")
tmpMat <- matrix(unlist(tmpSplit),nrow = length(tmpSplit),byrow = TRUE)
urlDF <- as.data.frame(tmpMat[,4:7])
colnames(urlDF) <- c('country','state','city','date')


