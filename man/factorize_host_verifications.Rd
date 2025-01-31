% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data_cleaning_utils.R
\name{factorize_host_verifications}
\alias{factorize_host_verifications}
\title{Add factor columns indicating host_verifications.}
\usage{
factorize_host_verifications(
  df,
  verifications = verifications_list,
  prop_filter = 0.1
)
}
\arguments{
\item{df}{A data frame like 'detailed_listings' with a host verifications 
column where each record contains a list of verifications the host has.}

\item{verifications}{A list of verifications to detect and convert to factors.}

\item{prop_filter}{The minimum poportion of either TRUE or FALSE 
required for a verification to be included.}
}
\value{
A data frame with indicator columns for each verification.
}
\description{
Add factor columns indicating host_verifications.
}
\examples{
dirty_df <- data.frame(verifications = c({ID,picture},{ID},{picture}))
ver_list <- c('ID', 'picture')
clean_df <- factorize_verifications(dirty_df, verifications = ver)
}
