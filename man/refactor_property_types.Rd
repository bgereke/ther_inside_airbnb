% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data_cleaning_utils.R
\name{refactor_property_types}
\alias{refactor_property_types}
\title{Refactor property types to either 'house', 'apartment', or 'other'.}
\usage{
refactor_property_types(df)
}
\arguments{
\item{df}{A data frame with a 'property_type' column where many 
entries contain either 'house' or 'part'.}
}
\value{
A data frame where entries in the 'property_type' column 
containing 'house' are converted to 'house', entries containing
'part' are converted to 'apartment', and all others are converted 
to 'other' and factorized.
}
\description{
Refactor property types to either 'house', 'apartment', or 'other'.
}
\examples{
prop_df <- data.frame(property_type = c('big house', 'small house', 'Apart. build', 'igloo'))
fac_df <- refactor_property_types(prop_df)
is.factor(fac_df$property_type)
levels(fac_df$property_type)
}
