% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dshSophiaCreateBaseline.R
\name{dshSophiaCreateBaseline}
\alias{dshSophiaCreateBaseline}
\title{Create a baseline data frame on the federated node(s)}
\usage{
dshSophiaCreateBaseline(
  name = name,
  procedure_id = NULL,
  observation_id = NULL,
  age_at_first = NULL,
  age_at_year = NULL
)
}
\arguments{
\item{name}{A character, the name of the resulting data frame. Defaults to \code{baseline}.}

\item{procedure_id}{A numeric, must be a valid Concept ID in the Procedure table, that is coded as either present or not present. If supplied, will create a factor column named after the Concept ID, prefixed with \code{has_}. A value of 1 will mean that the procedure is present, and a value of 0 will mean that the procedure is not present or is missing. Defaults to \code{NULL}.}

\item{observation_id}{A numeric, must be a valid Concept ID in the Observation table, that is coded as either present or not present. If supplied, will create a factor column named after the Concept ID, prefixed with \code{has_}. A value of 1 will mean that the observation is present, and a value of 0 will mean that the observation is not present or is missing. Defaults to \code{NULL}.}

\item{age_at_first}{Either a character \code{visit} or a valid Concept ID from the Measurement table. If \code{visit} then age at first visit is calculated. If a Concept ID, then the age at first available measurement of that Concept ID is calculated. Cannot be used at the same time as \code{age_at_year}. Defaults to \code{NULL}.}

\item{age_at_year}{A numeric, corresponding to a year (e.g. \code{2000}). The age at that specific year is calculated. Cannot be used at the same time as \code{age_at_first}. Defaults to \code{NULL}.}
}
\value{
A federated data frame named 'baseline'.
}
\description{
Creates a 'baseline' data frame on the federated node(s). Here, 'baseline' simply means baseline characteristics such as gender, age, and various procedures and observations. The 'baseline' data frame can be used as a stepping stone for creating more elaborate data frames.
}
\examples{
\dontrun{
# connect to the federated system
dshSophiaConnect(include = "abos")

# load database resources
dshSophiaLoad()

# create a 'baseline' data frame on the federated node
dshSophiaCreateBaseline(procedure_id = c(3038553, 3025315, 37020574),
observation_id = 201826, 
age_at_first = "visit")

# check result
dsBaseClient::ds.summary(name)
}
}
