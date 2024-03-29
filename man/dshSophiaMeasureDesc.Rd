% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dshSophiaMeasureDesc.R
\name{dshSophiaMeasureDesc}
\alias{dshSophiaMeasureDesc}
\title{Get descriptive information about a variable in the measurement table}
\usage{
dshSophiaMeasureDesc(
  dataframe = "baseline",
  variable,
  keep_procedure = NA,
  remove_procedure = NA,
  keep_observation = NA,
  remove_observation = NA,
  keep_gender = NA,
  remove_gender = NA
)
}
\arguments{
\item{dataframe}{A character, the name of the data frame holding the data. Defaults to \code{baseline}.}

\item{variable}{A character, must correspond to a column present in the data frame.}

\item{keep_procedure}{A numeric, must be a valid Concept ID for a \code{has_} column created using \code{dshSophiaCreateBaseline}. Will only keep rows with this procedure. Defaults to \code{NA}.}

\item{remove_procedure}{A numeric, must be a valid Concept ID for a \code{has_} column created using \code{dshSophiaCreateBaseline}. Will remove all rows with this procedure. Defaults to \code{NA}.}

\item{keep_observation}{A numeric, must be a valid Concept ID for a \code{has_} column created using \code{dshSophiaCreateBaseline}. Will only keep rows with this observation. Defaults to \code{NA}.}

\item{remove_observation}{A numeric, must be a valid Concept ID for a \code{has_} column created using \code{dshSophiaCreateBaseline}. Will remove all rows with this observation. Defaults to \code{NA}.}

\item{keep_gender}{A character, either \code{MALE} or \code{FEMALE}. If supplied, only IDs matching the specified gender will be kept. Defaults to \code{NA}.}

\item{remove_gender}{A character, either \code{MALE} or \code{FEMALE}. If supplied, all IDs matching the specified gender will be removed. Defaults to \code{NA}.}
}
\value{
A data frame with descriptive information.
}
\description{
Given a valid Concept ID the function gathers descriptive information about the corresponding variable in the measurement table and outputs a summary of the results. If the data is longitudinal the output will contain one row per time point.
}
\examples{
\dontrun{
# connect to the federated system
dshSophiaConnect(include = "abos")

# load database resources
dshSophiaLoad()

# get descriptive information about BMI at timepoint 1, only in males with T2D
dshSophiaMeasureDesc(variable = "t1_3038553", keep_observation = 201826, keep_gender = "MALE")
}
}
