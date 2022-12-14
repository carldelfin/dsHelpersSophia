% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dshSophiaPrompt.R
\name{dshSophiaPrompt}
\alias{dshSophiaPrompt}
\title{Prompt for log in details}
\usage{
dshSophiaPrompt()
}
\value{
An Opals object (\code{opals}) is assigned to the Global environment.
}
\description{
Prompts the user for login details (if those are not available via \code{Sys.getenv()}) and then connects via \code{dshSophiaConnect}. The user is also given the option to supply a single character or a list of characters separated by a single space denoting the nodes to either include or exclude. The function is primarily a fallback used within \code{dshSophiaLoad} when the user has not logged in to the federated system.
}
