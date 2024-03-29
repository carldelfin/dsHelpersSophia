% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dshSophiaExit.R
\name{dshSophiaExit}
\alias{dshSophiaExit}
\title{Log out from the SOPHIA federated database}
\usage{
dshSophiaExit()
}
\value{
Nothing, the user is logged out.
}
\description{
Wrapper around \code{DSI::datashield.logout} that logs the user out from the federated system.
}
\examples{
\dontrun{
# connect to the federated system
dshSophiaConnect()

# load database resources
dshSophiaLoad()

# -- do some work --

# log out
dshSophiaExit()
}
}
