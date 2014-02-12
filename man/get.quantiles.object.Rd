\name{get.quantiles.object}
\alias{get.quantiles.object}
\title{Extract a single quantiles object from a set of thresholds.}
\usage{
get.quantiles.object(thresholds, idx)
}
\arguments{
  \item{thresholds}{The thresholds, as extracted by
  \code{\link{get.thresholds.chunk}}.}

  \item{idx}{The index to extract.}
}
\value{
A quantiles object suitable for passing to
\code{climdexInput.raw} as the \code{quantiles} argument.
}
\description{
Extract a single quantiles object from a set of thresholds.
}
\details{
From a set of thresholds as retrieved from one or more
NetCDF files containing thresholds, this function extracts
a single point and converts the format to one suitable for
passing to \code{climdexInput.raw}.
}
\examples{
## FIXME
}

