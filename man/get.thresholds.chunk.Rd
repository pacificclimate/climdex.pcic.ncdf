\name{get.thresholds.chunk}
\alias{get.thresholds.chunk}
\title{Retrieve thresholds for a subset}
\usage{
get.thresholds.chunk(subset, cdx.funcs, thresholds.netcdf, t.f.idx,
  thresholds.name.map)
}
\arguments{
  \item{subset}{The subset to use.}

  \item{cdx.funcs}{The functions to be applied to the data,
  as created by \code{\link{get.climdex.functions}}.}

  \item{thresholds.netcdf}{One or more NetCDF files
  containing thresholds.}

  \item{t.f.idx}{A mapping from threshold variables to
  threshold files, as created by
  \code{\link{get.var.file.idx}}.}

  \item{thresholds.name.map}{A mapping from standardized
  names (tx10thresh, tn90thresh, etc) to NetCDF variable
  names.}
}
\description{
Retrieve thresholds for a subset
}
\details{
Given a subset, a set of Climdex functions (as created by
\code{\link{get.climdex.functions}}), and ancillary data,
load the thresholds required for the functions being called
and return them.
}
\examples{
## FIXME
}

