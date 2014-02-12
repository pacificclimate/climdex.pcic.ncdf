\name{get.quantiles.for.stripe}
\alias{get.quantiles.for.stripe}
\title{Compute Climdex thresholds for a subset / stripe}
\usage{
get.quantiles.for.stripe(subset, ts, base.range, dim.axes, v.f.idx,
  variable.name.map, src.units, dest.units,
  pad.data.with.first.last.values = FALSE)
}
\arguments{
  \item{subset}{The subset to use.}

  \item{ts}{The associated time data, as created by
  \code{nc.get.time.series}.}

  \item{base.range}{The base range; a vector of two numeric
  years.}

  \item{dim.axes}{The dimension axes for the input data.}

  \item{v.f.idx}{A mapping from variables to files, as
  created by \code{\link{get.var.file.idx}}.}

  \item{variable.name.map}{A mapping from standardized
  names (tmax, tmin, prec) to NetCDF variable names.}

  \item{src.units}{The source units to convert data from.}

  \item{dest.units}{The destination units to convert to.}

  \item{pad.data.with.first.last.values}{Whether to make
  the results identical to those of fclimdex; this affects
  how the data in the base period is padded.}
}
\description{
Compute Climdex thresholds for a subset / stripe
}
\details{
Given a subset and ancillary data, load and convert data,
get the out-of-base quantiles for the data for each point,
and return the result.
}
\note{
This function relies on an object named 'f' and containing
the opened NetCDF files being part of the global namespace.
}
\examples{
## FIXME
}

