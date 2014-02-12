\name{compute.indices.for.stripe}
\alias{compute.indices.for.stripe}
\title{Compute Climdex indices for a subset / stripe}
\usage{
compute.indices.for.stripe(subset, cdx.funcs, ts, base.range, dim.axes, v.f.idx,
  variable.name.map, src.units, dest.units, t.f.idx, thresholds.name.map,
  fclimdex.compatible = TRUE, projection = NULL)
}
\arguments{
  \item{subset}{The subset to use.}

  \item{cdx.funcs}{The functions to be applied to the data,
  as created by \code{\link{get.climdex.functions}}.}

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

  \item{t.f.idx}{A mapping from threshold variables to
  threshold files, as created by
  \code{\link{get.var.file.idx}}.}

  \item{thresholds.name.map}{A mapping from standardized
  names (tx10thresh, tn90thresh, etc) to NetCDF variable
  names.}

  \item{fclimdex.compatible}{Whether to make the results
  identical to those of fclimdex; this affects how the data
  in the base period is padded.}

  \item{projection}{A proj4 string representing the
  projection the data is in.}
}
\description{
Compute Climdex indices for a subset / stripe
}
\details{
Given a subset, a set of Climdex functions (as created by
\code{\link{get.climdex.functions}}), and ancillary data,
load and convert data, create a climdexInput object for
each point, run all of the functions in \code{cdx.funcs} on
that data, and return the result.
}
\note{
This function relies on an object named 'f' and containing
the opened NetCDF files being part of the global namespace.
}
\examples{
## FIXME
}

