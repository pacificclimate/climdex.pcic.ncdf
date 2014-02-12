\name{compute.climdex.indices}
\alias{compute.climdex.indices}
\title{Compute Climdex indices using provided data.}
\usage{
compute.climdex.indices(in.dat, cdx.funcs, ts, base.range, fclimdex.compatible)
}
\arguments{
  \item{in.dat}{The input data to compute indices on.}

  \item{cdx.funcs}{The functions to be applied to the data,
  as created by \code{\link{get.climdex.functions}}.}

  \item{ts}{The associated time data, as created by
  \code{nc.get.time.series}.}

  \item{base.range}{The base range; a vector of two numeric
  years.}

  \item{fclimdex.compatible}{Whether to make the results
  identical to those of fclimdex; this affects how the data
  in the base period is padded.}
}
\value{
A list of data for each index.
}
\description{
Compute Climdex indices using provided data.
}
\details{
Given the provided data and functions, compute the Climdex
indices defined by the functions.
}
\examples{
## FIXME
}

