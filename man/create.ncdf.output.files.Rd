\name{create.ncdf.output.files}
\alias{create.ncdf.output.files}
\title{Creates output files for Climdex variables.}
\usage{
create.ncdf.output.files(cdx.dat, f, v.f.idx, variable.name.map, ts,
  time.origin, base.range, out.dir, author.data)
}
\arguments{
  \item{cdx.dat}{The variable description data, as created
  by \code{\link{get.climdex.variable.metadata}}.}

  \item{f}{The file(s) being used as input.}

  \item{v.f.idx}{A mapping from variables to files, as
  created by \code{\link{get.var.file.idx}}.}

  \item{variable.name.map}{A mapping from standardized
  names (tmax, tmin, prec) to NetCDF variable names.}

  \item{ts}{The associated time data, as created by
  \code{nc.get.time.series}.}

  \item{time.origin}{The time origin, as specified in the
  source NetCDF file(s).}

  \item{base.range}{The base range; a vector of two numeric
  years.}

  \item{out.dir}{The output directory name.}

  \item{author.data}{A vector containing named elements
  describing the author; see
  \code{\link{create.indices.from.files}}.}
}
\value{
A list of objects of type \code{ncdf4}.
}
\description{
Creates output files for Climdex variables.
}
\details{
This function creates a set of output files for the set of
variable parameters passed in \code{cdx.dat}, as created by
\code{\link{get.climdex.variable.metadata}}. It copies
metadata from input files as appropriate and adds new
metadata as required.
}
\examples{
## FIXME
}

