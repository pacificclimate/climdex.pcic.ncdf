\name{get.northern.hemisphere.booleans}
\alias{get.northern.hemisphere.booleans}
\title{Determine what portions of a subset are within the northern hemisphere.}
\usage{
get.northern.hemisphere.booleans(subset, f, v, projection)
}
\arguments{
  \item{subset}{The subset to use.}

  \item{f}{The NetCDF file to use; an object of class
  \code{ncdf4}.}

  \item{v}{The variable in question.}

  \item{projection}{The proj4 string to use; NULL if the
  data is not in a projected coordinate space.}
}
\value{
An array of booleans corresponding to the subset containing
TRUE if the point is within the northern hemisphere, and
FALSE otherwise.
}
\description{
Determine what portions of a subset are within the northern
hemisphere.
}
\details{
Given a subset, a file, a variable, and a projection,
determine what positions are within the northern
hemisphere, returning the result as an array of booleans.
}
\examples{
## FIXME
}

