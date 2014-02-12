\name{get.climdex.variable.list}
\alias{get.climdex.variable.list}
\title{Returns a list of Climdex variables given constraints}
\usage{
get.climdex.variable.list(source.data.present, time.resolution = c("all",
  "annual", "monthly"), climdex.vars.subset = NULL)
}
\arguments{
  \item{source.data.present}{A vector of strings naming the
  data that's present; at least one of (tmin, tmax, prec,
  tavg).}

  \item{time.resolution}{The time resolutions to compute
  indices at. See \code{\link{create.indices.from.files}}.}

  \item{climdex.vars.subset}{A character vector of
  lower-case names of Climdex indices to calculate (eg: tr,
  fd, rx5day). See
  \code{\link{create.indices.from.files}}.}
}
\value{
A character vector containing variable names with time
resolutions appended.
}
\description{
Returns a list of Climdex variables given constraints.
}
\details{
This function takes a character vector which specifies what
source data is present and a time resolution, and generates
a list of names consisting of the variable and the time
resolution, separated by an underscore.
}
\examples{
## FIXME
}
\seealso{
\code{\link{create.indices.from.files}}
}

