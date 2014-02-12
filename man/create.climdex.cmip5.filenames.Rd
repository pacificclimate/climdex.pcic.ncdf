\name{create.climdex.cmip5.filenames}
\alias{create.climdex.cmip5.filenames}
\title{Creates a list of CMIP5-compliant filenames reflecting the input data.}
\usage{
create.climdex.cmip5.filenames(fn.split, vars.list)
}
\arguments{
  \item{fn.split}{A vector containing named components, as
  created by \code{get.split.filename.cmip5}.}

  \item{vars.list}{A vector containing names of variables,
  as created by \code{\link{get.climdex.variable.list}}.}
}
\value{
A vector containing filenames corresponding to the
variables and filename bits supplied.
}
\description{
Creates a list of CMIP5-compliant filenames reflecting the
input data.
}
\details{
This function takes a split filename (as created by
\code{get.split.filename.cmip5}) and a list of variables
and creates corresponding filenames for the given
variables.
}
\examples{
## FIXME
}

