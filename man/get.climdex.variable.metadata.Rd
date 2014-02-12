\name{get.climdex.variable.metadata}
\alias{get.climdex.variable.metadata}
\title{Returns metadata for specified Climdex variables}
\usage{
get.climdex.variable.metadata(vars.list, template.filename)
}
\arguments{
  \item{vars.list}{The list of variables, as returned by
  \code{\link{get.climdex.variable.list}}.}

  \item{template.filename}{The filename template to be used
  when generating filenames.}
}
\value{
A data frame containing the following: \itemize{
\item{long.name}{Long names for the variable}
\item{var.name}{Variable name for use in the file}
\item{units}{Units for the variable} \item{annual}{Whether
the variable is annual} \item{base.period.attr}{Whether to
include a base period attribute}
\item{standard.name}{Standard name to use for the variable}
\item{filename}{Filename to be written out} }
}
\description{
Returns metadata for specified Climdex variables.
}
\details{
This function returns metadata suitable for use in NetCDF
files for the specified variables.
}
\examples{
## FIXME
}

