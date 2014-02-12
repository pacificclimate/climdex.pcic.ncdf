\name{flatten.dims}
\alias{flatten.dims}
\title{Flatten the X and Y dimensions down to a space dimension.}
\usage{
flatten.dims(dat, reduce.dims, names.subset)
}
\arguments{
  \item{dat}{The data to operate on.}

  \item{reduce.dims}{The names or indices of the dimensions
  to reduce to 1 dimension.}

  \item{names.subset}{Optionally, a subset of dimension
  names to copy.}
}
\value{
The data with the specified dimensions reduced to 1
dimension.
}
\description{
Flatten the X and Y dimensions down to a space dimension.
}
\details{
This function takes input data, a vector of dimensions to
reduce to 1 dimension, and optionally a subset of dimnames
to copy. It returns the data with the specified dimensions
shrunk down to 1 dimension.
}
\note{
The dimensions to reduce must be adjoining dimensions.
}
\examples{
## FIXME
}

