author.data <- list(institution="Pacific Climate Impacts Consortium",
                    institution_id="PCIC",
                    indices_archive="Please check http://www.cccma.ec.gc.ca/data/climdex/climdex.shtml for errata or updates.",
                    contact="bronaugh@uvic.ca",
                    references="http://www.cccma.ec.gc.ca/data/climdex/"
                    )

## FIXME: Needs proper test data. This is just a framework...
test.file.funcs <- function() {
  source("../R/ncdf.R")
  input.files <- c("/home/data/climate/CMIP5/incoming/CMIP5_current/CCCMA/CanESM2/rcp45/day/atmos/day/r1i1p1/v20120410/pr/pr_day_CanESM2_rcp45_r1i1p1_20060101-23001231.nc")
  variable.name.map <- c(tmax="tasmax", tmin="tasmin", prec="pr")
  f <- lapply(input.files, ncdf4::nc_open)
  f.meta <- create.file.metadata(f, variable.name.map)
  lapply(f, ncdf4::nc_close)
}

test.thresholds.create <- function() {
  test.set <- paste("test", 1:6, "/", sep="")
  lapply(test.set, function(test) {
    input.file.list <- list.files(test, full.names=TRUE)
    thresh.file <- tempfile()
    indices.dir.thresh <- tempdir()
    indices.dir.nothresh <- tempdir()
    create.thresholds.from.file(input.file.list, thresh.file, author.data, parallel=FALSE, base.range=c(2010, 2029))
    create.indices.from.files(input.file.list, indices.dir.thresh, input.file.list[1], author.data, parallel=FALSE, thresholds.files=thresh.file)
    create.indices.from.files(input.file.list, indices.dir.nothresh, input.file.list[1], author.data, parallel=FALSE, base.range=c(2010, 2029))
  })
}
