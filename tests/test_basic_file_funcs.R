## FIXME: Needs proper test data. This is just a framework...
test.file.funcs <- function() {
  source("../R/ncdf.R")
  input.files <- c("/home/data/climate/CMIP5/incoming/CMIP5_current/CCCMA/CanESM2/rcp45/day/atmos/day/r1i1p1/v20120410/pr/pr_day_CanESM2_rcp45_r1i1p1_20060101-23001231.nc")
  variable.name.map <- c(tmax="tasmax", tmin="tasmin", prec="pr")
  f <- lapply(input.files, ncdf4::nc_open)
  f.meta <- create.file.metadata(f, variable.name.map)
  

  
  lapply(f, ncdf4::nc_close)
}
