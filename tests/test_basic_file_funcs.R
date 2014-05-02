author.data <- list(institution="Pacific Climate Impacts Consortium",
                    institution_id="PCIC",
                    indices_archive="Please check http://www.cccma.ec.gc.ca/data/climdex/climdex.shtml for errata or updates.",
                    contact="bronaugh@uvic.ca",
                    references="http://www.cccma.ec.gc.ca/data/climdex/"
                    )
x.subset <- 33:34
y.subset <- 17:18
correct.data.dir <- "correct_output/"
correct.thresh.file.6190 <- paste(correct.data.dir, "thresholds_monClim_CanESM2_historical_r1i1p1_1961-1990.nc", sep="")
thresh.omit.list <- c("tx10p", "tn10p", "tx10p", "tx90p", "wsdi", "csdi")

## FIXME: Needs proper test data. This is just a framework...
test.file.funcs <- function() {
  test.dir <- "test3/"
  if(file.exists(test.dir)) {
    input.files <- list.files(test.dir, full.names=TRUE)
    variable.name.map <- c(tmax="tasmax", tmin="tasmin", prec="pr")
    f <- lapply(input.files, ncdf4::nc_open)
    f.meta <- create.file.metadata(f, variable.name.map)
    lapply(f, ncdf4::nc_close)
  }
}

test.thresholds.create.and.indices <- function() {
  test.set <- paste("test", 1:6, "/", sep="")
  lapply(test.set[file.exists(test.set)], function(test) {
    input.file.list <- list.files(test, full.names=TRUE)
    print(file.exists(input.file.list))
    print(input.file.list)
    thresh.file <- tempfile()
    indices.dir.thresh <- tempdir()
    indices.dir.nothresh <- tempdir()
    create.thresholds.from.file(input.file.list, thresh.file, author.data, parallel=FALSE, base.range=c(2010, 2019))
    create.indices.from.files(input.file.list, indices.dir.thresh, input.file.list[1], author.data, parallel=FALSE, thresholds.files=correct.thresh.file.6190)

    ## Compare to base data.
    test.file.list <- list.files(indices.dir.thresh, pattern="ETCCDI")
    lapply(test.file.list, function(fn) {
      print(fn)
      f.test <- nc_open(paste(indices.dir.thresh, fn, sep="/"))
      f.correct <- nc_open(paste(correct.data.dir, fn, sep="/"))

      d.test <- ncvar_get(f.test, ncdf4.helpers::nc.get.variable.list(f.test)[1])
      d.correct <- ncvar_get(f.correct, ncdf4.helpers::nc.get.variable.list(f.correct)[1])

      checkEquals(d.test, d.correct)
      
      nc_close(f.test)
      nc_close(f.correct)
    })
    
    create.indices.from.files(input.file.list, indices.dir.thresh, input.file.list[1], author.data, parallel=FALSE, thresholds.files=thresh.file)
    create.indices.from.files(input.file.list, indices.dir.nothresh, input.file.list[1], author.data, parallel=FALSE, base.range=c(2010, 2019))

    unlink(paste(indices.dir.nothresh, "*", sep="/"))
    unlink(paste(indices.dir.thresh, "*", sep="/"))
    gc()
  })
}

parallel.thresholds.create.and.indices <- function() {
  test.set <- paste("test", 1:6, "/", sep="")
  lapply(test.set[file.exists(test.set)], function(test) {
    input.file.list <- list.files(test, full.names=TRUE)
    print(file.exists(input.file.list))
    print(input.file.list)
    thresh.file <- tempfile()
    indices.dir.thresh <- tempdir()
    indices.dir.nothresh <- tempdir()
    create.thresholds.from.file(input.file.list, thresh.file, author.data, parallel=4, base.range=c(2010, 2029))
    create.indices.from.files(input.file.list, indices.dir.thresh, input.file.list[1], author.data, parallel=4, thresholds.files=thresh.file)
    create.indices.from.files(input.file.list, indices.dir.nothresh, input.file.list[1], author.data, parallel=4, base.range=c(2010, 2029))
  })
}
