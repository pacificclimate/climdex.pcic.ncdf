library(ncdf4)
library(ncdf4.helpers)
library(climdex.pcic)
library(snow)
library(udunits2)

## Parallel lapply across 'x', running remote.func, and filtering with local.filter.func .
## Processing is incremental, not batch, to improve parallel throughput and reduce memory consumption.
parLapplyLBFiltered <- function(cl, x, remote.func, ..., local.filter.func=NULL) {
  checkCluster(cl)
  cluster.size <- length(cl)
  num.tasks <- length(x)
  if(num.tasks == 0)
    return(list())
  if(cluster.size == 0)
    stop("Impossible happened; cluster size = 0")

  data.to.return <- vector("list", num.tasks)

  submit.job <- function(cluster.id, task.id) {
    sendCall(cl[[cluster.id]], remote.func, args=c(x[task.id], list(...)), tag=task.id)
  }
  
  ## Fire off jobs, filling in the cur.task table as we go.
  for(i in 1:min(cluster.size, num.tasks))
    submit.job(i, i)

  next.task <- min(cluster.size, num.tasks)
  
  ## Stalk and feed jobs
  for(i in 1:num.tasks) {
    d <- recvOneResult(cl)
    next.task <- next.task + 1
    
    ## Feed the finished node another task if we have one.
    if(next.task <= num.tasks)
      submit.job(d$node, next.task)
    
    if(!is.null(local.filter.func))
      data.to.return[d$tag] <- list(local.filter.func(d$value, x[[d$tag]]))
    else
      data.to.return[d$tag] <- list(d$value)
      
    rm(d)
  }

  ## Return data when complete
  return(data.to.return)
}

put.history.att <- function(f, v, definemode=FALSE) {
  history.string <- paste("Created by climdex.pcic", packageVersion("climdex.pcic"), "on", date())
  ncatt_put(f, v, "history", history.string, definemode=definemode)
}

put.ETCCDI.atts <- function(f, freq, orig.title, definemode=FALSE) {
  ## FIXME
  ncatt_put(f, 0, "ETCCDI_institution", "Pacific Climate Impacts Consortium", definemode=definemode)
  ncatt_put(f, 0, "ETCCDI_institution_id", "PCIC", definemode=definemode)
  ncatt_put(f, 0, "ETCCDI_indices_archive", "Please check http://www.cccma.ec.gc.ca/data/climdex/climdex.shtml for errata or updates.", definemode=definemode)

  ncatt_put(f, 0, "ETCCDI_software", "climdex.pcic", definemode=definemode)
  ncatt_put(f, 0, "ETCCDI_software_version", as.character(packageVersion("climdex.pcic")), definemode=definemode)

  ## FIXME
  ncatt_put(f, 0, "contact", "bronaugh@uvic.ca", definemode=definemode)
  ncatt_put(f, 0, "references", "http://www.cccma.ec.gc.ca/data/climdex/", definemode=definemode)

  ncatt_put(f, 0, "frequency", freq, definemode=definemode)
  ncatt_put(f, 0, "creation_date", format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz="GMT"), definemode=definemode)
  ncatt_put(f, 0, "title", paste("ETCCDI indices computed on", orig.title), definemode=definemode)
}

get.last.monthday.of.year <- function(d, sep="-") {
  if(!is.null(attr(d, "months"))) paste("12", attr(d, "months")[12], sep=sep) else paste("12", "31", sep)
}

all.the.same <- function(dat) {
  ifelse(length(dat) == 1, TRUE, all(unlist(lapply(dat, identical, dat[[1]]))))
}

curry.args <- function(f.name, defined.args, undefined.args, env=.GlobalEnv) {
  defined.args <- lapply(defined.args, function(x) if(is.character(x)) paste("\"", x, "\"", sep="") else x)
  udef.args.str <- paste(undefined.args, sep=", ")
  func.str <- paste("function(", udef.args.str, ") { ", f.name, "(", udef.args.str, paste(rep(", ", length(defined.args)), paste(names(defined.args), defined.args, sep="="), collapse=""), ") }", sep="")
  eval(parse(text=func.str), envir=env)
}

get.split.filename <- function(cmip5.file) {
  split.path <- strsplit(cmip5.file, "/")[[1]]
  fn.split <- strsplit(tail(split.path, n=1), "_")[[1]]
  names(fn.split) <- c("var", "tres", "model", "emissions", "run", "trange", rep(NA, max(0, length(fn.split) - 6)))
  fn.split[length(fn.split)] <- strsplit(fn.split[length(fn.split)], "\\.")[[1]][1]
  fn.split[c('tstart', 'tend')] <- strsplit(fn.split['trange'], "-")[[1]]
  fn.split
}

create.postfix <- function(fn.split, time.res, time.range=NULL, emissions=NULL) {
  postfixes <- switch(time.res, yr=c("", ""), mon=c("01", "12"), day=c("0101", "1231"), monClim=c("", ""))
  if(is.null(time.range))
    time.range <- substr(fn.split[c('tstart', 'tend')], 1, 4)

  if(is.null(emissions))
    emissions <- fn.split['emissions']
  
  paste("_", paste(c(time.res, fn.split['model'], emissions, fn.split['run'], paste(time.range, postfixes, sep="", collapse="-"), fn.split[is.na(names(fn.split))]), collapse="_"), ".nc", sep="")
}

get.climdex.info.struct <- function(subset.vars=NULL, fclimdex.compatible=TRUE, out.dir, out.postfix) {
  long.names <- c("Number of Frost Days", "Number of Summer Days", "Number of Icing Days", "Number of Tropical Nights", "Growing Season Length",
                  "Monthly Maximum of Daily Maximum Temperature", "Monthly Maximum of Daily Minimum Temperature",
                  "Monthly Minimum of Daily Maximum Temperature", "Monthly Minimum of Daily Minimum Temperature",
                  "Percentage of Days when Daily Minimum Temperature is Below the 10th Percentile", "Percentage of Days when Daily Maximum Temperature is Below the 10th Percentile",
                  "Percentage of Days when Daily Minimum Temperature is Above the 90th Percentile", "Percentage of Days when Daily Maximum Temperature is Above the 90th Percentile",
                  "Annual Maximum of Daily Maximum Temperature", "Annual Maximum of Daily Minimum Temperature",
                  "Annual Minimum of Daily Maximum Temperature", "Annual Minimum of Daily Minimum Temperature",
                  "Percentage of Days when Daily Minimum Temperature is Below the 10th Percentile", "Percentage of Days when Daily Maximum Temperature is Below the 10th Percentile",
                  "Percentage of Days when Daily Minimum Temperature is Above the 90th Percentile", "Percentage of Days when Daily Maximum Temperature is Above the 90th Percentile",
                  "Warm Spell Duration Index", "Cold Spell Duration Index",
                  "Mean Diurnal Temperature Range", "Monthly Maximum 1-day Precipitation", "Monthly Maximum Consecutive 5-day Precipitation",
                  "Mean Diurnal Temperature Range", "Annual Maximum 1-day Precipitation", "Annual Maximum Consecutive 5-day Precipitation",
                  "Simple Precipitation Intensity Index", "Annual Count of Days with At Least 10mm of Precipitation",
                  "Annual Count of Days with At Least 20mm of Precipitation", "Annual Count of Days with At Least 1mm of Precipitation",
                  "Maximum Number of Consecutive Days with Less Than 1mm of Precipitation", "Maximum Number of Consecutive Days with At Least 1mm of Precipitation",
                  "Annual Total Precipitation when Daily Precipitation Exceeds the 95th Percentile of Wet Day Precipitation",
                  "Annual Total Precipitation when Daily Precipitation Exceeds the 99th Percentile of Wet Day Precipitation", "Annual Total Precipitation in Wet Days",
                  "Maximum Number of Consecutive Days Per Year with Less Than 1mm of Precipitation", "Maximum Number of Consecutive Days Per Year with At Least 1mm of Precipitation",
                  "Warm Spell Duration Index Spanning Years", "Cold Spell Duration Index Spanning Years")
  standard.name.lookup <- c(fdETCCDI="number_frost_days", suETCCDI="number_summer_days", idETCCDI="number_icing_days", trETCCDI="number_tropical_nights", gslETCCDI="growing_season_length",
                            txxETCCDI="maximum_daily_maximum_temperature", tnxETCCDI="maximum_daily_minimum_temperature", txnETCCDI="minimum_daily_maximum_temperature", tnnETCCDI="minimum_daily_minimum_temperature",
                            tn10pETCCDI="percent_days_when_daily_minimum_temperature_below_10p", tx10pETCCDI="percent_days_when_daily_maximum_temperature_below_10p",
                            tn90pETCCDI="percent_days_when_daily_minimum_temperature_above_90p", tx90pETCCDI="percent_days_when_daily_maximum_temperature_above_90p",
                            wsdiETCCDI="warm_spell_duration_index", csdiETCCDI="cold_spell_duration_index", dtrETCCDI="diurnal_temperature_range",
                            rx1dayETCCDI="maximum_1day_precipitation", rx5dayETCCDI="maximum_5day_precipitation", sdiiETCCDI="simple_precipitation_intensity_index",
                            r10mmETCCDI="count_days_more_than_10mm_precipitation", r20mmETCCDI="count_days_more_than_20mm_precipitation", r1mmETCCDI="count_days_more_than_1mm_precipitation",
                            cddETCCDI="maximum_number_consecutive_dry_days", cwdETCCDI="maximum_number_consecutive_wet_days",
                            r95pETCCDI="total_precipitation_exceeding_95th_percentile", r99pETCCDI="total_precipitation_exceeding_99th_percentile", prcptotETCCDI="total_wet_day_precipitation")
  climdex.names <- c("climdex.fd", "climdex.su", "climdex.id", "climdex.tr", "climdex.gsl",
                     "climdex.txx", "climdex.tnx", "climdex.txn", "climdex.tnn", "climdex.tn10p", "climdex.tx10p", "climdex.tn90p", "climdex.tx90p",
                     "climdex.txx", "climdex.tnx", "climdex.txn", "climdex.tnn", "climdex.tn10p", "climdex.tx10p", "climdex.tn90p", "climdex.tx90p",
                     "climdex.wsdi", "climdex.csdi",
                     "climdex.dtr", "climdex.rx1day", "climdex.rx5day",
                     "climdex.dtr", "climdex.rx1day", "climdex.rx5day",
                     "climdex.sdii", "climdex.r10mm", "climdex.r20mm", "climdex.rnnmm", "climdex.cdd", "climdex.cwd", "climdex.r95ptot", "climdex.r99ptot", "climdex.prcptot",
                     "climdex.cdd", "climdex.cwd", "climdex.csdi", "climdex.wsdi")
  name.ncdf <-  c("fdETCCDI", "suETCCDI", "idETCCDI", "trETCCDI", "gslETCCDI",
                  "txxETCCDI", "tnxETCCDI", "txnETCCDI", "tnnETCCDI", "tn10pETCCDI", "tx10pETCCDI", "tn90pETCCDI", "tx90pETCCDI",
                  "txxETCCDI", "tnxETCCDI", "txnETCCDI", "tnnETCCDI", "tn10pETCCDI", "tx10pETCCDI", "tn90pETCCDI", "tx90pETCCDI",
                  "wsdiETCCDI", "csdiETCCDI",
                  "dtrETCCDI", "rx1dayETCCDI", "rx5dayETCCDI",
                  "dtrETCCDI", "rx1dayETCCDI", "rx5dayETCCDI",
                  "sdiiETCCDI", "r10mmETCCDI", "r20mmETCCDI", "r1mmETCCDI", "cddETCCDI", "cwdETCCDI", "r95pETCCDI", "r99pETCCDI", "prcptotETCCDI",
                  "altcddETCCDI", "altcwdETCCDI", "altcsdiETCCDI", "altwsdiETCCDI")
  units <- c("days", "days", "days", "days", "days",
             "degrees_C", "degrees_C", "degrees_C", "degrees_C", "%", "%", "%", "%",
             "degrees_C", "degrees_C", "degrees_C", "degrees_C", "%", "%", "%", "%",
             "days", "days",
             "degrees_C", "mm", "mm",
             "degrees_C", "mm", "mm",
             "mm d-1", "days", "days", "days", "days", "days", "mm", "mm", "mm",
             "days", "days", "days", "days")
  standard.names <- standard.name.lookup[name.ncdf]
  el <- list()
  af <- list(freq="annual")
  mf <- list(freq="monthly")
  cwdd.opts <- list(spells.can.span.years=TRUE)
  altcwdd.opts <- list(spells.can.span.years=FALSE)
  wcsdi.opts <- list(spells.can.span.years=FALSE)
  altwcsdi.opts <- list(spells.can.span.years=TRUE)
  rx5day.opts <- list(center.mean.on.last.day=fclimdex.compatible)
  r1mm.opts <- list(threshold=1)
  base.period.attr <- c(F, F, F, F, F,
                        F, F, F, F, T, T, T, T,
                        F, F, F, F, T, T, T, T,
                        T, T,
                        F, F, F,
                        F, F, F,
                        F, F, F, F, F, F, T, T, F,
                        F, F, T, T)
  annual <- c(T, T, T, T, T,
              F, F, F, F, F, F, F, F,
              T, T, T, T, T, T, T, T,
              T, T,
              F, F, F,
              T, T, T,
              T, T, T, T, T, T, T, T, T,
              T, T, T, T)
  options <- list(el, el, el, el, el,
                  mf, mf, mf, mf, mf, mf, mf, mf,
                  af, af, af, af, af, af, af, af,
                  wcsdi.opts, wcsdi.opts,
                  mf, mf, c(mf, rx5day.opts),
                  af, af, c(af, rx5day.opts),
                  el, el, el, r1mm.opts, cwdd.opts, cwdd.opts, el, el, el,
                  altcwdd.opts, altcwdd.opts, altwcsdi.opts, altwcsdi.opts)
  func <- lapply(1:length(climdex.names), function(n) curry.args(climdex.names[n], options[[n]], "ci"))
  
  ann.chosen <- sapply(options, function(x) { ("freq" %in% names(x) && x$freq == "annual") })

  ## Cobble together new filenames with properly formatted dates, time coding, etc.
  fn.split <- get.split.filename(out.postfix)
  tfreq.map <- c("mon", "yr")
  postfixes <- sapply(tfreq.map[annual + 1], function(tfreq) { create.postfix(fn.split, tfreq) })
  filename <- paste(out.dir, "/", name.ncdf, postfixes, sep="")

  if(is.null(subset.vars)) {
    subset <- 1:length(name.ncdf)
  } else {
    subset <- which(climdex.names %in% subset.vars)
  }
  
  return(list(func=func[subset], name.ncdf=name.ncdf[subset], filename=filename[subset], units=units[subset], annual=annual[subset], long.names=long.names[subset], standard.names=standard.names[subset], base.period.attr=base.period.attr[subset]))
}

make.time.bounds <- function(ts, unit=c("year", "month")) {
  unit <- match.arg(unit)
  multiplier <- switch(unit, year=1, month=12)
  r <- range(ts)
  r.years <- as.numeric(format(r, "%Y"))
  start.date <- as.PCICt(paste(r.years[1], "-01-01", sep=""), attr(ts, "cal"))
  num.years <- r.years[2] - r.years[1] + 1
  padded.dates <- seq(start.date, by=paste("1", unit), length.out=num.years * multiplier + 1)
  padded.length <- length(padded.dates)
  bounds <- c(padded.dates[1:(padded.length - 1)], padded.dates[2:padded.length] - 86400)
  dim(bounds) <- c(padded.length - 1, 2)
  bounds
}

## Create all the new files...
create.ncdf.output.files <- function(cdx.dat, f, v.list, ts, time.origin, base.range) {
  cal <- attr(ts, "cal")

  time.dim.name <- nc.get.dim.for.axis(f$tasmax, v.list$tasmax, "T")$name
  
  ## Create new time dimensions
  last.day.of.year <- get.last.monthday.of.year(ts)
  time.origin.PCICt <- as.PCICt(time.origin, cal=cal)
  time.units <- paste("days since", time.origin)
  annual.bounds <- make.time.bounds(ts, "year")
  monthly.bounds <- make.time.bounds(ts, "month")
  annual.series <- as.PCICt((unclass(annual.bounds[,1]) + unclass(annual.bounds[,2])) / 2, cal=attr(annual.bounds, "cal"), origin="1970-01-01")
  monthly.series <- as.PCICt((unclass(monthly.bounds[,1]) + unclass(monthly.bounds[,2])) / 2, cal=attr(annual.bounds, "cal"), origin="1970-01-01")
  
  ## Make stuff for NetCDF use
  annual.bounds.days <- as.numeric(julian(t(annual.bounds), origin=time.origin.PCICt))
  monthly.bounds.days <- as.numeric(julian(t(monthly.bounds), origin=time.origin.PCICt))
  annual.days <- as.numeric(julian(annual.series, origin=time.origin.PCICt))
  monthly.days <- as.numeric(julian(monthly.series, origin=time.origin.PCICt))

  time.dim.ann <- ncdim_def(time.dim.name, units=time.units, vals=annual.days, unlim=TRUE, longname='')
  time.dim.mon <- ncdim_def(time.dim.name, units=time.units, vals=monthly.days, unlim=TRUE, longname='')

  vars.to.clone.atts.for <- NULL
  vars.to.copy <- NULL
  vars.data <- NULL

  ## Get bounds variables
  old.time.bnds.att <- ncatt_get(f$tasmax, time.dim.name, "bounds")
  time.bnds.name <- if(old.time.bnds.att$hasatt) old.time.bnds.att$value else "time_bnds"
  input.bounds <- nc.get.dim.bounds.var.list(f$tasmax)
  input.bounds <- input.bounds[input.bounds != time.bnds.name]
  input.bounds <- input.bounds[input.bounds %in% names(f$tasmax$var)]
  bnds <- ncdim_def("bnds", "", 1:2, create_dimvar=FALSE)
  if(length(input.bounds) > 0)
    bnds <- f$tasmax$var[[input.bounds[1]]]$dim[[1]]
  
  time.bnds.var.ann <- ncvar_def(time.bnds.name, '', list(bnds, time.dim.ann), longname='', prec="double")
  time.bnds.var.mon <- ncvar_def(time.bnds.name, '', list(bnds, time.dim.mon), longname='', prec="double")
  vars.to.copy <- c(vars.to.copy, input.bounds)
  
  ## Get coordinate variables and grid mapping variables
  coordinate.vars.att <- ncatt_get(f$tasmax, v.list$tasmax, "coordinates")
  grid.mapping.att <- ncatt_get(f$tasmax, v.list$tasmax, "grid_mapping")
  if(coordinate.vars.att$hasatt) {
    coordinate.vars.names <- strsplit(coordinate.vars.att$value, " ")[[1]]
    vars.to.copy <- c(vars.to.copy, coordinate.vars.names)
  }
  if(grid.mapping.att$hasatt)
    vars.to.copy <- c(vars.to.copy, grid.mapping.att$value)

  input.dim.names <- nc.get.dim.names(f$tasmax, v.list$tasmax)
  vars.to.clone.atts.for <- c(vars.to.clone.atts.for, vars.to.copy, input.dim.names)
  vars.ncvars <- lapply(vars.to.copy, function(x) { f$tasmax$var[[x]] })
  vars.data <- lapply(vars.ncvars, function(ncvar) { if(length(ncvar$dim) == 0) NULL else ncvar_get(f$tasmax, ncvar) })
  names(vars.data) <- vars.to.copy
  
  return(lapply(1:length(cdx.dat$name.ncdf), function(x) {
    ann.var <- cdx.dat$annual[x]
    time.dim <- if(ann.var) time.dim.ann else time.dim.mon
    time.bnds <- if(ann.var) time.bnds.var.ann else time.bnds.var.mon
    time.bnds.data <- if(ann.var) annual.bounds.days else monthly.bounds.days
    
    tasmax.name <- v.list$tasmax
    nc.var.list <- c(vars.ncvars, list(time.bnds, ncvar_def(name=cdx.dat$name.ncdf[x], units=cdx.dat$units[x], dim=list(f$tasmax$var[[tasmax.name]]$dim[[1]], f$tasmax$var[[tasmax.name]]$dim[[2]], time.dim), missval=1e20, longname=cdx.dat$long.names[x])))

    new.file <- nc_create(cdx.dat$filename[x], nc.var.list, force_v4=TRUE)

    ## Copy attributes for all variables plus global attributes
    nc_redef(new.file)
    inst.id <- ncatt_get(f$tasmax, 0, "institution_id")$value
    att.rename <- c("frequency"="input_frequency", "creation_date"="input_creation_date", "title"="input_title", "tracking_id"="input_tracking_id")
    att.rename.inst <- c("contact"="contact", "references"="references")
    names(att.rename.inst) <- paste(inst.id, names(att.rename.inst), sep="_")
    att.rename <- c(att.rename, att.rename.inst)
    
    nc.copy.atts(f$tasmax, 0, new.file, 0, definemode=TRUE, rename.mapping=att.rename)
    for(v in vars.to.clone.atts.for) {
      nc.copy.atts(f$tasmax, v, new.file, v, definemode=TRUE)
    }
    ncatt_put(new.file, time.dim.name, "units", time.units, definemode=TRUE)
    
    ## Attach history data to threshold data.
    put.history.att(new.file, cdx.dat$name.ncdf[x], definemode=TRUE)

    #put.ETCCDI.atts <- function(f, freq, orig.title, definemode=FALSE) {
    tfreq.map <- c("mon", "yr")
    put.ETCCDI.atts(new.file, tfreq.map[1 + as.integer(cdx.dat$annual[x])], ncatt_get(f$tasmax, 0, "title")$value, definemode=TRUE)

    ## Put coordinates and grid_mapping onto var if present in original data.
    if(coordinate.vars.att$hasatt)
      ncatt_put(new.file, cdx.dat$name.ncdf[x], "coordinates", coordinate.vars.att$value)
    if(grid.mapping.att$hasatt)
      ncatt_put(new.file, cdx.dat$name.ncdf[x], "grid_mapping", grid.mapping.att$value)
    
    ## Tag base period onto var
    if(cdx.dat$base.period.attr[x])
      ncatt_put(new.file, cdx.dat$name.ncdf[x], "base_period", paste(base.range, collapse="-"), definemode=TRUE)
    nc_enddef(new.file)

    ncvar_put(new.file, time.bnds.name, time.bnds.data)
    for(v in vars.to.copy)
      if(!is.null(vars.data[[v]]))
         ncvar_put(new.file, v, vars.data[[v]])
    
    new.file
  }))
}

## Get dim sizes, with checking to make sure sizes are all the same.
get.dim.size <- function(f, v.list) {
  dim.size.list <- lapply(1:3, function(i) { f[[i]]$var[[v.list[[i]]]]$varsize })
  stopifnot(all.the.same(dim.size.list))
  dim.size.list[[1]]
}

## Get dim axes, with checking to make sure they all have same axes.
get.dim.axes <- function(f, v.list) {
  dim.axes.list <- lapply(1:3, function(i) { nc.get.dim.axes(f[[i]], v.list[[i]]) })
  stopifnot(all.the.same(dim.axes.list))
  dim.axes.list[[1]]
}

## Get timeseries (as PCICt), with error checking to ensure input files have same TS.
get.ts <- function(f) {
  ts.list <- lapply(lapply(f, nc.get.time.series), trunc, "days")
  stopifnot(all.the.same(ts.list))
  ts.list[[1]]
}

## Compute all indices for a single grid box
compute.climdex.indices <- function(in.dat, cdx.funcs, ts, base.range, fclimdex.compatible) {
  ci <- NULL
  if("bs.pctile" %in% names(in.dat)) {
    bs.pctile <- as.data.frame(in.dat$bs.pctile)
    prec.pctile <- in.dat$prec.pctile
    ci <- climdexInput.raw(in.dat$tasmax, in.dat$tasmin, in.dat$pr, ts, ts, ts, base.range=base.range, northern.hemisphere=in.dat$northern.hemisphere, pad.data.with.first.last.values=fclimdex.compatible, temp.quantiles.notbase=bs.pctile, prec.quantiles=prec.pctile)
  } else {
    ci <- climdexInput.raw(in.dat$tasmax, in.dat$tasmin, in.dat$pr, ts, ts, ts, base.range=base.range, northern.hemisphere=in.dat$northern.hemisphere, pad.data.with.first.last.values=fclimdex.compatible)
  }
  ## NOTE: Names must be stripped here because it increases memory usage on the head by a factor of 8-9x (!)
  return(lapply(cdx.funcs, function(f) { d <- f(ci); names(d) <- NULL; d }))
}

## Reduce dims must be contiguous.
flatten.dims <- function(dat, reduce.dims, names.subset) {
  stopifnot(all(diff(reduce.dims) == 1))
  dat.dim <- dim(dat)
  if(!missing(names.subset))
    dat.dimnames <- dimnames(dat)
  before.reduce <- 1:length(dat.dim) < min(reduce.dims)
  after.reduce <- 1:length(dat.dim) > max(reduce.dims)
  new.dims <- c(dat.dim[before.reduce], prod(dat.dim[reduce.dims]), dat.dim[after.reduce])
  dim(dat) <- new.dims
  if(!missing(names.subset))
    dimnames(dat) <- dat.dimnames[names.subset]
  return(dat)
}

## Handles sucking data in, converting units, transposing data to (T, S) dimensionality, dropping spurious Z dims.
get.data <- function(f, v, subset, src.units, dest.units, dim.axes) {
  dat <- if(!missing(src.units) && !missing(dest.units))
    ud.convert(nc.get.var.subset.by.axes(f, v, subset), src.units, dest.units)
  else
    nc.get.var.subset.by.axes(f, v, subset)
    
  reduce.dims <- which(dim.axes %in% c("X", "Y", "Z"))
  return(t(flatten.dims(dat, reduce.dims=reduce.dims)))
}

## FIXME: Look into whether I can simply pass in 'f' from the global namespace... would be less horrific
compute.indices.for.stripe <- function(subset, cdx.funcs, ts, base.range, fclimdex.compatible, src.units, dest.units, v.list) {
  ## Dimension order: Time, Space for each Var in list
  var.idx <- 1:3
  names(var.idx) <- names(f)
  dim.axes <- get.dim.axes(f, v.list)
  data.list <- lapply(var.idx, function(x) { gc(); get.data(f[[x]], v.list[[x]], subset, src.units[x], dest.units[x], dim.axes) })
  gc()
  
  ## FIXME: Should be more careful when choosing what the lat dim is.
  lat.vals <- f[[1]]$var[[v.list[[1]]]]$dim[[2]]$vals
  if(any(names(subset) == "Y"))
    lat.vals <- lat.vals[subset$Y]
  lat.vals <- rep(lat.vals, each=(dim(data.list[[1]])[2] / length(lat.vals)))
  northern.hemisphere <- lat.vals >= 0

  if(!is.null(thresholds.netcdf)) {
    thresholds <- get.thresholds.chunk(subset, thresholds.netcdf)
    return(lapply(1:(dim(data.list[[1]])[2]), function(x) { compute.climdex.indices(list(tasmax=data.list$tasmax[,x], tasmin=data.list$tasmin[,x], pr=data.list$pr[,x], northern.hemisphere=northern.hemisphere[x], bs.pctile=thresholds$bs.pctile[,,x], prec.pctile=thresholds$prec.pctile[,x]), cdx.funcs, ts, base.range, fclimdex.compatible) } ))
  } else {
    return(lapply(1:(dim(data.list[[1]])[2]), function(x) { compute.climdex.indices(list(tasmax=data.list$tasmax[,x], tasmin=data.list$tasmin[,x], pr=data.list$pr[,x], northern.hemisphere=northern.hemisphere[x]), cdx.funcs, ts, base.range, fclimdex.compatible) } ))
  }
}
## Get a strip of thresholds; unpleasant because we need to handle dims correctly, fetch multiple vars, and aggregate these into a sensible data structure
get.thresholds.chunk <- function(subset, thresholds.netcdf) {
  vars.3d <- c("tx10thresh", "tx90thresh", "tn10thresh", "tn90thresh")
  vars.2d <- c("r95thresh", "r99thresh")

  dim.axes.3d <- nc.get.dim.axes(thresholds.netcdf, vars.3d[1])
  dim.axes.2d <- nc.get.dim.axes(thresholds.netcdf, vars.2d[1])
  bs.pctile <- sapply(vars.3d, function(threshold.var) { get.data(thresholds.netcdf, threshold.var, subset, dim.axes=dim.axes.3d) }, simplify="array")
  prec.pctile <- sapply(vars.2d, function(threshold.var) { get.data(thresholds.netcdf, threshold.var, subset, dim.axes=dim.axes.2d) }, simplify=TRUE)
  
  ## Change dimensionality so data is T, Var, S (and Var, S for 2D)
  bs.pctile <- aperm(bs.pctile, c(1, 3, 2))
  prec.pctile <- t(prec.pctile)
  
  list(bs.pctile=bs.pctile, prec.pctile=prec.pctile)
}

## Write out results for variables computed
write.climdex.results <- function(climdex.results, chunk.subset, cdx.dat, cdx.ncfile, dim.size) {
  xy.dims <- dim.size[1:2]
  if(!is.null(chunk.subset$X))
    xy.dims[1] <- length(chunk.subset$X)
  if(!is.null(chunk.subset$Y))
    xy.dims[2] <- length(chunk.subset$Y)

  ## Write out results, variable by variable
  lapply(1:length(cdx.ncfile), function(v) {
    gc()
    dat <- t(do.call(cbind, lapply(climdex.results, function(cr) { cr[[v]] })))
    dat.dim <- dim(dat)
    if(length(dat) == 1)
      print(dat)
    dim(dat) <- c(xy.dims, dat.dim[2])
    nc.put.var.subset.by.axes(cdx.ncfile[[v]], cdx.dat$name.ncdf[v], dat, chunk.subset)
  })
}

get.quantiles.for.cell <- function(dat, ts, base.range, pad.data.with.first.last.values=FALSE, new.names=NULL) {
  res <- get.outofbase.quantiles(dat$tasmax, dat$tasmin, dat$pr, ts, ts, ts, base.range, pad.data.with.first.last.values=pad.data.with.first.last.values)
  res <- c(res$bs.pctile, res$prec.pctile)
  if(!is.null(new.names))
    names(res) <- new.names
  res
}

get.quantiles.for.stripe <- function(subset, ts, base.range, v.list, src.units, dest.units, pad.data.with.first.last.values=FALSE, new.names=NULL) {
  var.idx <- 1:3
  names(var.idx) <- names(f)
  dim.axes <- get.dim.axes(f, v.list)
  data.list <- lapply(var.idx, function(x) { gc(); get.data(f[[x]], v.list[[x]], subset, src.units[x], dest.units[x], dim.axes) })
  gc()
  
  return(lapply(1:(dim(data.list[[1]])[2]), function(x) { get.quantiles.for.cell(list(tasmax=data.list$tasmax[,x], tasmin=data.list$tasmin[,x], pr=data.list$pr[,x]), ts, base.range, pad.data.with.first.last.values, new.names) } ))
}

set.up.cluster <- function(parallel, file.to.source, type="SOCK") {
  ## Fire up the cluster...
  cluster <- NULL

  if(!file.exists(file.to.source))
    stop("Code file does not exist.")
  
  if(!is.logical(parallel)) {
    cat(paste("Creating cluster of", parallel, "nodes of type", type, "\n"))
    cluster <- makeCluster(parallel, type)
    ## Bootstrap it with the packages we'll need.
    clusterExport(cluster, "file.to.source", environment())
    clusterEvalQ(cluster, source(file.to.source))

    ## This should not be necessary. Really...
    clusterEvalQ(cluster, library(climdex.pcic))
    clusterEvalQ(cluster, library(udunits2))
    clusterEvalQ(cluster, library(ncdf4))
    clusterEvalQ(cluster, library(ncdf4.helpers))
  }
  cluster
}

shut.down.cluster <- function(cluster) {
  ## Kill off the cluster...
  if(!is.null(cluster))
    stopCluster(cluster)
}

create.thresholds.file <- function(thresholds.file, f, ts, v.list, base.range, dim.size, dim.axes, threshold.info) {
  exemplar.var <- f$tasmax$var[[v.list$tasmax]]
  num.thresholds <- ifelse(is.null(attr(ts, "dpy")), 365, attr(ts, "dpy"))
  cal <- attr(ts, "cal")

  ## Get time metadata...
  old.time.dim <- exemplar.var$dim[[which(dim.axes == "T")]]
  time.units <- old.time.dim$units
  time.units.split <- strsplit(time.units, " ")[[1]]
  time.origin <- if(time.units.split[2] == "as") format(trunc(min(ts), units="days"), "%Y-%m-%d") else time.units.split[3]
  time.dim.name <- old.time.dim$name
  old.time.bnds.att <- ncatt_get(f$tasmax, time.dim.name, "bounds")
  time.bnds.name <- if(old.time.bnds.att$hasatt) old.time.bnds.att$value else "time_bnds"

  ## Set up time variables
  out.time <- as.numeric(julian(as.PCICt(paste(floor(mean(base.range)), 1:num.thresholds, sep="-"), attr(ts, "cal"), format="%Y-%j"), as.PCICt(time.origin, cal)), units="days")
  out.time.dim <- ncdim_def("time", paste("days since", time.origin), out.time, unlim=TRUE, calendar=cal, longname="time")

  ## Set up bounds
  input.bounds <- nc.get.dim.bounds.var.list(f$tasmax)
  input.bounds <- input.bounds[input.bounds != time.bnds.name]
  input.dim.names <- nc.get.dim.names(f$tasmax, "tasmax")
  input.varname.list <- c(input.bounds, input.dim.names)
  
  bnds.dim <- ncdim_def("bnds", "", 1:2, create_dimvar=FALSE)
  if(length(input.bounds) > 0)
    bnds.dim <- f$tasmax$var[[input.bounds[1]]]$dim[[1]]
  out.time.bnds <- as.numeric(julian(as.PCICt(c(paste(base.range[1], 1:num.thresholds, sep="-"), paste(base.range[2], 1:num.thresholds, sep="-")), attr(ts, "cal"), format="%Y-%j"), as.PCICt(time.origin, cal)), units="days")
  dim(out.time.bnds) <- c(num.thresholds, 2)
  out.time.bnds <- t(out.time.bnds)
  out.time.bnds.var <- ncvar_def(time.bnds.name, '', list(bnds.dim, out.time.dim), longname='', prec="double")

  input.bounds.vars <- c(lapply(input.bounds, function(x) { f$tasmax$var[[x]] }), list(out.time.bnds.var))
  input.bounds.data <- c(lapply(input.bounds, function(x) { ncvar_get(f$tasmax, x) }), list(out.time.bnds))
  all.bounds <- c(input.bounds, time.bnds.name)
  names(input.bounds.data) <- names(input.bounds.vars) <- all.bounds

  ## Set up 2d and 3d dims
  out.dims.3d <- list(exemplar.var$dim[[which(dim.axes == 'X')]], exemplar.var$dim[[which(dim.axes == 'Y')]], out.time.dim)
  out.dims.2d <- list(exemplar.var$dim[[which(dim.axes == 'X')]], exemplar.var$dim[[which(dim.axes == 'Y')]])
  out.vars <- lapply(1:length(threshold.info$names), function(i) {
    if(threshold.info$has.time[i])
      ncvar_def(threshold.info$names[i], threshold.info$units[i], out.dims.3d, 1e20, threshold.info$longname[i], prec="double")
    else
      ncvar_def(threshold.info$names[i], threshold.info$units[i], out.dims.2d, 1e20, threshold.info$longname[i], prec="double")
  })
  names(out.vars) <- threshold.info$names

  ## Tack bounds vars onto var list so they get created...
  all.vars <- c(input.bounds.vars, out.vars)

  ## Create file
  thresholds.netcdf <- nc_create(thresholds.file, all.vars, force_v4=TRUE)
  out.dim.axes <- c("X", "Y", "T")

  ## Copy attributes for all variables plus global attributes
  nc_redef(thresholds.netcdf)
  nc.copy.atts(f$tasmax, 0, thresholds.netcdf, 0, definemode=TRUE)
  for(v in input.varname.list) {
    nc.copy.atts(f$tasmax, v, thresholds.netcdf, v, definemode=TRUE)
  }

  put.ETCCDI.atts(thresholds.netcdf, "monClim", ncatt_get(f$tasmax, 0, "title")$value, definemode=TRUE)

  ## Attach history data to threshold data.
  lapply(out.vars, function(v) {
    put.history.att(thresholds.netcdf, v, definemode=TRUE)
    ncatt_put(thresholds.netcdf, v, "base_period", paste(base.range, collapse="-"), definemode=TRUE)
  })
  nc_enddef(thresholds.netcdf)
  
  ## Put bounds data.
  for(v in all.bounds) {
    ncvar_put(thresholds.netcdf, v, input.bounds.data[[v]])
  }

  return(thresholds.netcdf)
}

## Run Climdex to generate indices for computing Climdex on future data
create.thresholds.from.file <- function(prec.file, tmax.file, tmin.file, thresholds.file, axis.to.split.on="Y", base.range=c(1961, 1990), parallel=4, verbose=FALSE, fclimdex.compatible=TRUE, max.vals.millions=10, cluster.type="SOCK", code.file="/home/data/projects/CMIP5_climdex/code/compute_index_on_gcm.r", tmax.as.prec.dirty.hack=FALSE) {
  if(is.character(parallel))
    parallel <- as.numeric(parallel)

  ## Open all the files and verify that everything matches
  nc.file.list <- c(pr=prec.file, tasmax=tmax.file, tasmin=tmin.file)
  f <- lapply(nc.file.list, nc_open)
  v.list <- lapply(f, nc.get.variable.list, min.dims=2)
  stopifnot(all(sapply(v.list, length) == 1))
  names(v.list) <- names(f) <- c("pr", "tasmax", "tasmin")
  
  src.units <- sapply(1:length(v.list), function(i) { f[[i]]$var[[v.list[[i]]]]$units })
  dest.units <- c("kg m-2 d-1", "degrees_C", "degrees_C")

  ## Define what the threshold indices will look like...
  threshold.info <- list(names=c("tx10thresh", "tx90thresh", "tn10thresh", "tn90thresh", "r95thresh", "r99thresh"),
                         units=dest.units[c(2, 2, 3, 3, 1, 1)],
                         longname=c("10th_percentile_running_baseline_tasmax", "90th_percentile_running_baseline_tasmax", "10th_percentile_running_baseline_tasmin",
                           "90th_percentile_running_baseline_tasmin", "95th_percentile_baseline_wet_day_pr", "99th_percentile_baseline_wet_day_pr"),
                         has.time=c(TRUE, TRUE, TRUE, TRUE, FALSE, FALSE)
                         )
  
  ts <- get.ts(f)
  dim.size <- get.dim.size(f, v.list)
  dim.axes <- get.dim.axes(f, v.list)

  ## Create the output file
  thresholds.netcdf <- create.thresholds.file(thresholds.file, f, ts, v.list, base.range, dim.size, dim.axes, threshold.info)

  cluster <- set.up.cluster(parallel, code.file, cluster.type)

  ## Dark magic to open the NetCDF files on all the cluster nodes.
  if(!is.null(cluster)) {
    clusterExport(cluster, "nc.file.list", environment())
    clusterEvalQ(cluster, f <<- lapply(nc.file.list, nc_open, readunlim=FALSE))
    clusterEvalQ(cluster, names(f) <- c("pr", "tasmax", "tasmin"))
  } else {
    lapply(f, nc_close)
    rm(f)
    f <<- lapply(nc.file.list, nc_open, readunlim=FALSE)
    names(f) <<- c("pr", "tasmax", "tasmin")
  }

  ## Compute subsets and fire jobs off; collect and write out chunk-at-a-time
  subsets <- get.cluster.worker.subsets(max.vals.millions * 1000000, dim.size, dim.axes, axis.to.split.on)
  parLapplyLBFiltered(cluster, subsets,   get.quantiles.for.stripe, ts, base.range, v.list, src.units, dest.units, pad.data.with.first.last.values=FALSE, new.names=NULL, local.filter.func=function(out.list, out.sub) {
    lapply(threshold.info$names, function(name) {
      dat <- sapply(out.list, function(y) { return(y[[name]]) })
      dim.axes.thresh <- nc.get.dim.axes(thresholds.netcdf, name)
      dim.size <- sapply(dim.axes.thresh, function(x) { if(any(names(out.sub) == x)) length(out.sub[[x]]) else thresholds.netcdf$dim[[names(dim.axes.thresh)[dim.axes.thresh == x]]]$len })
      if(length(dim.size) == 3)
        dat <- t(dat)
      dim(dat) <- dim.size
      nc.put.var.subset.by.axes(thresholds.netcdf, name, dat, out.sub)
    })
    gc()
  })

  shut.down.cluster(cluster)
  
  ## Close all the files
  lapply(f, nc_close)
  nc_close(thresholds.netcdf)

  cat("Finished computing thresholds\n")
}

open.thresholds <- function(thresholds.files) {
  f <- lapply(thresholds.files, nc_open)
  if(length(f) == 1)
    return(f[[1]])
  return(f)
}

close.thresholds <- function(thresholds.nc) {
  if(is.list(thresholds.nc) && class(thresholds.nc) != "ncdf4")
    lapply(thresholds.nc, nc_close)
  else
    nc_close(thresholds.nc)
}                                           
  
## Run Climdex and populate the output files
create.indices.from.file <- function(prec.file, tmax.file, tmin.file, out.dir, output.filename.template, axis.to.split.on="Y", fclimdex.compatible=TRUE, base.range=c(1961, 1990), parallel=4, verbose=FALSE, thresholds.file=NULL, max.vals.millions=10, cluster.type="SOCK", code.file="/home/data/projects/CMIP5_climdex/code/compute_index_on_gcm.r", tmax.as.prec.dirty.hack=FALSE) {
  if(is.character(parallel))
    parallel <- as.numeric(parallel)

  ## Create data structure containing all the Climdex stuff we're going to compute

  ## Open all the files and verify that everything matches
  dest.units <- c("kg m-2 d-1", "degrees_C", "degrees_C")
  nc.file.list <- c(prec.file, tmax.file, tmin.file)
  subset.vars <- NULL
  if(tmax.as.prec.dirty.hack) {
    nc.file.list <- c(tmax.file, tmax.file, tmin.file)
    dest.units <- c("degrees_C", "degrees_C", "degrees_C")
    subset.vars <- c("climdex.fd", "climdex.su", "climdex.id", "climdex.tr", "climdex.gsl",
                     "climdex.txx", "climdex.tnx", "climdex.txn", "climdex.tnn", "climdex.tn10p", "climdex.tx10p", "climdex.tn90p", "climdex.tx90p",
                     "climdex.wsdi", "climdex.csdi", "climdex.dtr")
  }
  f <- lapply(nc.file.list, nc_open)
  v.list <- lapply(f, nc.get.variable.list, min.dims=2)
  stopifnot(all(sapply(v.list, length) == 1))
  names(v.list) <- names(f) <- c("pr", "tasmax", "tasmin")
  exemplar.var <- f$tasmax$var[[v.list$tasmax]]
  
  cdx.dat <- get.climdex.info.struct(subset.vars=subset.vars, fclimdex.compatible=fclimdex.compatible, out.dir=out.dir, out.postfix=output.filename.template)

  ts <- get.ts(f)
  dim.size <- get.dim.size(f, v.list)
  dim.axes <- get.dim.axes(f, v.list)

  time.units <- exemplar.var$dim[[which(dim.axes == "T")]]$units

  time.units.split <- strsplit(gsub("[ ]+", " ", time.units), " ")[[1]]
  time.origin <- if(time.units.split[2] == "as") format(trunc(min(ts), units="days"), "%Y-%m-%d") else time.units.split[3]
  
  ## Get units and specify destination units
  src.units <- sapply(1:length(v.list), function(i) { f[[i]]$var[[v.list[[i]]]]$units })

  cdx.ncfile <- create.ncdf.output.files(cdx.dat, f, v.list, ts, time.origin, base.range)
  
  cluster <- set.up.cluster(parallel, code.file, cluster.type)
  ## Dark magic to open the NetCDF files on all the cluster nodes.
  if(!is.null(cluster)) {
    clusterExport(cluster, "nc.file.list", environment())
    clusterExport(cluster, "thresholds.file", environment())
    clusterEvalQ(cluster, f <<- lapply(nc.file.list, nc_open, readunlim=FALSE))
    clusterEvalQ(cluster, names(f) <- c("pr", "tasmax", "tasmin"))
    if(!is.null(thresholds.file))
      clusterEvalQ(cluster, thresholds.netcdf <<- open.thresholds(thresholds.file))
    else
      clusterEvalQ(cluster, thresholds.netcdf <<- NULL)
  } else {
    lapply(f, nc_close)
    rm(f)
    f <<- lapply(nc.file.list, nc_open, readunlim=FALSE)
    names(f) <<- c("pr", "tasmax", "tasmin")
    if(!is.null(thresholds.file))
      thresholds.netcdf <<- open.thresholds(thresholds.file)
    else
      thresholds.netcdf <<- NULL
  }

  ## Compute subsets and fire jobs off
  subsets <- get.cluster.worker.subsets(max.vals.millions * 1000000, dim.size, dim.axes, axis.to.split.on)

  if(!is.null(cluster)) {
    parLapplyLBFiltered(cluster, subsets, compute.indices.for.stripe, cdx.dat$func, ts, base.range, fclimdex.compatible, src.units, dest.units, v.list, local.filter.func=function(x, x.sub) {
      write.climdex.results(x, x.sub, cdx.dat, cdx.ncfile, dim.size)
    })
  } else {
    lapply(subsets, function(x) { write.climdex.results(compute.indices.for.stripe(x, cdx.dat$func, ts, base.range, fclimdex.compatible, src.units, dest.units, v.list), x, cdx.dat, cdx.ncfile, dim.size) })
  }

  shut.down.cluster(cluster)
  
  ## Close all the files
  lapply(f, nc_close)
  lapply(cdx.ncfile, nc_close)
  if(!is.null(thresholds.file) && is.null(cluster))
    close.thresholds(thresholds.netcdf)

  cat("Finished computing indices\n")
}
