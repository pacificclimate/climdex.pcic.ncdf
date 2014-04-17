test.get.climdex.variable.list <- function() {
  checkEquals(climdex.var.list$tavg.all, get.climdex.variable.list(c("tavg")))
  checkEquals(climdex.var.list$tmax.all, get.climdex.variable.list(c("tmax")))
  checkEquals(climdex.var.list$tmax.mon, get.climdex.variable.list(c("tmax"), time.resolution="monthly"))
  checkEquals(climdex.var.list$tmax.yr, get.climdex.variable.list(c("tmax"), time.resolution="annual"))
  checkEquals(climdex.var.list$tmax.tmin.all, get.climdex.variable.list(c("tmax", "tmin")))
  checkEquals(climdex.var.list$tmax.prec.yr, get.climdex.variable.list(c("tmax", "prec"), time.resolution="annual"))
  checkEquals(climdex.var.list$prec.mon, get.climdex.variable.list(c("prec"), time.resolution="monthly"))
  checkEquals(climdex.var.list$prec.yr, get.climdex.variable.list(c("prec"), time.resolution="annual"))
  checkEquals(climdex.var.list$tmax.tmin.prec.all, get.climdex.variable.list(c("tmax", "tmin", "prec")))
  checkEquals(climdex.var.list$tmax.tmin.prec.sub, get.climdex.variable.list(c("tmax", "tmin", "prec"), climdex.vars.subset=c("su", "tr", "cdd", "gsl")))
  checkEquals(climdex.var.list$prec.sub, get.climdex.variable.list(c("prec"), climdex.vars.subset=c("su", "tr", "cdd", "gsl")))
  checkEquals(NULL, get.climdex.variable.list(c()))
}

test.get.climdex.variable.metadata <- function() {
  fn1 <- "tasmax_NAM44_CanRCM4_ERAINT_r1i1p1_1989-2009.nc"
  fn2 <- "pr_day_CanESM2_rcp85_r2i1p1_20060101-21001231.nc"
  checkEquals(climdex.var.meta$tavg.all.1, get.climdex.variable.metadata(climdex.var.list$tavg.all, fn1))
  checkEquals(climdex.var.meta$prec.yr.2, get.climdex.variable.metadata(climdex.var.list$prec.yr, fn2))
}

test.get.climdex.functions <- function() {
  checkEquals(climdex.functions$tmax.yr, get.climdex.functions(climdex.var.list$tmax.yr))
  checkEquals(climdex.functions$tmax.tmin.prec.all.fclimdex, get.climdex.functions(climdex.var.list$tmax.tmin.prec.all))
  checkEquals(climdex.functions$tmax.tmin.prec.all.notfclimdex, get.climdex.functions(climdex.var.list$tmax.tmin.prec.all, FALSE))
}

test.create.cmip5.climdex.filenames <- function() {
  
}
