library(climdex.pcic)
load("exemplar_data.rda")

test_that("get.climdex.variable.list returns all needed data", {
	expect_equal(climdex.var.list$tavg.all, get.climdex.variable.list(c("tavg")))
	expect_equal(climdex.var.list$tmax.all, get.climdex.variable.list(c("tmax")))
	expect_equal(climdex.var.list$tmax.mon, get.climdex.variable.list(c("tmax"), time.resolution="monthly"))
	expect_equal(climdex.var.list$tmax.yr, get.climdex.variable.list(c("tmax"), time.resolution="annual"))
	expect_equal(climdex.var.list$tmax.tmin.all, get.climdex.variable.list(c("tmax", "tmin")))
	expect_equal(climdex.var.list$tmax.prec.yr, get.climdex.variable.list(c("tmax", "prec"), time.resolution="annual"))
	expect_equal(climdex.var.list$prec.mon, get.climdex.variable.list(c("prec"), time.resolution="monthly"))
	expect_equal(climdex.var.list$prec.yr, get.climdex.variable.list(c("prec"), time.resolution="annual"))
	expect_equal(climdex.var.list$tmax.tmin.prec.all, get.climdex.variable.list(c("tmax", "tmin", "prec")))
	expect_equal(climdex.var.list$tmax.tmin.prec.sub, get.climdex.variable.list(c("tmax", "tmin", "prec"), climdex.vars.subset=c("su", "tr", "cdd", "gsl")))
	expect_equal(climdex.var.list$prec.sub, get.climdex.variable.list(c("prec"), climdex.vars.subset=c("su", "tr", "cdd", "gsl")))
	expect_equal(NULL, get.climdex.variable.list(c()))
})

test_that("climdex.var.meta accesses variable metadata", {
	fn1 <- "tasmax_NAM44_CanRCM4_ERAINT_r1i1p1_1989-2009.nc"
	fn2 <- "pr_day_CanESM2_rcp85_r2i1p1_20060101-21001231.nc"
	expect_equal(climdex.var.meta$tavg.all.1, get.climdex.variable.metadata(climdex.var.list$tavg.all, fn1))
	expect_equal(climdex.var.meta$prec.yr.2, get.climdex.variable.metadata(climdex.var.list$prec.yr, fn2))
})

test_that("get.climdex.functions returns curried functions for indices", {
	expect_equal(climdex.functions$tmax.yr, get.climdex.functions(climdex.var.list$tmax.yr))
	expect_equal(climdex.functions$tmax.tmin.prec.all.fclimdex, get.climdex.functions(climdex.var.list$tmax.tmin.prec.all))
	expect_equal(climdex.functions$tmax.tmin.prec.all.notfclimdex, get.climdex.functions(climdex.var.list$tmax.tmin.prec.all, FALSE))
})

test_that("create.climdex.cmip5.filenames generates valid CMIP5-compliant filenames for inputs", {
	fn.split <- c(model="CanESM2", emissions="rcp45", run="r1i1p1", tstart="20100101", tend="20991231")
	
	valid.tmax.mon.fn <- c("txxETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "txnETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc",
			"tx10pETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "tx90pETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc")
	valid.tmax.all.fn <- c("suETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc", "idETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc",
			"txxETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "txxETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc",
			"txnETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "txnETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc",
			"tx10pETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "tx10pETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc",
			"tx90pETCCDI_mon_CanESM2_rcp45_r1i1p1_201001-209912.nc", "tx90pETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc",
			"wsdiETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc", "altwsdiETCCDI_yr_CanESM2_rcp45_r1i1p1_2010-2099.nc")
	
	expect_equal(valid.tmax.mon.fn, create.climdex.cmip5.filenames(fn.split, climdex.var.list$tmax.mon))
	expect_equal(valid.tmax.all.fn, create.climdex.cmip5.filenames(fn.split, climdex.var.list$tmax.all))
})

test_that("flatten.dims collapses X and Y to a single dimension", {
	dat <- structure(1:8, .Dim=c(2, 2, 2))
	valid.flat <- structure(1:8, .Dim = c(2L, 4L))
	expect_equal(flatten.dims(dat, 2:3), valid.flat)
})