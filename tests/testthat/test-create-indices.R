library(climdex.pcic)
library(snow)
library(ncdf4)

# slow end-to-end tests. Can't be run on CRAN, too slow.

author.data <- list(institution="Test Institution", 
		institution_id="TI",
		indices_archive="https://example.com/nonexistant",
		contact="email@example.com",
		references="{}")

input.files <- list("input_pr_1950-2000.nc", 
		"input_tasmax_1950-2000.nc",
		"input_tasmin_1950-2000.nc")

output.filename.template <- "variable_table_CanESM2_historical+rcp85_r1i1p1_1950-2000"

out.dir <- "output"

test_that("create.indices.from.files calculates gsl (non-threshold index)", {
  skip_on_cran()
  create.indices.from.files(input.files, out.dir, output.filename.template, 
	author.data, axis.to.split.on="X", climdex.vars.subset=list("gsl"))
  expected_nc <- nc_open("output/expected_gsl.nc")
  file.copy("output/gslETCCDI_yr_CanESM2_historical+rcp85_r1i1p1_1950-2000.nc", 
		    "output/output_gsl.nc")
  output_nc <- nc_open("output/output_gsl.nc")
  expect_equal(ncvar_get(expected_nc, "gslETCCDI"), ncvar_get(output_nc, "gslETCCDI"))
  nc_close(expected_nc)
  nc_close(output_nc)
  file.remove("output/output_gsl.nc")
})

test_that("create.indices.from.files calculates tn10p (threshold index)", {
  skip_on_cran()
  create.indices.from.files(input.files, out.dir, output.filename.template, 
	author.data, axis.to.split.on="X", climdex.vars.subset=list("tn10p"),
	climdex.time.resolution="annual")
  expected_nc <- nc_open("output/expected_tn10p.nc")
  file.copy("output/tn10pETCCDI_yr_CanESM2_historical+rcp85_r1i1p1_1950-2000.nc",
		  "output/output_tn10p.nc")
  output_nc <- nc_open("output/output_tn10p.nc")
  expect_equal(ncvar_get(expected_nc, "tn10pETCCDI"), ncvar_get(output_nc, "tn10pETCCDI"))
  nc_close(expected_nc)
  nc_close(output_nc)
  file.remove("output/output_tn10p.nc")
})