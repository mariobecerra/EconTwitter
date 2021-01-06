#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(JJHmisc) # you need to add this package from https://github.com/johnjosephhorton/JJHmisc
  library(latex2exp)
  library(ggplot2)
  library(magrittr)
  library(feather)
  library(here)
})

# latest version of data: https://drive.google.com/drive/folders/1Z0BFSTK_dkMiSRrfeulgf6YcA4jzSuDx?usp=sharing
# more details for data:  https://docs.google.com/document/d/1Vdy-lPRmbDSeQhpg2Edc1gWJMj8xNSgXiA_VS0Itne0/edit?usp=sharing

# After downloading, read the files
df.repec     <- readRDS(here("data/repec_economists.rds"))
df.seeds     <- readRDS(here("data/data_seeds.rds"))
df.data      <- readRDS(here("data/data.rds"))
df.relation  <- readRDS(here("data/relation.rds"))

# convert RDS -> FTR : size increases, but I/O speeds increases *a lot*
write_feather(df.repec,     here("temp/repec_economists.ftr"))
write_feather(df.seeds,     here("temp/data_seeds.ftr"))
write_feather(df.data,      here("temp/data.ftr"))
write_feather(df.relation,  here("temp/relation.ftr"))