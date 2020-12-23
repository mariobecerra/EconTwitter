#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(JJHmisc)
  library(latex2exp)
  library(ggplot2)
  library(magrittr)
  library(feather)
})

# latest version of data: https://drive.google.com/drive/folders/1Z0BFSTK_dkMiSRrfeulgf6YcA4jzSuDx?usp=sharing
# more details for data:  https://docs.google.com/document/d/1Vdy-lPRmbDSeQhpg2Edc1gWJMj8xNSgXiA_VS0Itne0/edit?usp=sharing

# After downloading, read the files
df.repec     <- readRDS("../data/raw/repec_economists.rds")
df.seeds     <- readRDS("../data/raw/data_seeds.rds")
df.data      <- readRDS("../data/raw/data.rds")
df.relation  <- readRDS("../data/raw/relation.rds")

# convert RDS -> FTR : size increases, but I/O speeds increases *a lot*
write_feather(df.repec,     "../data/raw/repec_economists.ftr")
write_feather(df.seeds,     "../data/raw/data_seeds.ftr")
write_feather(df.data,      "../data/raw/data.ftr")
write_feather(df.relation,  "../data/raw/relation.ftr")