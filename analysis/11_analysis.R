library(tidyverse)
library(data.table)
library(fst)
library(here)

chunk_folder = here("temp/")

n_followback_A_filename = here("temp/n_followback_A.fst")

info_users_A = read_fst(n_followback_A_filename)
setDT(info_users_A, key = "user")








