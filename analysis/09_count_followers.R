# Counts how many followers each user has. It does so by counting the already arranged index table. The fact that it's already ordered makes the creation of data.table indexes very fast, making the counting also quite fast.
# The follower count is saved in a table which is saved as a fst file.

library(tidyverse)
library(data.table)
library(fst)
library(here)

chunk_folder = here("temp/indices_A_folder/")

(t1 = Sys.time())
indices_A = read_fst(paste0(chunk_folder, "indices_A.fst"))
setDT(indices_A, key = "user")
(t2 = Sys.time())
t2 - t1


(t1 = Sys.time())
# Count number of followers and arrange in descending order
# only 12 seconds!!
info_users_A = indices_A[, .N, by=.(user)][order(-N)] %>% 
  set_names(c("user", "n_followers"))
(t2 = Sys.time())
t2 - t1

write_fst(info_users_A, here("temp/n_followers_A.fst"), compress = 50)


