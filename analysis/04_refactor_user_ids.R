# Setup -------------------------------------------------------------------

#Around 20 minutes

library(tidyverse)
library(data.table)
# library(fst)
library(here)


# Read data ---------------------------------------------------------------

relation_chunks_folder = "temp/relation_chunks/"

# Code --------------------------------------------------------------------


# The unique_users object takes up 7 GB of RAM space according to object.size, although the R session was taking up 9 GB of space according to Mac's Activity Monitor.

unique_users = fread(
  here(paste0(relation_chunks_folder, "unique_users_A.csv")),
  colClasses = list(character = 1), 
  header = T) %>% 
  bind_rows(
    fread(
      here(paste0(relation_chunks_folder, "unique_users_B.csv")),
      colClasses = list(character = 1), 
      header = T
    )
  ) %>% 
  distinct() %>% 
  mutate(new_id = 1:nrow(.))

unique_users
# 93,220,265 unique users

fwrite(
  unique_users,
  file = here(paste0(relation_chunks_folder, "user_id_mapping.csv"))
)



