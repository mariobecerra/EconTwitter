# Reads files in unique_users_A.csv and unique_users_B.csv in relation_chunks_folder folder. Creates a dataframe with the unique users and assigns a unique user id to each user. This new ID is an integer that goes from 1 to the number of unique users. The dataframe is ordered according to the original user ID (string).
# The whole script takes around 10 minutes to run.

# Setup -------------------------------------------------------------------

#Around 20 minutes

library(tidyverse)
library(data.table)
library(here)


# Read data ---------------------------------------------------------------

relation_chunks_folder = "temp/relation_chunks/"

# Code --------------------------------------------------------------------


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
  arrange(user) %>% 
  mutate(new_id = 1:nrow(.))

# Original relation file: 93,220,265 unique users
# New relation file: 76,491,046 unique users

# The unique_users object takes up 7 GB of RAM space according to object.size, although the R session was taking up 9 GB of space according to Mac's Activity Monitor.


fwrite(
  unique_users,
  file = here(paste0(relation_chunks_folder, "user_id_mapping.csv"))
)



