# Reads each chunk in relation_chunks_folder and finds the unique users in column A. It saves the list of unique users in relation_chunks_folder in the unique_users_A.csv file.
# The whole script takes about 15 minutes.

# Setup -------------------------------------------------------------------

library(tidyverse)
library(data.table)
# library(fst)
library(here)


# Read data ---------------------------------------------------------------

relation_chunks_folder = "temp/relation_chunks/"

# Code --------------------------------------------------------------------


counts_A_chunk_filenames = grep(
  pattern = "counts_A_chunk_",
  list.files(here(relation_chunks_folder)),
  value = T
)


unique_users_A = c("")
for(i in seq_along(counts_A_chunk_filenames)){
  
  cat("Reading chunk", i, "...")
  
  unique_users_A_i = fread(
    here(relation_chunks_folder, counts_A_chunk_filenames[i]), 
    colClasses = list(character = 1, integer = 2:3), 
    drop = 2:3,
    header = T
  )
  
  cat("Done.\n")
  
  cat("Appending new users...")
  unique_users_A = union(unique_users_A, unique_users_A_i$user)
  cat("Done. There are", length(unique_users_A), "so far.\n\n")
  
}

# unique_users_A = distinct(users_A)
fwrite(
  data.table(user = unique_users_A),
  file = here(paste0(relation_chunks_folder, "unique_users_A.csv"))
)





