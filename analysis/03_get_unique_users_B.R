# Setup -------------------------------------------------------------------

# Takes around 3 minutes

library(tidyverse)
library(data.table)
# library(fst)
library(here)


# Read data ---------------------------------------------------------------

relation_chunks_folder = "temp/relation_chunks/"

# Code --------------------------------------------------------------------



counts_B_chunk_filenames = grep(
  pattern = "counts_B_chunk_",
  list.files(here(relation_chunks_folder)),
  value = T
)

unique_users_B = c("")
for(i in seq_along(counts_B_chunk_filenames)){
  
  cat("Reading chunk", i, "...")
  
  unique_users_B_i = fread(
    here(relation_chunks_folder, counts_B_chunk_filenames[i]), 
    colClasses = list(character = 1, integer = 2:3), 
    drop = 2:3,
    header = T
  )
  
  cat("Done.\n")
  
  cat("Appending new users...")
  unique_users_B = union(unique_users_B, unique_users_B_i$user)
  cat("Done. There are", length(unique_users_B), "so far.\n\n")
  
}

fwrite(
  data.table(user = unique_users_B),
  file = here(paste0(relation_chunks_folder, "unique_users_B.csv"))
)





