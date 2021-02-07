# Reads the chunks created in the previous step, merges them into a single table, and orders them for quick access later.
# Saves this table in a fast file.
# Whole process takes around 6 minutes (1 or 2 reading and sorting, 4 or 5 for writing the fst file)

library(tidyverse)
library(data.table)
library(fst)
library(here)

chunk_folder = here("temp/indices_A_folder/")

indices_A_files = grep(pattern = "indices_A_chunk_", x = list.files(chunk_folder), value = T)


# Takes 1 minute in total to read
(t1 = Sys.time())
chunk_indexes_A = lapply(seq_along(indices_A_files), function(i){
  
  chunk_file_i = paste0(chunk_folder, indices_A_files[i])
  
  chunk_i = read_fst(chunk_file_i) %>% 
    arrange(user) %>%   # IDK if this helps a lot (ordering each chunk), but it can't hurt
    data.table(.)
    
  return(chunk_i)
  
}) %>% 
  bind_rows() 
cat("Total time:\n")
(t2 = Sys.time())
t2 - t1

setkey(chunk_indexes_A, user)

write_fst(chunk_indexes_A, paste0(chunk_folder, "indices_A.fst"), compress = 50)