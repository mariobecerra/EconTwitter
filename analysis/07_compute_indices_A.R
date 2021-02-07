# Computes indices of where each ID of column A is in relation_newids.csv and relation_newids.fst.
# It reads relation.fst in chunks (a total of 20 chunks), finds the row of each user, saves this into a tibble, and saves this tibble into a fst file (indices_A_chunk_i.fst).
# The whole script takes around 10 minutes to run.

t_init_script = Sys.time()
cat("Script starts at", as.character(t_init_script), "\n")

library(fst)
library(data.table)
library(tidyverse)
library(here)

output_folder = here("temp/indices_A_folder/")
dir.create(output_folder)

# (t1 = Sys.time())
# # 60 seconds to read
# relation = read_fst(here("temp/relation_newids.fst")) %>% 
#   as_tibble()
# # relation takes up around 3.4 GB of RAM
# (t2 = Sys.time())
# t2 - t1


(t1 = Sys.time())
# 15 seconds to read
relation_A = read_fst(
  here("temp/relation_newids.fst"),
  columns = "A") %>% 
  as_tibble()
# relation takes up around 3.4 GB of RAM
(t2 = Sys.time())
t2 - t1

n_rows = nrow(relation_A)
n_chunks = 20
n_rows_per_iter = ceiling(n_rows/n_chunks)


chunk_index_A = lapply(1:n_chunks, function(i){
  t1 = Sys.time()
  init = (i-1)*n_rows_per_iter + 1
  end =  (i)*n_rows_per_iter
  
  if(end > n_rows){
    end = n_rows
  }
  
  cat(
    "Doing chunk", i, "of", n_chunks, "from row", scales::comma(init), "to", scales::comma(end), "(",
    as.character(t1), ")\n")
  
  cat("\tComputing indices...")
  indices_chunk = split(init:end, relation_A$A[init:end])
  cat("Done.\n")
  
  cat("\tConverting to dataframe indices of", scales::comma(length(indices_chunk)), "users...")
  
  names_indices_chunk_integer = as.integer(names(indices_chunk))
  lengths = sapply(indices_chunk, length)
  indices_chunk_tbl = tibble(
    user = rep(names_indices_chunk_integer, times = lengths)
  ) %>% 
    mutate(index = unlist(indices_chunk))
  cat("Done\n")

  
  
 
  
  cat("\tSaving chunk...")
  write_fst(indices_chunk_tbl, paste0(output_folder, "/indices_A_chunk_", sprintf("%02d", i), ".fst"), compress = 0)
  cat("Done\n")
  
  
  out = tibble(
    user = names_indices_chunk_integer,
    chunk = i
  )
  
  
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("\tElapsed minutes in chunk:", minutes_passed, "\n\n")
  
  return(out)
  
}) %>% 
  bind_rows() 

t_end_script = Sys.time()
cat("Script ends at", as.character(t_end_script), "\n")
t_end_script - t_init_script

