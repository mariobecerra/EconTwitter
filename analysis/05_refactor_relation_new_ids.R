# Reads relation.csv file in parts, appends it the new IDs for columns A and B, and saves the new relations in a csv file with the new IDs. It saves only the new IDs and not the older ones. It has the same order as the original relation.csv file.

# Setup -------------------------------------------------------------------

library(tidyverse)
library(data.table)
library(here)


# Read data ---------------------------------------------------------------

relation_chunks_folder = "temp/relation_chunks/"
relation_file = here("data/relation.csv")

# Code --------------------------------------------------------------------


n_rows_per_iter_usermap = 5000000


n_rows = 448411843 # 448,411,842 without header
n_chunks = 10
n_rows_per_iter = ceiling(n_rows/n_chunks)

for(i in 1:n_chunks){
  start_time_chunk = Sys.time()
  cat("Chunk ", i, " starting at ", as.character(start_time_chunk), "\n", sep = "")
  
  
  t1 = Sys.time()
  skip_chunk_i = (i-1)*n_rows_per_iter + 1
  cat("\tStarting to read from line ", scales::comma(skip_chunk_i), "...", sep = "")
  chunk = fread(
    relation_file, 
    colClasses = list(character = 1:2), 
    skip = skip_chunk_i,
    nrows = n_rows_per_iter,
    header = F
  ) %>% 
    set_names(c("A", "B"))
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("Successfully read", scales::comma(nrow(chunk)), "lines in chunk", i, ". Elapsed time reading:", minutes_passed, "minutes.\n")
  
  # Empty dataframe
  matched_A = head(chunk) %>% 
    select(A) %>%
    mutate(row_ix = 1:nrow(.)) %>% 
    slice(0)
  
  unmatched_A = chunk %>% 
    select(A) %>% 
    mutate(row_ix = 1:nrow(.))
  
  # Empty dataframe
  matched_B = head(chunk) %>% 
    select(B) %>%
    mutate(row_ix = 1:nrow(.)) %>% 
    slice(0)
  
  unmatched_B = chunk %>% 
    select(B) %>% 
    mutate(row_ix = 1:nrow(.))
  
  # rm(chunk) # Maybe not the best idea
  
  flag_while = T
  k = 1
  while(flag_while){
    
    user_ids_chunk = try(
      fread(
        file = here(paste0(relation_chunks_folder, "user_id_mapping.csv")),
        colClasses = list(character = 1, integer = 2),
        skip = (k-1)*n_rows_per_iter_usermap + 1,
        nrows = n_rows_per_iter_usermap,
        header = F
      ) %>% 
        set_names(c("user", "new_id")),
      silent = T)
    
    if(class(user_ids_chunk) == "try-error"){
      # If there was an error it is because the code already read all lines in the file
      break
    } else{
      cat("\tMatching chunk", k, "of user IDs\n")
      
      # 30 seconds
      unmatched_A = unmatched_A %>% 
        left_join(user_ids_chunk, by = c("A" = "user")) %>% 
        set_names(c("A", "row_ix", "new_A"))
      
      NA_ix_A = which(is.na(unmatched_A$new_A))
      not_NA_ix_A = setdiff(1:nrow(unmatched_A), NA_ix_A)
      
      matched_A = matched_A %>% 
        bind_rows(
          unmatched_A %>% 
            slice(not_NA_ix_A)
        )
      
      unmatched_A = unmatched_A %>% 
        select(A, row_ix) %>% 
        slice(NA_ix_A)
      
      
      
      
      
      unmatched_B = unmatched_B %>% 
        left_join(user_ids_chunk, by = c("B" = "user")) %>% 
        set_names(c("B", "row_ix", "new_B"))
      
      NA_ix_B = which(is.na(unmatched_B$new_B))
      not_NA_ix_B = setdiff(1:nrow(unmatched_B), NA_ix_B)
      
      matched_B = matched_B %>% 
        bind_rows(
          unmatched_B %>% 
            slice(not_NA_ix_B)
        )
      
      unmatched_B = unmatched_B %>% 
        select(B, row_ix) %>% 
        slice(NA_ix_B)
      
      
    } # End else
    
    k = k + 1
  }
  
  cat("\tJoining matched dataframes...")
  chunk_write = matched_A %>% 
    full_join(matched_B, by = "row_ix") %>% 
    arrange(row_ix) %>%  # To preserve the same order as original file
    select(new_A, new_B) 
  cat("Done.\n")
  
  fwrite(chunk_write, file = here("temp/relation_newids.csv"), append = T)
  
  end_time_chunk = Sys.time()
  minutes_passed_chunk = round(as.numeric(difftime(end_time_chunk, start_time_chunk, units = "mins")), 2)
  cat("\tElapsed time in chunk: ", minutes_passed_chunk, " minutes.\n\n\n", sep = "")
  
}





















(t1 = Sys.time())
user_ids = fread(
  file = here(paste0(relation_chunks_folder, "user_id_mapping.csv")),
  colClasses = list(character = 1, integer = 2),
  header = T
)
(t2 = Sys.time())
t2 - t1


n_rows = 448411843 # 448,411,842 without header
n_chunks = 10
n_rows_per_iter = ceiling(n_rows/n_chunks)

for(i in 1:n_chunks){
  start_time_chunk = Sys.time()
  cat("Chunk ", i, " starting at ", as.character(start_time_chunk), "\n", sep = "")
  
  
  t1 = Sys.time()
  skip_chunk_i = (i-1)*n_rows_per_iter + 1
  cat("\tStarting to read from line ", scales::comma(skip_chunk_i), "...", sep = "")
  chunk = fread(
    relation_file, 
    colClasses = list(character = 1:2), 
    skip = skip_chunk_i,
    nrows = n_rows_per_iter,
    header = F
  ) 
  
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("Successfully read", scales::comma(nrow(chunk)), "lines in chunk", i, ". Elapsed time reading:", minutes_passed, "minutes.\n")
  
  chunk = chunk %>% 
    left_join(
      user_ids,
      by = c("V1" = "user")
    ) %>% 
    left_join(
      user_ids,
      by = c("V2" = "user")
    )
  
  fwrite(chunk, file = here("temp/relation_newids.csv"), append = T)
  
  end_time_chunk = Sys.time()
  minutes_passed_chunk = round(as.numeric(difftime(end_time_chunk, start_time_chunk, units = "mins")), 2)
  cat("\tElapsed time in chunk: ", minutes_passed_chunk, " minutes.\n\n\n", sep = "")
  
}
