# reads relation.csv in chunks and saves the counts of unique users in each chunk.
# It also indexes where each user is located (but in the end I modified the code, so this is not used because it is not useful.)

# Setup -------------------------------------------------------------------

library(tidyverse)
# library(disk.frame)
library(data.table)
library(fst)
library(here)

# setup_disk.frame()
# 
# # this will allow unlimited amount of data to be passed from worker to worker
# options(future.globals.maxSize = Inf)

# Read data ---------------------------------------------------------------

# disk_frame_folder = here("temp/relation_diskframe/")
# relation_df = disk.frame(disk_frame_folder)
# diskframe_files = list.files(disk_frame_folder)


# Before reading the file, we have to first make sure file exists and/or isn't compressed.
# This has to be done manually, it is not checked in this file.
relation_file = here("data/relation.csv")



# Code --------------------------------------------------------------------

n_rows = 448411843 # 448,411,842 without header
n_chunks = 10
n_rows_per_iter = ceiling(n_rows/n_chunks)

out_folder = "temp/relation_chunks/"
dir.create(here(out_folder))

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
  
  t1 = Sys.time()
  cat("\tComputing indices for column A...")
  index_A_list = split(1:nrow(chunk), chunk$V1) # Around 2 minutes
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("Done. Elapsed time:", minutes_passed, "minutes.\n")
  
  
  t1 = Sys.time()
  cat("\tCreating count tibble for column A...")
  n_followers_A = sapply(index_A_list, length) # Around 30 seconds
  
  # This tibble has each unique user in the A column, its number of followers, and in which position of index_A_list it is
  counts_A = tibble(
    user = names(n_followers_A),
    n_followers = n_followers_A,
    ix_index_list = 1:length(n_followers_A)
  ) %>% 
    arrange(desc(n_followers))
  
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("Done. Elapsed time:", minutes_passed, "minutes.\n")
  
  # cat("\tSaving RDS for column A...")
  # saveRDS(
  #   index_A_list, 
  #   here(paste0(out_folder, "index_A_list_chunk_", sprintf("%02d", i), ".rds")),
  #   compress = F
  # )
  # cat("Done.\n")
  
  # cat("\tSaving csv for column A...")
  fwrite(
    counts_A,
    file = here(paste0(out_folder, "counts_A_chunk_", sprintf("%02d", i), ".csv"))
  )
  cat("CSV file for column A written.\n")
  
  
  
  cat("\tComputing indices for column B...")
  index_B_list = split(1:nrow(chunk), chunk$V2) # Around 3 minutes
  cat("Done.\n")
  
  
  t1 = Sys.time()
  cat("\tCreating count tibble for column B...")
  n_appearances_B = sapply(index_B_list, length) # Around 30 seconds
  
  # This tibble has each unique user in the B column, its number of appearances, and in which position of index_B_list it is
  counts_B = tibble(
    user = names(n_appearances_B),
    n_appearances = n_appearances_B,
    ix_index_list = 1:length(n_appearances_B)
  ) %>% 
    arrange(desc(n_appearances))
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("Done. Elapsed time:", minutes_passed, "minutes.\n")
  
  
  # cat("\tSaving RDS for column B...")
  # saveRDS(
  #   index_B_list, 
  #   here(paste0(out_folder, "index_B_list_chunk_", sprintf("%02d", i), ".rds")),
  #   compress = F
  # )
  # cat("Done.\n")
  
  # cat("\tSaving csv for column B...")
  fwrite(
    counts_B,
    file = here(paste0(out_folder, "counts_B_chunk_", sprintf("%02d", i), ".csv"))
  )
  cat("CSV file for column B written.\n")
  
  end_time_chunk = Sys.time()
  minutes_passed_chunk = round(as.numeric(difftime(end_time_chunk, start_time_chunk, units = "mins")), 2)
  cat("\tElapsed time in chunk: ", minutes_passed_chunk, " minutes.\n\n\n", sep = "")
  
}





















# summary_followers = lapply(seq_along(diskframe_files), function(file_index){
#   file_name = paste0(disk_frame_folder, diskframe_files[file_index])
#   # Around 0.5 minutes to read each chunk
#   # Around 36 or 37 million rows each chunk
#   cat("Reading file ", file_index, " of ", length(diskframe_files), " (", file_name, ")...", sep = "")
#   t1 = Sys.time()
#   chunk = read.fst(file_name) %>% 
#     as_tibble()
#   t2 = Sys.time()
#   minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
#   cat("Successfully read. Elapsed time:", minutes_passed, "minutes.\n")
#   
#   
#   t1 = Sys.time()
#   cat("\tComputing indices (", as.character(t1), ")...", sep = "")
#   index_list = split(1:nrow(chunk), chunk$A)
#   t2 = Sys.time()
#   minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
#   cat("Done. Elapsed time:", minutes_passed, "minutes.\n")
#   
#   
#   t1 = Sys.time()
#   cat("\tCreating dataframe for indices (", as.character(t1), ")...", sep = "")
#   # 1 hour and 20 minutes passed in chunk 1 and it didn't finish
#   # Maybe shouldn't arrange??
#   n_users_chunk = length(index_list)
#   summary_chunk = lapply(seq_along(index_list), function(i){
#     
#     if(i %% floor(n_users_chunk/10) == 0) cat("User", i, "of", n_users_chunk, "\n")
#     
#     n_followers = length(index_list[[i]])
#     ix_init = index_list[[i]][1]
#     ix_end = index_list[[i]][n_followers]
#     
#     out = tibble(
#       user = names(index_list)[i],
#       n_followers = n_followers,
#       ix_init = ix_init,
#       ix_end = ix_end
#     )
#     
#     return(out)
#     
#   }) %>% 
#     bind_rows() %>% 
#     arrange(ix_init)
#   
#   t2 = Sys.time()
#   minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
#   cat("Done. Elapsed time:", minutes_passed, "minutes.\n\n")
#   
#   return(
#     summary_chunk %>% 
#       mutate(chunk = file_index)
#   )
#   
# }) %>% 
#   bind_rows()
# 
# 
# 
# # summary_followers = relation_df %>% 
# #   chunk_group_by(A) %>% 
# #   chunk_summarize(
# #     n_followers = n()
# #   ) %>% 
# #   collect()
#   
# 
# relation_head = relation_df %>% 
#   head(1000000) %>% 
#   collect() %>% 
#   as_tibble()
# 
# 
# relation_head %>% 
#   group_by(A) %>% 
#   group_map(~ head(.x, 2))
# 
# 
# 
# 
# lapply(seq_along(diskframe_files), function(file_index){
#   file_name = paste0(disk_frame_folder, diskframe_files[file_index])
#   # Around 2 minutes to read each chunk
#   # Around 36 or 37 million rows each chunk
#   chunk = read.fst(file_name) %>% 
#     as_tibble()
#   
#   chunk_head = head(chunk, 100000)
#   
#   chunk_head %>% 
#     head(5000) %>% 
#     group_by(A) %>% 
#     group_map(~{
#       for(i in 1:nrow(.x)){
#         follower_i = .x$B[[i]]
#         
#         
#         
#       }
#       head(.x, 3)
#     }, .keep = T)
# 
#   
#   })
# 
# 











