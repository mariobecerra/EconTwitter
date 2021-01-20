# Copmutes indices of where each ID of column A is in relation_newids.csv and relation_newids.fst.

library(fst)
library(data.table)
library(tidyverse)
library(here)

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



# Attempt 5
# Whole process takes around half an hour

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
  write_fst(indices_chunk_tbl, here(paste0("temp/indices_A_chunk_", sprintf("%02d", i), ".fst")), compress = 0)
  cat("Done\n")
  
  
  # if(i %% 10 == 1) {
  #   time_now = as.character(Sys.time())
  #   cat("\tSaving checkpoint...")
  #   saveRDS(indices_A, here("temp/temp_indices_A_checkpoint.rds"), compress = F)
  #   cat("Done\n")
  #   gc()
  # }
  
  out = tibble(
    user = names_indices_chunk_integer,
    chunk = i
  )
  
  
  t2 = Sys.time()
  minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
  cat("\tElapsed minutes in chunk:", minutes_passed, "\n\n")
  
  return(out)
  
}) %>% 
  bind_rows() %>% 
  arrange(user)


Sys.time()
write_fst(chunk_index_A, here("temp/chunk_index_A.fst"), compress = 0)
Sys.time()





# # Attempt 4
# 
# n_rows = nrow(relation_A)
# n_chunks = 20
# n_rows_per_iter = ceiling(n_rows/n_chunks)
# 
# indices_A = list()
# 
# for(i in 1:n_chunks){
#   t1 = Sys.time()
#   init = (i-1)*n_rows_per_iter + 1
#   end =  (i)*n_rows_per_iter
#   
#   if(end > n_rows){
#     end = n_rows
#   }
#   
#   cat(
#     "Doing chunk", i, "of", n_chunks, "from row", scales::comma(init), "to", scales::comma(end), "(",
#     as.character(t1), ")\n")
#   
#   cat("\tComputing indices...")
#   indices_chunk = split(init:end, relation_A$A[init:end])
#   cat("Done.\n")
#   
#   cat("\tAppending...")
#   indices_A = append(indices_A, indices_chunk)
#   cat("Indices appended.\n")
#   
#   t2 = Sys.time()
#   minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
#   cat("\tElapsed minutes in chunk:", minutes_passed, "\n\n")
#   
#   # if(i %% 10 == 1) {
#   #   time_now = as.character(Sys.time())
#   #   cat("\tSaving checkpoint...")
#   #   saveRDS(indices_A, here("temp/temp_indices_A_checkpoint.rds"), compress = F)
#   #   cat("Done\n")
#   #   gc()
#   # }
#   
# }
# 
# Sys.time()
# saveRDS(indices_A, here("temp/indices_A.rds"), compress = F)
# Sys.time()




# # Attempt 3
# indices_A = data.frame(
#   A = rep(NA_integer_, nrow(relation_A)),
#   row = rep(NA_integer_, nrow(relation_A))
# )
# 
# unique_users_A = unique(relation_A$A)
# n_users = length(unique_users_A)
# 
# checkpoint_user = 100000
# 
# row_pointer = 1L
# for(i in seq_along(unique_users_A)){
#   
#   if(i %% checkpoint_user == 1) {
#     time_now = as.character(Sys.time())
#     cat("User", scales::comma(i), "of", scales::comma(n_users), "(", time_now, ")\n")
#     cat("\tSaving checkpoint...")
#     write_fst(indices_A, here("temp/temp_indices_A_checkpoint.fst"), compress = 0)
#     # saveRDS(indices_A, here("temp/temp_indices_A_checkpoint.rds"), compress = F)
#     cat("Done\n")
#   }
#   
#   if(i %% 10 == 1) {
#     time_now = as.character(Sys.time())
#     cat("User", scales::comma(i), "of", scales::comma(n_users), "(", time_now, ")\n")
#   }
#   
#   user = unique_users_A[i]
#   
#   indices_user = which(relation_A$A == user)
#   indices_A$row[row_pointer:(row_pointer + length(indices_user) - 1)] = indices_user
#   indices_A$A[row_pointer:(row_pointer + length(indices_user) - 1)] = user
#   row_pointer = row_pointer + length(indices_user)
# }
# 
# write_fst(indices_A, here("temp/temp_indices_A.fst"), compress = 50)
# fwrite(indices_A, file = here("temp/indices_A.csv"))







# # Attempt 2
# 
# append_lists_using_names = function(list_1, list_2, verbose = 0){
#   # Appends two lists such that if there are elements in each list that are named the same, the appended list combines the vectors in the elements that are called the same. The elements in the list must be vectors, otherwise the result might not be what is expected.
#   # Examples:
#   # list_1 = list(
#   #   a = 1:5,
#   #   b = 2:5,
#   #   c = 6:10,
#   #   e = 1:6
#   # )
#   #
#   #
#   # list_2 = list(
#   #   b = 6:9,
#   #   c = 11:15,
#   #   d = 9:11
#   # )
#   #
#   # list_3 = list(
#   #   f = 5:7,
#   #   g = 9:10
#   # )
#   #
#   # append_lists_using_names(list_1, list_2)
#   # append_lists_using_names(list_1, list_3)
# 
#   intersection = intersect(names(list_1), names(list_2))
#   length_intersection = length(intersection)
#   indices_1 = match(intersection, names(list_1))
#   indices_2 = match(intersection, names(list_2))
# 
#   if(verbose) cat("\n\t", scales::comma(length_intersection), "elements in common in both lists\n")
# 
#   if(length_intersection == 0){
#     out_list = append(list_1, list_2)
#   } else{
# 
#     # https://stackoverflow.com/questions/18538977/combine-merge-lists-by-elements-names
#     keys <- unique(c(names(list_1), names(list_2)))
#     out_list = setNames(mapply(c, list_1[keys], list_2[keys]), keys)
# 
#     # # Much much slower:
#     # out_list = list()
#     #
#     # for(i in seq_along(intersection)){
#     #   appended_vector = c(list_1[[indices_1[i]]], list_2[[indices_2[i]]])
#     #   out_list = append(out_list, list(appended_vector))
#     # }
#     #
#     # names(out_list) = intersection
#     #
#     # unmatched_indices_1 = setdiff(1:length(list_1), indices_1)
#     # unmatched_indices_2 = setdiff(1:length(list_2), indices_2)
#     #
#     # out_list = append(out_list, list_1[unmatched_indices_1])
#     # out_list = append(out_list, list_2[unmatched_indices_2])
# 
#   }
# 
# 
#   return(out_list)
# 
# }
# 
# 
# 
# 
# n_rows = nrow(relation_A)
# n_chunks = 100
# n_rows_per_iter = ceiling(n_rows/n_chunks)
# 
# indices_A = list()
# 
# for(i in 1:n_chunks){
#   t1 = Sys.time()
#   init = (i-1)*n_rows_per_iter + 1
#   end =  (i)*n_rows_per_iter
# 
#   if(end > n_rows){
#     end = n_rows
#   }
#   
#   cat(
#     "Doing chunk", i, "of", n_chunks, "from row", scales::comma(init), "to", scales::comma(end), "(",
#     as.character(t1), ")\n")
# 
#   cat("\tComputing indices...")
#   indices_chunk = split(init:end, relation_A$A[init:end])
#   cat("Done.\n")
# 
#   cat("\tAppending...")
#   indices_A = append_lists_using_names(indices_A, indices_chunk, verbose = 1)
#   cat("\tIndices appended.\n")
# 
#   t2 = Sys.time()
#   minutes_passed = round(as.numeric(difftime(t2, t1, units = "mins")), 2)
#   cat("\tElapsed minutes in chunk:", minutes_passed, "\n\n")
#   
#     if(i %% 10 == 1) {
#       time_now = as.character(Sys.time())
#       cat("\tSaving checkpoint...")
#       saveRDS(indices_A, here("temp/temp_indices_A_checkpoint.rds"), compress = F)
#       cat("Done\n")
#       gc()
#     }
# 
# }
# 
# Sys.time()
# saveRDS(indices_A, here("temp/indices_A.rds"), compress = F)
# Sys.time()







# # Attempt 1
# (t3 = Sys.time())
# indices_A = split(1:nrow(relation_A), relation_A$A)
# (t4 = Sys.time())
# t4 - t3

# indices_B = split(1:nrow(relation), relation$B)
# (t5 = Sys.time())
# t5 - t4


































