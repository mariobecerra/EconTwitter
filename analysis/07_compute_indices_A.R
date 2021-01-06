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


indices_A = data.frame(
  A = rep(NA_integer_, nrow(relation_A)),
  row = rep(NA_integer_, nrow(relation_A))
)

unique_users_A = unique(relation_A$A)
n_users = length(unique_users_A)


row_pointer = 1L
for(i in seq_along(unique_users_A)){
  
  if(i %% floor(i/10000) == 0) {
    time_now = as.character(Sys.time())
    cat("User", scales::comma(i), "of", scales::comma(n_users), "(", time_now, ")\n")
    saveRDS(indices_A, here("temp/temp_indices_A_checkpoint.rds"), compress = F)
  }
  
  user = unique_users_A[i]
  
  indices_user = which(relation_A$A == user)
  indices_A$row[row_pointer:(row_pointer + length(indices_user) - 1)] = indices_user
  indices_A$A[row_pointer:(row_pointer + length(indices_user) - 1)] = user
  row_pointer = row_pointer + length(indices_user)
}

fwrite(indices_A, file = here("temp/indices_A_checkpoint.csv"))








rm(indices_A)
rm(unique_users_A)
rm(relation_A)
gc()


relation_B = read_fst(
  here("temp/relation_newids.fst"),
  columns = "B") %>% 
  as_tibble()



indices_B = data.frame(
  B = rep(NA_integer_, nrow(relation_B)),
  row = rep(NA_integer_, nrow(relation_B))
)

unique_users_B = unique(relation_B$B)
n_users = length(unique_users_B)


row_pointer = 1L
for(i in seq_along(unique_users_B)){
  
  if(i %% floor(i/10000) == 0) {
    time_now = as.character(Sys.time())
    cat("User", scales::comma(i), "of", scales::comma(n_users), "(", time_now, ")\n")
    saveRDS(indices_B, here("temp/temp_indices_B_checkpoint.rds"), compress = F)
  }
  
  user = unique_users_B[i]
  
  indices_user = which(relation_B$B == user)
  indices_B$row[row_pointer:(row_pointer + length(indices_user) - 1)] = indices_user
  indices_B$B[row_pointer:(row_pointer + length(indices_user) - 1)] = user
  row_pointer = row_pointer + length(indices_user)
}

fwrite(indices_B, file = here("temp/indices_B_checkpoint.csv"))






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
#   cat("Doing chunk", i, "from row", scales::comma(init), "to", scales::comma(end), "\n")
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
# }




# (t3 = Sys.time())
# indices_A = split(1:nrow(relation_A), relation_A$A)
# (t4 = Sys.time())
# t4 - t3

# indices_B = split(1:nrow(relation), relation$B)
# (t5 = Sys.time())
# t5 - t4







