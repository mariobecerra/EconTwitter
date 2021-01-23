# This script computes the number of followbacks for each user in column A of the relation table. This means that for each user, it looks for which of the users in the relation table follow them back.

library(tidyverse)
library(data.table)
library(fst)
library(here)

chunk_folder = here("temp/")


n_followback_A_filename = here("temp/n_followback_A.fst")
n_followers_A_filename = here("temp/n_followers_A.fst")


# n_followback_A_filename is the name of the file in which the information about the followbacks is being stored.
if(!file.exists(n_followback_A_filename)){
  # If the file does not exist, then it created a dataframe with three columns: user, n_followers, and n_followback.
  # The first two columns come from reading the n_followers_A_filename file. The third column is initiated with integer NAs all the way down.
  (t1 = Sys.time())
  info_users_A = read_fst(n_followers_A_filename) %>% 
    mutate(n_followback = rep(NA_integer_, nrow(.)))
  setDT(info_users_A)
  (t2 = Sys.time())
  t2 - t1
  
} else{
  # If the file exists, it's read here.
  (t1 = Sys.time())
  info_users_A = read_fst(n_followback_A_filename)
  setDT(info_users_A)
  (t2 = Sys.time())
  t2 - t1
  
}


# Find users with only one follower
indices_one_follower = which(info_users_A$n_followers == 1)

# Indices of rows with NAs, i.e., users for which n_followback hasn't been computed.
# This is because the for loop at the end of the file iterates over all users and creates checkpoints. This way, indices_to_do knows where to start.
# It also ignores the users with only one follower.
# indices_to_do = which(is.na(info_users_A$n_followback))
indices_to_do = setdiff(which(is.na(info_users_A$n_followback)), indices_one_follower)


# Read file with the indexes for each user in relation file.
(t1 = Sys.time())
# Around 30 seconds to read
indices_A = read_fst(paste0(chunk_folder, "indices_A.fst"))
setDT(indices_A, key = "user")
(t2 = Sys.time())
t2 - t1


# Read relation file.
(t1 = Sys.time())
# Around 1 minute to read
relation = read_fst(paste0(chunk_folder, "relation_newids.fst")) 
# relation takes up around 3.4 GB of RAM
(t2 = Sys.time())
t2 - t1


gc()


# This for loop goes through the index vector indices_to_do. Each index in indices_to_do is a row (i.e. a user) of the info_users_A dataframe for which the number of followbacks has to be computed using the relation dataframe.
# It calculates around 130,154 users per hour.
# I calculated this by dividing the running hours on 2021-01-22 at 1 pm (13 hours at that moment), and the 1,692,000 processed users until that time.
# From user 1,692,000 to 2,610,100 it took 84 minutes, meaning that it did 655,714 users per hour. User 2,610,100 had 19 followers.
last_index_to_do = tail(indices_to_do, 1)
start_time = Sys.time()
for(i in indices_to_do){
  
  curr_time = Sys.time()
  total_elapsed_minutes = as.numeric(difftime(curr_time, start_time, units = "mins"))
  if(i %% 100 == 1){
    cat("Doing user", scales::comma(i), "of", scales::comma(last_index_to_do))
    cat("\n\tElapsed minutes so far:", total_elapsed_minutes, "\n\n")
  }
  
  # Current user ID.
  user_i = info_users_A$user[i]
  
  # Filter indices of user_i using data.table syntax (super fast because user is a key).
  indices_i = indices_A[.(user_i), index]
  
  # Compute number of followbacks:
  nf = sum(relation$B[indices_A[.(relation$B[indices_i]), index]] == user_i, na.rm = T)
  # Same as doing this:
  # followers_A = relation$B[indices_i]
  # IDK why it creates some NAs, but they seem to be fine
  # indices_followers_A = indices_A[.(followers_A), index]
  # followees_of_followers_of_A = relation$B[indices_followers_A]
  # nf = sum(followees_of_followers_of_A == user_i, na.rm = T)
  # But the one-liner is much faster because it doesn't have to allocate new memory space for the vectors.
  # What it does is first get the followers of the current user, i.e., user_i (followers_A). 
  # Then, it gets its indices to subset them (indices_followers_A).
  # Then, it finds the users that the followers of user_i follow (followees_of_followers_of_A).
  # Finally, it finds how many of the followees of the followers of user_i are user_i (followees_of_followers_of_A == user_i) and counts them (sum).
  
  # Update the information in the info_users_A dataframe. With the data.table syntax this is done by passing by reference, so it's much faster than doing it the usual way in R.
  info_users_A[i, `:=`(n_followback = nf)] 
  
  
  # Save checkpoint.
  # if(i %% 1000 == 1){
  if(i %% 10000 == 1){ # Originally it was every 1000, but when my computer ran out of battery and turned off and started from user 1,692,000; from which there are users with less than 29 followers, I changed it to checkpoint every 10,000 users.
    cat("Saving checkpoint...")
    write_fst(info_users_A, n_followback_A_filename)
    cat("Done.\n")
  }
  
  
}







indices_one_follower = which(info_users_A$n_followers == 1)


# users = info_users_A$user[indices_one_follower[1:1000000]]
users = info_users_A$user[indices_one_follower[2:10]]

indices_users = indices_A[.(users), index]
# followers_users = relation$B[indices_users]

users_follower = relation[indices_A[.(users), index],]
setDT(users_follower, key = c("A", "B"))


temp = relation[indices_users,] %>% 
  mutate(aux = 1L)
setDT(temp, key = "B")
indices_A[temp, on = "user", aux := i.aux, allow.cartesian=TRUE]

indices_followers_users = indices_A[aux == 1, index]
# indices_followers_users1 = indices_A[!is.na(aux), index] # same as before
# indices_followers_users0 = indices_A[.(followers_users), index] # Same as before
# indices_followers_users2 = temp %>%  # In theory should be same as before, but I ran out of RAM.
#   inner_join(indices_A)

# # Remove the created column
# indices_A[, aux:=NULL]
# We can actually remove the whole table now
rm(indices_A)


followers_of_followers = relation[indices_followers_users,] %>% 
  set_names(c("B", "A")) %>% 
  mutate(n_followback = 1L)
setDT(followers_of_followers, key = c("B", "A"))


users_follower[followers_of_followers, on = c("A", "B"), n_followback := i.n_followback]
users_follower[is.na(n_followback), n_followback := ifelse(is.na(n_followback), 0L, n_followback)]
# Same as doing this, but much faster (around 20x faster) because it's updated by reference
# users_follower = users_follower %>% 
#   left_join(followers_of_followers) %>% 
#   mutate(n_followback = ifelse(is.na(n_followback), 0L, n_followback))
# https://stackoverflow.com/questions/34598139/left-join-using-data-table
# If you want to add the b values of B to A, then it's best to join A with B and update A by reference as follows:
# A[B, on = 'a', bb := i.b]







