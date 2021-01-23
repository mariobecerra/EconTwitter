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


# Indices of rows with NAs, i.e., users for which n_followback hasn't been computed.
# This is because the for loop at the end of the file iterates over all users and creates checkpoints. This way, indices_to_do knows where to start.
indices_to_do = which(is.na(info_users_A$n_followback))


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
# For users with only 2 followers, it did 1,372,454 per hour. Computation: From user 29,890,101 to 30,452,807 it took 24.6 minutes.
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









