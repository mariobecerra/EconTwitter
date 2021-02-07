# Reads relation_newids.csv file and saves it in fst format with no compression. When reading, it reads it as a data.table with keys, meaning that the rows are ordered; first by column A and then by column B.
# The whole script takes aroung 5 minutes to run.


cat("Script starts at", as.character(Sys.time()), "\n")

library(tidyverse)
library(data.table)
library(fst)
library(here)



(t1 = Sys.time())
# 2.3 minutes to read
user_mapping = fread(
  here("temp/relation_chunks/user_id_mapping.csv"),
  # nrows = 1000000,
  colClasses = c("character", "integer")
)
(t2 = Sys.time())
t2 - t1


write_fst(
  user_mapping, 
  here("temp/user_id_mapping.fst"),
  compress = 50, 
  uniform_encoding = TRUE)

rm(user_mapping)


# 12 minutes when it was guessing what type of column each was
# (t1 = Sys.time())
# relation = read_csv(
#   here("temp/relation_newids.csv"), 
#   col_names = c("A", "B"),
#   col_types =  cols(A = col_integer(), B = col_integer()), 
#   skip = 1
# )
# (t2 = Sys.time())
# t2 - t1
# object.size(relation)
# # relation takes up around 3.4 GB of RAM
# (t3 = Sys.time())
# t3 - t2




(t1 = Sys.time())
# 2 minutes
relation = fread(
  here("temp/relation_newids.csv"), 
  col.names = c("A", "B"),
  colClasses =  c("integer", "integer"), 
  skip = 1, 
  key = c("A", "B")
)
(t2 = Sys.time())
t2 - t1


# relation

write_fst(
  relation, 
  here("temp/relation_newids.fst"),
  compress = 0, 
  uniform_encoding = TRUE)

cat("Script ends at", as.character(Sys.time()), "\n")