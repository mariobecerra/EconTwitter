library(tidyverse)
library(fst)
library(here)


# 12 minutes when it was guessing what type of column each was
(t1 = Sys.time())
relation = read_csv(
  here("temp/relation_newids.csv"), 
  col_names = c("A", "B"),
  col_types =  cols(A = col_integer(), B = col_integer()), 
  skip = 1
)
(t2 = Sys.time())
t2 - t1
object.size(relation)
# relation takes up around 3.4 GB of RAM
(t3 = Sys.time())
t3 - t2

relation

write_fst(
  relation, 
  here("temp/relation_newids.fst"),
  compress = 0, 
  uniform_encoding = TRUE)

