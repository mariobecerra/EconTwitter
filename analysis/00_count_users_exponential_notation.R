# First ran in terminal: grep e data/relation.csv > temp/relation_exp_rows.csv
# Takes a couple of minutes but not too long

library(data.table)
library(here)

exp_rows = fread(here("temp/relation_exp_rows.csv"),  colClasses = list(character = 1:2), header = F)

unique_A_exp = unique(exp_rows$V1)
length(unique_A_exp) # 8,639,123

unique_B_exp = unique(exp_rows$V2)
length(unique_B_exp) # 1,801,386

unique_exp = unique(c(unique_A_exp, unique_B_exp))
length(unique_exp) # 10,147,666

unique_exp_final = grep("e", unique_exp, value = T)
length(unique_exp_final) # 6,736,860
