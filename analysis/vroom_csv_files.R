library(tidyverse)
library(vroom)
library(here)

### Code to read each of the four files using vroom.


# 49,537,813 lines according to wc -l and 19 columns
# 12 GB uncompressed csv file
# This code says 42,815,594 rows and 18 columns
# Mac's activity monitor says the rsession that read only this file takes up 6 GB of RAM
# Takes around 4 minutes to read and index
data <- vroom(
  # file = here("data/data.csv.zip"), # Failed with the zip file. Maybe file is corrupted or maybe because of skip = 1
  file = here("data/data.csv"),
  delim = ",", 
  skip = 1,
  col_select = -row_id,
  col_names = c(
    "row_id",
    "id",
    "id_str",
    "name",
    "screen_name",
    "location",
    "description",
    "url",
    "protected",
    "verified",
    "followers_count",
    "friends_count",
    "listed_count",
    "favourites_count",
    "statuses_count",
    "created_at",
    "profile_banner_url",
    "profile_image_url_https",
    "default_profile"
  ),
  col_types = list(
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_double(),
    col_double(),
    col_double(),
    col_double(),
    col_double(),
    col_character(),
    col_character(),
    col_character(),
    col_character())
)



# data_seeds = readRDS(here("data/data_seeds.rds"))
# 4,313,880 x 24
# 1.3 GB uncompressed csv
data_seeds <- vroom(
  file = here("data/data_seeds.csv.zip"), 
  delim = ",", 
  col_types = list(
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_character(),
    col_double(),
    col_double(),
    col_double(),
    col_character(),
    col_character(),
    col_character(),
    col_logical(),
    col_logical(),
    col_integer(),
    col_integer(),
    col_integer(),
    col_integer(),
    col_integer()
  )
)



# 9.5 GB uncompressed csv
# 2 columns, 448,411,842 rows
relation = vroom(
  file = here("data/relation.csv"),
  delim = ",", 
  col_types = list(
    col_character(),
    col_character()
  )
)




repec_economists = readRDS(here("data/repec_economists.rds"))
# repec_economists = read_csv(here("data/repec_economists.csv.zip"))






