# Setup -------------------------------------------------------------------

# library(tidyverse)
library(disk.frame)
library(here)


setup_disk.frame()

# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)

# Convert csv to several files with disk.frame ----------------------------

diskframe_folder = here("temp/relation_diskframe/")
# dir.create(diskframe_folder)

# Whole process took 1.3 hours on a laptop with 2 cores
(t1 = Sys.time())
relation_df <- csv_to_disk.frame(
  here("data/relation.csv"),
  outdir = diskframe_folder,
  shardby = "A",
  in_chunk_size = 100000000,
  overwrite = T,
  colClasses = list(character = c("A", "B"))
)
(t2 = Sys.time())
t2 - t1



