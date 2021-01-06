
# Setup -------------------------------------------------------------------

library(tidyverse)
library(scales)
library(here)

theme_set(theme_bw())

# Read data ---------------------------------------------------------------


Sys.time()
# Reads only the three columns we're interested in.
# Reading this takes around 7 minutes and uses around 4.5 GB of RAM.
data <- read_csv(
  file = here("data/data.csv"),
  skip = 1,
  col_names = c("id_str", "followers_count", "friends_count"),
  col_types = cols_only(
    col_skip(), # "row_id",
    col_skip(), # "id",
    col_character(), # "id_str",
    col_skip(), # "name",
    col_skip(), # "screen_name",
    col_skip(), # "location",
    col_skip(), # "description",
    col_skip(), # "url",
    col_skip(), # "protected",
    col_skip(), # "verified",
    col_double(), # "followers_count",
    col_double(), # "friends_count",
    col_skip(), # "listed_count",
    col_skip(), # "favourites_count",
    col_skip(), # "statuses_count",
    col_skip(), # "created_at",
    col_skip(), # "profile_banner_url",
    col_skip(), # "profile_image_url_https",
    col_skip() # "default_profile"
    )
)
Sys.time()



# Functions ---------------------------------------------------------------


create_hist_bins = function(x, n_bins = 30, range_min = NULL, range_max = NULL, min_x = NULL, max_x){
  # This function returns the counts of bins for a histogram
  # x: numeric vector
  # n_bins: Number of bins in the histogram
  # range_min: the minimum of the range we're interested in, e.g., the 5th percentile because we don't want to see outliers in the histogram.
  # range_max: the maximum of the range we're interested in, e.g., the 95th percentile because we don't want to see outliers in the histogram.
  # min_x: the minimum value of x. This can be an approximation, but the approximation must be strictly lower than the actual minimum value, otherwise the hist() function will throw an error. If this value is unknown, the function calculates it.
  # max_x: the maximum value of x. This can be an approximation, but the approximation must be strictly larger than the actual maximum value, otherwise the hist() function will throw an error. If this value is unknown, the function calculates it.
  
  # Returns a histogram() object
  
  # If the maximum and minimum of the vector are not known, they're computed here:
  if(is.null(min_x) | is.null(max_x)){
    five_num_x = fivenum(x, na.rm = T)
    min_x = five_num_x[1]
    max_x = five_num_x[5]
  }
  
  
  if(is.null(range_min)) range_min = min_x
  if(is.null(range_max)) range_max = max_x
  
  step_size = (range_max - range_min)/n_bins
  br = range_min + (0:n_bins)*step_size
  out = hist(x, plot = F, breaks = unique(c(min_x, br, max_x)))
  return(out)
  
}





ggplot_hist = function(hist_obj, range_min = NULL, range_max = NULL){
  # Plots the histogram. The hist_object must be an object of type histogram, just like the one that is returned by the create_hist_bins() function. 
  
  if(!inherits(hist_obj, "histogram")) stop("hist_object must be of type 'histogram'")
  
  if(is.null(range_min)){
    range_min = hist_obj$breaks[1]
  }
  
  if(is.null(range_max)){
    range_max = hist_obj$breaks[length(hist_obj$breaks)]
  }
  
  
  out_plot = data.frame(
    mids = hist_obj$mids,
    counts = hist_obj$counts
  ) %>% 
    filter(mids >=range_min, mids <= range_max) %>% 
    ggplot() +
    geom_bar(aes(mids, counts), stat = "identity") +
    theme_bw()
  
  return(out_plot)
}



# Analysis ----------------------------------------------------------------

Sys.time()
# 30 seconds
# 31,562,700 unique IDs
(n_unique_ids = length(unique(data$id_str)))
Sys.time()

Sys.time()
# 1 minute
summary_data = data %>% 
  select(-id_str) %>% 
  summarize_all(
    .funs = list(
      mean = ~mean(., na.rm = T), 
      min = ~min(., na.rm = T),
      p05 = ~as.numeric(quantile(., probs = 0.05, na.rm = T)),
      p25 = ~as.numeric(quantile(., probs = 0.25, na.rm = T)),
      p50 = ~median(., na.rm = T),
      p75 = ~as.numeric(quantile(., probs = 0.75, na.rm = T)),
      p95 = ~as.numeric(quantile(., probs = 0.95, na.rm = T)),
      max = ~max(., na.rm = T) 
      
    )
  )
Sys.time()


summary_data_tibble = summary_data %>% 
  t() %>% 
  as.data.frame() %>% 
  rownames_to_column(var = "statistic") %>% 
  arrange(statistic)

summary_data_tibble

write_csv(summary_data_tibble, here("temp/followers_friends_count_statistics.csv"))


# 10 seconds
followers_count_hist_bins = create_hist_bins(data$followers_count, n_bins = 30, range_min = 0, range_max = 5000, min_x = 0, max_x = 1.3e+18)
followers_histogram = ggplot_hist(followers_count_hist_bins, range_min = 0, range_max = 5000) +
  xlab("Followers count") +
  ggtitle("Followers count histogram") +
  scale_y_continuous(label = comma)
print(followers_histogram)
ggsave(plot = followers_histogram, filename = here("temp/followers_histogram.png"))


friends_count_hist_bins = create_hist_bins(data$friends_count, n_bins = 30, range_min = 0, range_max = 100, min_x = 0, max_x = 1.3e+18)
friends_histogram = ggplot_hist(friends_count_hist_bins, range_min = 0, range_max = 100) +
  xlab("Friends count") +
  ggtitle("Friends count histogram") +
  scale_y_continuous(label = comma)
print(friends_histogram)
ggsave(plot = friends_histogram, filename = here("temp/followers_histogram.png"))








