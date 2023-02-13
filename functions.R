# functions used repeatedly across multiple scripts
# Camilo B.

# 9/9/22
library(remotes)
library(HSClientR)
library(tidyverse)
library(DBI)

t_res <- 5

# Function to compute the mode
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

# Function to plot an event given the ID and a SiteID
plot_ev_site <- function(id_ev, site_id){
  # id_ev = 334
  # site_id = 30
  site_data <- ev_data %>% filter(SiteID == site_id)
  i = which(site_data$EventID == id_ev)
  end_time = site_data$datetime[i] + (site_data$duration_min[i] * 60)
  
  s_title = paste0('Date = ', as.Date(site_data$datetime[i]), '\n', 
                   'Label = ', site_data$label[id_ev], '\n', 
                   'Volume (Gal) = ', round(site_data$volume_gal[i],0), '\n', 
                   'Duration (min) = ', round(site_data$duration_min[i],0), '\n', 
                   'Participant #: ', site_id, '\n',
                   'City = ', flume_pd$City[site_id])
  
  
  data_evid_all %>%
    filter(SiteID == site_id) %>%
    select(id, datetime, VolumeGal) %>%
    filter(id == id_ev) %>%
    group_modify(~ add_row(.x, .before=0)) %>%
    fill(datetime, .direction = "downup") %>%
    mutate(datetime = if_else(is.na(VolumeGal), datetime - 5, datetime)) %>%
    mutate(VolumeGal = replace_na(VolumeGal, 0)) %>% 
    group_modify(~ add_row(.x, .after = max(nrow(.x)))) %>% 
    fill(datetime, .direction = "downup") %>%
    mutate(datetime = if_else(is.na(VolumeGal), datetime + 5, datetime)) %>%
    mutate(VolumeGal = replace_na(VolumeGal, 0)) %>%
    #add_column(label = site_data$label[id_ev]) %>%
    mutate(tdiff = as.numeric(difftime(datetime, lag(datetime), units = "secs"))) %>%
    mutate(tdiff = replace_na(tdiff, 0)) %>%
    mutate(n = cumsum(tdiff)) %>%
    ggplot(aes(datetime, VolumeGal * (60/5))) + 
    geom_line() +
    labs(y = 'Flow Rate (GPM)', x = 'Time', title = 'Sample event', subtitle = s_title) -> p
  
  return(p)
  
}

# Fucntion to remove outliers from plot
calc_boxplot_stat <- function(x) {
  coef <- 1.5
  n <- sum(!is.na(x))
  # calculate quantiles
  stats <- quantile(x, probs = c(0.0, 0.25, 0.5, 0.75, 1.0))
  names(stats) <- c("ymin", "lower", "middle", "upper", "ymax")
  iqr <- diff(stats[c(2, 4)])
  # set whiskers
  outliers <- x < (stats[2] - coef * iqr) | x > (stats[4] + coef * iqr)
  if (any(outliers)) {
    stats[c(1, 5)] <- range(c(stats[2:4], x[!outliers]), na.rm = TRUE)
  }
  return(stats)
}

# Fucntion to control the number of breaks
equal_breaks <- function(n = 3, s = 0.05, r = 0,...){
  function(x){
    d <- s * diff(range(x)) / (1+2*s)
    seq = seq(min(x)+d, max(x)-d, length=n)
    if(seq[2]-seq[1] < 10^(-r)) seq else floor(seq/5)*5      #round(seq, r)
  }
}

# function to the basit water use statistics 
wateruse_stats <- function(site) {
  # df = get_wateruse_data(sites)
  # firstday_wu <- as.Date(df$datetime[df$VolumeGal > 0][1])
  
  rd_all_50 %>%
    filter(SiteID == site) %>%
    mutate(h = hour(datetime), d = day(datetime), date = as.Date(datetime)) -> data
  
  # hourly water use
  data %>%
    group_by(d, h) %>%
    summarise(hourly_use = sum(VolumeGal)) -> huse
  
  # rada data in GPM
  data %>%
    mutate(flowrate = VolumeGal * (60/5)) -> rdata
  
  # Daily water use
  data %>%
    group_by(date) %>%
    summarise(daily_use = sum(VolumeGal)) -> duse 
  
  return(tibble(as.data.frame(rbind(as.numeric(summary(huse$hourly_use)),
                                    as.numeric(summary(rdata$flowrate)),
                                    as.numeric(summary(duse$daily_use))))) %>%
           rename(min = 1, q1 = 2, median = 3, mean = 4, q3 = 5, max = 6) %>%
           add_column(variable = c('hourlyuse', 'rawdata_GPM', 'dailyuse')) %>%
           add_column(SiteID = site))
}  

