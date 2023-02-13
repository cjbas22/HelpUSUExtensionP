# Script to compute events from Flume data
# Camilo B.
# 01/29/22

# This script will take some time to run
# Loading all raw data: 15 min

# rm(list = ls()) # remove everything

library(remotes)
library(HSClientR)
library(tidyverse)
library(DBI)
library(lubridate)
library(tidymodels)

# Database connection
con <- dbConnect(RSQLite::SQLite(), "./Database/ProjectDatabase_RawData.db")

# List tables
dbListTables(con)

t_res <- 5 # remporal resolution, in seconds

# Flume Information
tbl(con, "FlumePropertyData") %>%
  collect() -> FlumePropertyData

# Sites Information
tbl(con, "Sites") %>%
  collect() -> Sites

# WaterCheckData Information
tbl(con, "WaterCheckData") %>%
  collect() -> WaterCheckData

# Load raw data for all sites
RawData_AllSites <- dbGetQuery(con, paste0("SELECT Datetime,VolumeGal,SiteID FROM WaterUse")) %>%
  tibble() %>%
  mutate(datetime = as_datetime(Datetime, tz = "US/Mountain"), .before = Datetime) %>%
  select(-Datetime)

# replace large values - not the best solution but implementing anything additional will take longer to run
RawData_AllSites %>%
  mutate(fr_gpm = VolumeGal * (60/t_res)) %>% # flow rate
  mutate(VolumeGal = if_else(fr_gpm > 60, 60/(60/t_res), VolumeGal)) -> rd_all # replace values larger than 60 - these are errors - with 60 

# This is the only instance where raw data will be loaded - to save time
# Let's save supplementary files: hourly, daily volumes
dir.create(file.path('AdditionalFiles')) # create folder

# Hourly
rd_all %>%
  group_by(SiteID, date = as.Date(datetime), hour = hour(datetime)) %>%
  mutate(HourlyVolume = sum(VolumeGal), n = n()) %>%
  slice(1) %>%
  ungroup() %>%
  group_by(SiteID) %>%
  filter(row_number() != 1 & row_number() != n())  %>% # remove the first and last hour per site (incomplete)
  select(SiteID, datetime, HourlyVolume, n) %>%
  write_csv('./AdditionalFiles/hourlywateruse.csv') -> hourly_values

# Daily values
hourly_values %>%
  select(-n) %>%
  group_by(SiteID, date = as.Date(datetime)) %>%
  summarise(DailyVolume = sum(HourlyVolume)) %>%
  group_by(SiteID) %>%
  filter(DailyVolume > 0) %>% # remove days wihtout water use
  filter(row_number() != 1 & row_number() != n()) %>% # remove the first and last day as these are incomplete
  write_csv('./AdditionalFiles/dailywateruse.csv') -> daily_values

# Weekly values
daily_values %>%
  mutate(week = isoweek(date)) %>%
  group_by(SiteID, week) %>%
  summarise(WeeklyVolume = sum(DailyVolume), n = n()) %>%
  # filter(n > 6) %>% # re
  write_csv('./AdditionalFiles/weeklywateruse.csv') -> weekly_values

# Event computation

# Remove 0 - not needed 
rd_all %>%
  filter(VolumeGal > 0) -> rd_all # raw data without 0 values

# add event ID data to all sites and remove events lasting less than 5 minutes - to save time
# Function to add event ID data to all sites and remove events that last less than 5 min
event_id_adder <- function(siteid){
  return(rd_all %>% 
           filter(SiteID == siteid) %>%
           mutate(tdiff = as.numeric(difftime(datetime, lag(datetime), units = "secs"))) %>%
           mutate(tdiff = replace_na(tdiff, 6)) %>% # replace the NA from the 1st row with a value > td so id start at 1
           mutate(i = case_when(tdiff > t_res ~ 1, tdiff <= t_res ~ 0)) %>% # assign an id to each event 
           mutate(id = cumsum(i)) %>%
           group_by(id) %>%
           filter(n() > ((5*60/t_res) - 1)) %>% # remove events that last less than 5 minutes
           mutate(id = cur_group_id())) # add id and save
}

data_evid_all <- map_df(unique(rd_all$SiteID), event_id_adder) # raw data with event ID

# # save CSV - if needed later
data_evid_all %>%
  write_csv('./AdditionalFiles/data_evid_all_larger5mintues.csv')

# Compute event features
dir.create(file.path('Events_AllSites')) # create folder

# function to compute events 
Events_Calculation <- function(siteid){
  # siteid = 1
  data_evid_all %>%
    filter(SiteID == siteid) -> data_evid
  
  # 2) Compute events- for events with more than 1 pulse - All
  data_evid %>%
    group_by(id) %>%
    mutate(duration_min = (as.numeric(difftime(last(datetime), first(datetime), units = "secs")) + t_res) / 60) %>% # duration in minutes (need to add tres because the 1st value includes wter use for tres)
    mutate(pulses_total = VolumeGal) %>% # values
    mutate(volume_gal = sum(pulses_total)) %>% # volume Gallons
    mutate(median_fr_GPM = median(pulses_total) * (60/t_res)) %>% # median flow rate in GPM
    mutate(average_fr_GPM = volume_gal/duration_min) %>%
    slice(1) %>%
    relocate(SiteID, .after = last_col()) %>%
    select(-pulses_total, -tdiff, -i, - VolumeGal) %>%
    select(-fr_gpm) %>%
    ungroup() -> ev
  
  filename <- paste0('./Events_AllSites/Events_Site_', sprintf("%03d",siteid), '_From_',as.Date(data_evid$datetime[1]), '_To_',
                     as.Date(data_evid$datetime[nrow(data_evid)]), '.csv')
  
  ev %>% 
    write_csv(filename)
  return(ev)
}

ev_data <- map_df(Sites$SiteID, Events_Calculation)

summary(ev_data$duration_min)
# End of Event computation

# Train Random Forest
rf_all <-  rand_forest(mode = "classification") %>%
  set_engine("randomForest") %>%
  fit(label ~ ., data = td) # train a model with all the labelled data

# Label Flume data
predict(rf_all, ev_data) %>%
  bind_cols(ev_data) %>%
  rename(label = 1) %>%
  select(-label, label) %>%
  arrange(SiteID, id) %>%
  rename(EventID = id) %>%
  select(-SiteID, SiteID) -> ev_data_labeled

# WaterCheck data
tbl(con, "WaterCheckData") %>%
  collect() %>%
  #select(SiteID, WaterCheckDate) %>%
  mutate(WaterCheckDate = as_datetime(WaterCheckDate, tz = "US/Mountain")) -> WaterCheckData


# Filter only irrigation events for participants
ev_data_labeled %>%
  filter(!(SiteID %in% c(42, 101, 102, 103, 104, 105))) %>% # site not in HP or Logan - not part of the study
  filter(!(SiteID %in% c(7, 30, 40, 56, 72))) %>% # these sites have data issues
  filter(SiteID %in% WaterCheckData$SiteID) %>%
  filter(label == 'irrigation') -> irr_ev_participants

# Number of sites in the study
length(unique(irr_ev_participants$SiteID))


# KS test at the daily level

# generate a daily file for testing
irr_ev_participants %>%
  mutate(month = month(datetime), day = day(datetime)) %>%
  mutate(fr_dur = average_fr_GPM * duration_min) %>% # FR times duration
  group_by(SiteID, month, day) %>%
  mutate(dvol = sum(volume_gal), dmin = sum(duration_min), n_eve = n(), fr = sum(fr_dur)/sum(duration_min)) %>%
  slice(1) %>%
  mutate(date = date(datetime), .before = EventID) %>%
  select(SiteID, date, month, day, dvol, dmin, n_eve, fr) %>%
  left_join(WaterCheckData %>% select(SiteID, WaterCheckDate)) %>%
  mutate(pre_post = case_when(date < WaterCheckDate ~ 'pre',
                              date == WaterCheckDate ~ 'wc',
                              date > WaterCheckDate ~ 'post'), .before = fr) %>%
  group_by(SiteID) %>% # removing the first day at each site as it is not complete
  mutate(d_lastirr = as.numeric(difftime(date, lag(date), units = 'days')), .before = pre_post) %>% # days since last irrigation event
  filter(row_number() != 1) -> df_daily

# Daas of data pre and post
table(df_daily$pre_post)

# keep tests within 4 weeks of water check and remove sites with less than 6 days of data
df_daily %>%
  mutate(days_since_wc = abs(as.numeric(difftime(date, WaterCheckDate, units = 'days'))), .before= month) %>% 
  filter(days_since_wc < 31) %>%
  group_by(SiteID, pre_post) %>%
  filter(pre_post != 'wc') %>%
  tally() %>%
  filter(n > 4) %>% # sites with at least 4 days of pre and post data - this is arbitrary 
  group_by(SiteID) %>%
  filter(n() > 1) -> sites_with_pre_post



# Remove sites that only have pre or post data - no test possible
df_daily %>%
  filter(SiteID %in% unique(sites_with_pre_post$SiteID)) %>% # remove no data for test
  mutate(days_since_wc = abs(as.numeric(difftime(date, WaterCheckDate, units = 'days'))), .before= month) %>% 
  filter(SiteID %in% unique(sites_with_pre_post$SiteID)) -> data_test

# this data set has: days since water check, daily volume, daily duration of irrigation events
# number of events in a day, days since last irrigation event, pre_post indicates if an event happened pre or post the water check.
data_test

# Observe one site - for learning before interview
site = 21
data_test %>%
  filter(SiteID == site) %>%  
  # print(n = 100) #%>%
  ggplot(aes(date, dvol, fill = pre_post)) + # volume
  # ggplot(aes(date, dmin, fill = pre_post)) + # daily m inutes - change variable to observ other things
  geom_col() +
  geom_vline(xintercept = as.Date(data_test$WaterCheckDate[data_test$SiteID == site][1]))

# Function to do KS test to any column variable
KS_test <- function(siteid, variable){
  # siteid = 68
  # variable = 'dvol'
  data_test %>% filter(SiteID == siteid) -> data
  wc_date <- data$WaterCheckDate[1]
  
  post <- pull(data[, which(names(data) == variable)])[data$date > wc_date]
  pre <- pull(data[, which(names(data) == variable)])[data$date < wc_date]
  
  if (length(pre) < 1 | length(post) < 1) {
    return(tibble(pvalue = NA, D = NA, SiteID = siteid, alternative = NA,
                  variable = variable,
                  test = NA))
  } else {
    test <- ks.test(pre, post)
    test_l <- ks.test(pre, post, alternative = "less")
    test_g <- ks.test(pre, post, alternative = "greater")
    return(
      tibble(SiteID = siteid,
             pvalue = c(test$p.value, test_l$p.value, test_g$p.value),
             D = c(as.numeric(test$statistic), as.numeric(test_l$statistic), as.numeric(test_l$statistic)),
             alternative = c(test$alternative, test_l$alternative, test_g$alternative),
             variable = variable,
             pre_n = length(pre),
             post_n = length(post),
             test = c("two.sided", "less", "greater")))
  }
}

# apply the function to all sites comparing within 31 days of water check

d_lastirr <- map_df(unique(df_daily$SiteID), KS_test, variable = 'd_lastirr') # days since last irrigation event
n_eve <- map_df(unique(df_daily$SiteID), KS_test, variable = 'n_eve') # number of events
dmin <- map_df(unique(df_daily$SiteID), KS_test, variable = 'dmin') # daily minutes
fr <- map_df(unique(df_daily$SiteID), KS_test, variable = 'fr') # flow rate
vol <- map_df(unique(df_daily$SiteID), KS_test, variable = 'dvol') # daily volume

# Now we need to create a table that summarises all the results
# Basically combining the 5 tests done in a single table that shows what
# each site changed and what they didn't change.
# e.g., site 3 increased volume but reduced days since last irrigation

# View test results - example
d_lastirr %>%
  filter(pvalue < 0.05) # significant change 
  
# function to plot CDF
cdf_builder <- function(siteid, variable){
  # siteid = 12
  # variable = 'dvol'
  labels_df = tibble(variable = c('d_lastirr', 'n_eve', 'dmin', 'fr', 'dvol'), label = c('Days since last irrigation event',
                                                                                         'Numer of events per day',
                                                                                         'Duration of irrigation events (min)',
                                                                                         'Average weighted flow rate  (GPM)',
                                                                                         'Daily volume used for irrigation events (gal)'))
  
  data_test %>% filter(SiteID == siteid) -> data
  wc_date <- data$WaterCheckDate[1]
  
  pre <- tibble(Pre_WaterCheck = pull(data[, which(names(data) == variable)])[data$date > wc_date]) %>%
    mutate(id = row_number())
  
  post <- tibble(Post_WaterCheck = pull(data[, which(names(data) == variable)])[data$date < wc_date]) %>%
    mutate(id = row_number())
  
  left_join(pre, post) %>%
    pivot_longer(-id) %>%
    ggplot(aes(value, color = name, linetype = name)) + 
    theme_light() +
    stat_ecdf() +
    labs(y = "ECDF", color = 'Period', linetype = 'Period', x = paste(labels_df$label[match(variable, labels_df$variable)])) +
    theme(legend.position ="top") 
  
} 


# example - use
cdf_builder(siteid = 1, variable = 'd_lastirr')
cdf_builder(siteid = 5, variable = 'd_lastirr')
cdf_builder(siteid = 6, variable = 'dvol')


# Analysis at the weekly level 
# Add the budget
kc = 0.8 # generalization - crop coefficient

daily_wd %>% # daily weather data
  mutate(week = isoweek(date)) %>%
  filter(year(date) == '2022') %>%
  group_by(week) %>%
  summarise(weekly_pcp_in = sum(precip), weekly_tmp_F = mean(airt_avg), weekly_eto_in = sum(eto)) %>%
  mutate(budget = kc * weekly_eto_in - weekly_pcp_in) %>%
  mutate(budget = if_else(budget < 0, 0 , budget)) -> weekly_wd # remove negative values
  # pivot_longer(-week) %>%
  # ggplot(aes(week, value, color = name)) + geom_line()

# Irrigable areas
colnames(WaterCheckData)
WaterCheckData %>%
  select(SiteID, TurfAreaft2, OtherIrrAreaft2) %>%
  mutate(IrrigableArea_ft2 = TurfAreaft2 + OtherIrrAreaft2) %>%
  select(SiteID, IrrigableArea_ft2) -> IrrAreas_All

# Weekly
data_test %>%
  #filter(SiteID == 21) %>%
  select(SiteID, date, dvol, n_eve, dmin, WaterCheckDate, pre_post) %>%
  mutate(week = isoweek(date), wc_week = isoweek(WaterCheckDate)) %>%
  group_by(week) %>%
  filter(row_number() != 1 & row_number() != n()) %>% # remove the first and last day as these are incomplete
  filter(week != wc_week) %>%
  ungroup() %>%
  select(SiteID, week, dvol, n_eve, dmin, pre_post) %>%
  group_by(SiteID, week, pre_post) %>%
  summarise(weekly_volume = sum(dvol), total_irrigation_minutes = sum(dmin), frequency = sum(n_eve)) %>%
  left_join(weekly_wd) %>% # add weather data and budget
  left_join(IrrAreas_All) %>%
  mutate(IrrigationVolume_in = (weekly_volume * 1000 * 0.133681 / IrrigableArea_ft2) / 12) %>% # volume from 10^3 gal to ft3. ft3/ft2. ft to inches
  mutate(VolumeOverBudget = IrrigationVolume_in - budget) %>% # volume of water applied above the budget
  mutate(VolumeOverBudget = if_else(VolumeOverBudget < 0, 0, VolumeOverBudget)) -> df_test_weekly # remove negative values
  
# this data has all the information needed for analisis / test
# Looking at volume over bidget (or overirrigation) we can see if behavior changed related to weather
df_test_weekly

