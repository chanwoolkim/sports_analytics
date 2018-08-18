library(pitchRx)
library(plyr)
library(dplyr)
library(tidyr)
library(RSQLite)
library(devtools)
library(plm)
library(lmtest)
library(clubSandwich)
library(reshape)

data_path <- "/Volumes/huizinga/MLB/Chanwool/Scraped Data/Pitch Data/"
save_path <- "/Volumes/huizinga/MLB/Chanwool/Diagnosis/"

year_list <- c(2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)

#Load all data ####
for (y in year_list) {
  assign(paste0("pitch_", y),
         read.csv(paste0(data_path, y, "_pitch.csv")))
}


#Check what happened to BadData_P ####
check_pitch_game <- function(df) {
   df <- df %>%
     group_by(gameday_link) %>%
     summarise(count = n(),
               bad_data = sum(BadData_P, na.rm=TRUE)) %>%
     ungroup %>%
     mutate(type = ifelse(count == bad_data, "All Pitches Bad",
                          ifelse(bad_data == 0, "All Pitches Good", "Mixed"))) %>%
     group_by(type) %>%
     summarise(count = n())
}

check_pitch_game_2008 <- check_pitch_game(pitch_2008) %>% transmute(type, count_2008 = count)
check_pitch_game_2009 <- check_pitch_game(pitch_2009) %>% transmute(type, count_2009 = count)
check_pitch_game_2010 <- check_pitch_game(pitch_2010) %>% transmute(type, count_2010 = count)
check_pitch_game_2011 <- check_pitch_game(pitch_2011) %>% transmute(type, count_2011 = count)
check_pitch_game_2012 <- check_pitch_game(pitch_2012) %>% transmute(type, count_2012 = count)
check_pitch_game_2013 <- check_pitch_game(pitch_2013) %>% transmute(type, count_2013 = count)
check_pitch_game_2014 <- check_pitch_game(pitch_2014) %>% transmute(type, count_2014 = count)
check_pitch_game_2015 <- check_pitch_game(pitch_2015) %>% transmute(type, count_2015 = count)
check_pitch_game_2016 <- check_pitch_game(pitch_2016) %>% transmute(type, count_2016 = count)
check_pitch_game_2017 <- check_pitch_game(pitch_2017) %>% transmute(type, count_2017 = count)

check_pitch_game <- full_join(check_pitch_game_2008, check_pitch_game_2009, by = "type") %>%
  full_join(check_pitch_game_2010, by = "type") %>%
  full_join(check_pitch_game_2011, by = "type") %>%
  full_join(check_pitch_game_2012, by = "type") %>%
  full_join(check_pitch_game_2013, by = "type") %>%
  full_join(check_pitch_game_2014, by = "type") %>%
  full_join(check_pitch_game_2015, by = "type") %>%
  full_join(check_pitch_game_2016, by = "type") %>%
  full_join(check_pitch_game_2017, by = "type") %>%
  t() %>%
  as.data.frame() %>%
  filter(V2 != "All Pitches Good") %>%
  transmute("Year" = year_list,
         "All Pitches Bad" = V1,
         "All Pitches Good" = V2,
         "Mixed" = V3)

write.csv(check_pitch_game, file=paste0(save_path, "check_pitch_game.csv"), row.names=FALSE)

check_pitch_atbat <- function(df) {
  df <- df %>%
    group_by(gameday_link, num) %>%
    summarise(count = n(),
              bad_data = sum(BadData_P, na.rm=TRUE)) %>%
    ungroup %>%
    mutate(type = ifelse(count == bad_data, "All Pitches Bad",
                         ifelse(bad_data == 0, "All Pitches Good", "Mixed"))) %>%
    group_by(type) %>%
    summarise(count = n())
}

check_pitch_atbat_2008 <- check_pitch_atbat(pitch_2008) %>% transmute(type, count_2008 = count)
check_pitch_atbat_2009 <- check_pitch_atbat(pitch_2009) %>% transmute(type, count_2009 = count)
check_pitch_atbat_2010 <- check_pitch_atbat(pitch_2010) %>% transmute(type, count_2010 = count)
check_pitch_atbat_2011 <- check_pitch_atbat(pitch_2011) %>% transmute(type, count_2011 = count)
check_pitch_atbat_2012 <- check_pitch_atbat(pitch_2012) %>% transmute(type, count_2012 = count)
check_pitch_atbat_2013 <- check_pitch_atbat(pitch_2013) %>% transmute(type, count_2013 = count)
check_pitch_atbat_2014 <- check_pitch_atbat(pitch_2014) %>% transmute(type, count_2014 = count)
check_pitch_atbat_2015 <- check_pitch_atbat(pitch_2015) %>% transmute(type, count_2015 = count)
check_pitch_atbat_2016 <- check_pitch_atbat(pitch_2016) %>% transmute(type, count_2016 = count)
check_pitch_atbat_2017 <- check_pitch_atbat(pitch_2017) %>% transmute(type, count_2017 = count)

check_pitch_atbat <- full_join(check_pitch_atbat_2008, check_pitch_atbat_2009, by = "type") %>%
  full_join(check_pitch_atbat_2010, by = "type") %>%
  full_join(check_pitch_atbat_2011, by = "type") %>%
  full_join(check_pitch_atbat_2012, by = "type") %>%
  full_join(check_pitch_atbat_2013, by = "type") %>%
  full_join(check_pitch_atbat_2014, by = "type") %>%
  full_join(check_pitch_atbat_2015, by = "type") %>%
  full_join(check_pitch_atbat_2016, by = "type") %>%
  full_join(check_pitch_atbat_2017, by = "type") %>%
  t() %>%
  as.data.frame() %>%
  filter(V2 != "All Pitches Good") %>%
  transmute("Year" = year_list,
            "All Pitches Bad" = V1,
            "All Pitches Good" = V2,
            "Mixed" = V3)

write.csv(check_pitch_atbat, file=paste0(save_path, "check_pitch_atbat.csv"), row.names=FALSE)


#Deprecated as sz_top and sz_bot check is not reliable ####
# check_pitch <- function(df) {
#   df <- df %>%
#     group_by(gameday_link) %>%
#     mutate(count = n()) %>%
#     filter(row_number() == 1) %>%
#     mutate(sz_na = !is.na(sz_top) & !is.na(sz_bot)) %>%
#     select(gameday_link, sz_na, count) %>%
#     ungroup
# }
# 
# check_pitch_2008_0 <- data.frame(pitch_2008_0 = unclass(summary(check_pitch(pitch_2008) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2009_0 <- data.frame(pitch_2009_0 = unclass(summary(check_pitch(pitch_2009) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2010_0 <- data.frame(pitch_2010_0 = unclass(summary(check_pitch(pitch_2010) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2011_0 <- data.frame(pitch_2011_0 = unclass(summary(check_pitch(pitch_2011) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2012_0 <- data.frame(pitch_2012_0 = unclass(summary(check_pitch(pitch_2012) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2013_0 <- data.frame(pitch_2013_0 = unclass(summary(check_pitch(pitch_2013) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2014_0 <- data.frame(pitch_2014_0 = unclass(summary(check_pitch(pitch_2014) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2015_0 <- data.frame(pitch_2015_0 = unclass(summary(check_pitch(pitch_2015) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2016_0 <- data.frame(pitch_2016_0 = unclass(summary(check_pitch(pitch_2016) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# check_pitch_2017_0 <- data.frame(pitch_2017_0 = unclass(summary(check_pitch(pitch_2017) %>%
#                                                                   filter(sz_na == FALSE) %>%
#                                                                   pull(count))))
# 
# check_pitch_2008_1 <- data.frame(pitch_2008_1 = unclass(summary(check_pitch(pitch_2008) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2009_1 <- data.frame(pitch_2009_1 = unclass(summary(check_pitch(pitch_2009) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2010_1 <- data.frame(pitch_2010_1 = unclass(summary(check_pitch(pitch_2010) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2011_1 <- data.frame(pitch_2011_1 = unclass(summary(check_pitch(pitch_2011) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2012_1 <- data.frame(pitch_2012_1 = unclass(summary(check_pitch(pitch_2012) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2013_1 <- data.frame(pitch_2013_1 = unclass(summary(check_pitch(pitch_2013) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2014_1 <- data.frame(pitch_2014_1 = unclass(summary(check_pitch(pitch_2014) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2015_1 <- data.frame(pitch_2015_1 = unclass(summary(check_pitch(pitch_2015) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2016_1 <- data.frame(pitch_2016_1 = unclass(summary(check_pitch(pitch_2016) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# check_pitch_2017_1 <- data.frame(pitch_2017_1 = unclass(summary(check_pitch(pitch_2017) %>%
#                                                                   filter(sz_na == TRUE) %>%
#                                                                   pull(count))))
# 
# check_pitch_all_0 <- data.frame(t(cbind(check_pitch_2008_0, check_pitch_2009_0,
#                                         check_pitch_2010_0, check_pitch_2011_0,
#                                         check_pitch_2012_0, check_pitch_2013_0,
#                                         check_pitch_2014_0, check_pitch_2015_0,
#                                         check_pitch_2016_0, check_pitch_2017_0))) %>%
#   transmute(Year = year_list,
#             Min = round(Min., 0),
#             "1st-Quant" = round(X1st.Qu., 0),
#             Median = round(Median, 0),
#             Mean = round(Mean, 0),
#             "3rd-Quant" = round(X3rd.Qu., 0),
#             Max = round(Max., 0))
# 
# write.csv(check_pitch_all_0, file=paste0(save_path, "check_pitch_0.csv"), row.names=FALSE)
# 
# check_pitch_all_1 <- data.frame(t(cbind(check_pitch_2008_1, check_pitch_2009_1,
#                                         check_pitch_2010_1, check_pitch_2011_1,
#                                         check_pitch_2012_1, check_pitch_2013_1,
#                                         check_pitch_2014_1, check_pitch_2015_1,
#                                         check_pitch_2016_1, check_pitch_2017_1))) %>%
#   transmute(Year = year_list,
#             Min = round(Min., 0),
#             "1st-Quant" = round(X1st.Qu., 0),
#             Median = round(Median, 0),
#             Mean = round(Mean, 0),
#             "3rd-Quant" = round(X3rd.Qu., 0),
#             Max = round(Max., 0))
# 
# write.csv(check_pitch_all_1, file=paste0(save_path, "check_pitch_1.csv"), row.names=FALSE)
# 
# summarise_pitch <- function(df, year) {
#   df <- df %>%
#     summarise(total = n(),
#               sz = sum(sz_na, na.rm = TRUE)) %>%
#     mutate(year = year,
#            percent_exist = round(sz/total*100, 1))
# }
# 
# check_pitch_2008 <- summarise_pitch(check_pitch(pitch_2008), "2008")
# check_pitch_2009 <- summarise_pitch(check_pitch(pitch_2009), "2009")
# check_pitch_2010 <- summarise_pitch(check_pitch(pitch_2010), "2010")
# check_pitch_2011 <- summarise_pitch(check_pitch(pitch_2011), "2011")
# check_pitch_2012 <- summarise_pitch(check_pitch(pitch_2012), "2012")
# check_pitch_2013 <- summarise_pitch(check_pitch(pitch_2013), "2013")
# check_pitch_2014 <- summarise_pitch(check_pitch(pitch_2014), "2014")
# check_pitch_2015 <- summarise_pitch(check_pitch(pitch_2015), "2015")
# check_pitch_2016 <- summarise_pitch(check_pitch(pitch_2016), "2016")
# check_pitch_2017 <- summarise_pitch(check_pitch(pitch_2017), "2017")
# 
# check_pitch_all <- rbind(check_pitch_2008, check_pitch_2009,
#                          check_pitch_2010, check_pitch_2011,
#                          check_pitch_2012, check_pitch_2013,
#                          check_pitch_2014, check_pitch_2015,
#                          check_pitch_2016, check_pitch_2017) %>%
#   select(year, sz, total, percent_exist)
# 
# write.csv(check_pitch_all, file=paste0(save_path, "check_pitch.csv"), row.names=FALSE)
# 
# rm(list=ls(pattern="check"))
