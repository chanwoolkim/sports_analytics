library(pitchRx)
library(plyr)
library(dplyr)
library(RSQLite)
library(devtools)
library(plm)
library(lmtest)
library(clubSandwich)
library(reshape)

data_path <- "/Volumes/huizinga/MLB/Chanwool/Scraped Data/"
save_path <- "/Volumes/huizinga/MLB/Chanwool/Basic Data/Pitch/"

year_list <- c(2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)

#Load all data ####

for (y in year_list) {
  assign(paste0("atbat_", y),
         read.csv(paste0(data_path, "At Bat Data/", y, "_atbat.csv")))
  assign(paste0("pitch_", y),
         read.csv(paste0(data_path, "Pitch Data/", y, "_pitch.csv")))
  assign(paste0("runner_", y),
         read.csv(paste0(data_path, "Runner Data/", y, "_runner.csv")))
}


#Create game.bnum, which is a combination of gameday_link and num ####
atbat_2008 <- atbat_2008 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2009 <- atbat_2009 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2010 <- atbat_2010 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2011 <- atbat_2011 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2012 <- atbat_2012 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2013 <- atbat_2013 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2014 <- atbat_2014 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2015 <- atbat_2015 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2016 <- atbat_2016 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
atbat_2017 <- atbat_2017 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))

pitch_2008 <- pitch_2008 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2009 <- pitch_2009 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2010 <- pitch_2010 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2011 <- pitch_2011 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2012 <- pitch_2012 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2013 <- pitch_2013 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2014 <- pitch_2014 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2015 <- pitch_2015 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2016 <- pitch_2016 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
pitch_2017 <- pitch_2017 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))

runner_2008 <- runner_2008 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2009 <- runner_2009 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2010 <- runner_2010 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2011 <- runner_2011 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2012 <- runner_2012 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2013 <- runner_2013 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2014 <- runner_2014 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2015 <- runner_2015 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2016 <- runner_2016 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))
runner_2017 <- runner_2017 %>% mutate(game.bnum = paste(gameday_link, num, sep="."))


#Check that gameday_link*num has no duplicates in at.bat data ####
check_duplicates <- function(df) {
  df %>% add_count(game.bnum) %>% filter(n > 1) %>% nrow() == 0
}

data.frame(year = year_list,
           duplicates = lapply(list(atbat_2008, atbat_2009, atbat_2010, atbat_2011, atbat_2012,
                                    atbat_2013, atbat_2014, atbat_2015, atbat_2016, atbat_2017),
                               check_duplicates) %>% unlist)

#2010, 2015, 2016 has duplicates: these are ejections and have more than one time stamp
atbat_2010_duplicates <- atbat_2010 %>% add_count(game.bnum) %>% filter(n > 1)
atbat_2015_duplicates <- atbat_2015 %>% add_count(game.bnum) %>% filter(n > 1)
atbat_2016_duplicates <- atbat_2016 %>% add_count(game.bnum) %>% filter(n > 1)
atbat_duplicates <- rbind(atbat_2010_duplicates, atbat_2015_duplicates, atbat_2016_duplicates)

write.csv(atbat_duplicates,
          file="/Volumes/huizinga/MLB/Chanwool/atbat_duplicates.csv", row.names=FALSE)

rm(atbat_duplicates, atbat_2010_duplicates, atbat_2015_duplicates, atbat_2016_duplicates)

#Choose first one for these cases
atbat_2010 <- atbat_2010 %>%
  filter(!(game.bnum == "gid_2010_06_23_lanmlb_anamlb_1.68" & start_tfs == 43754))
atbat_2015 <- atbat_2015 %>%
  filter(!(game.bnum == "gid_2015_09_23_clemlb_minmlb_1.69" & start_tfs == 25802))
atbat_2016 <- atbat_2016 %>%
  filter(!(game.bnum == "gid_2016_05_20_kcamlb_chamlb_1.76" & start_tfs == 30845))


#Merge data ####
#First need to rename variables with same names but different values
clean_atbat <- function(atbat) {
  atbat %>% dplyr::rename(event_num_ab = event_num,
                          event_ab = event, event2_ab = event2, event3_ab = event3, event4_ab = event4,
                          score_ab = score) %>%
    mutate(gameday_link = as.character(gameday_link),
           date = as.character(date),
           inning_side = as.character(inning_side),
           next_ = as.character(next_),
           pitcher_name = as.character(pitcher_name),
           p_throws = as.character(p_throws),
           batter_name = as.character(batter_name),
           stand = as.character(stand),
           b_height = as.character(b_height),
           atbat_des = as.character(atbat_des),
           event_ab = as.character(event_ab),
           event2_ab = as.character(event2_ab),
           event3_ab = as.character(event3_ab),
           event4_ab = as.character(event4_ab),
           url = as.character(url))
}

atbat_2008 <- clean_atbat(atbat_2008)
atbat_2009 <- clean_atbat(atbat_2009)
atbat_2010 <- clean_atbat(atbat_2010)
atbat_2011 <- clean_atbat(atbat_2011)
atbat_2012 <- clean_atbat(atbat_2012)
atbat_2013 <- clean_atbat(atbat_2013)
atbat_2014 <- clean_atbat(atbat_2014)
atbat_2015 <- clean_atbat(atbat_2015)
atbat_2016 <- clean_atbat(atbat_2016)
atbat_2017 <- clean_atbat(atbat_2017)

clean_pitch <- function(pitch) {
  pitch %>% dplyr::rename(event_num_p = event_num, id_p = id) %>%
    select(-season, -gameday_link, -inning, -inning_side, -next_, -num, -url) %>%
    mutate(sv_id = as.character(sv_id),
           count = as.character(count),
           type = as.character(type),
           des = as.character(des),
           pitch_type = as.character(pitch_type),
           cc = as.character(cc))
}

pitch_2008 <- clean_pitch(pitch_2008)
pitch_2009 <- clean_pitch(pitch_2009)
pitch_2010 <- clean_pitch(pitch_2010)
pitch_2011 <- clean_pitch(pitch_2011)
pitch_2012 <- clean_pitch(pitch_2012)
pitch_2013 <- clean_pitch(pitch_2013)
pitch_2014 <- clean_pitch(pitch_2014)
pitch_2015 <- clean_pitch(pitch_2015)
pitch_2016 <- clean_pitch(pitch_2016)
pitch_2017 <- clean_pitch(pitch_2017)

#Make sure unique rows for game.bnum in runner data
#"Spread" variables by base
clean_runner <- function(runner) {
  runner %>% dplyr::rename(event_num_r = event_num) %>%
    select(-season, -gameday_link, -inning, -inning_side, -next_, -num, -url) %>%
    group_by(game.bnum) %>%
    mutate(event = as.character(event),
           start = as.character(start),
           end = as.character(end),
           r_h = ifelse(start == "", id, NA),
           r_1b = ifelse(start == "1B", id, NA),
           r_2b = ifelse(start == "2B", id, NA),
           r_3b = ifelse(start == "3B", id, NA),
           event_h = ifelse(start == "", event, ""),
           event_1b = ifelse(start == "1B", event, ""),
           event_2b = ifelse(start == "2B", event, ""),
           event_3b = ifelse(start == "3B", event, ""),
           score_h = ifelse(start == "", score, NA),
           score_1b = ifelse(start == "1B", score, NA),
           score_2b = ifelse(start == "2B", score, NA),
           score_3b = ifelse(start == "3B", score, NA),
           rbi_h = ifelse(start == "", rbi, NA),
           rbi_1b = ifelse(start == "1B", rbi, NA),
           rbi_2b = ifelse(start == "2B", rbi, NA),
           rbi_3b = ifelse(start == "3B", rbi, NA),
           earned_h = ifelse(start == "", earned, NA),
           earned_1b = ifelse(start == "1B", earned, NA),
           earned_2b = ifelse(start == "2B", earned, NA),
           earned_3b = ifelse(start == "3B", earned, NA)) %>%
    ungroup %>%
    distinct(game.bnum, .keep_all = TRUE) %>%
    select(-start, -end, -id, -event, -score, -rbi, -earned)
}

runner_2008 <- clean_runner(runner_2008)
runner_2009 <- clean_runner(runner_2009)
runner_2010 <- clean_runner(runner_2010)
runner_2011 <- clean_runner(runner_2011)
runner_2012 <- clean_runner(runner_2012)
runner_2013 <- clean_runner(runner_2013)
runner_2014 <- clean_runner(runner_2014)
runner_2015 <- clean_runner(runner_2015)
runner_2016 <- clean_runner(runner_2016)
runner_2017 <- clean_runner(runner_2017)

#Merge at.bat to pitch first
df_2008 <- join(pitch_2008, atbat_2008, by="game.bnum", type="left")
df_2009 <- join(pitch_2009, atbat_2009, by="game.bnum", type="left")
df_2010 <- join(pitch_2010, atbat_2010, by="game.bnum", type="left")
df_2011 <- join(pitch_2011, atbat_2011, by="game.bnum", type="left")
df_2012 <- join(pitch_2012, atbat_2012, by="game.bnum", type="left")
df_2013 <- join(pitch_2013, atbat_2013, by="game.bnum", type="left")
df_2014 <- join(pitch_2014, atbat_2014, by="game.bnum", type="left")
df_2015 <- join(pitch_2015, atbat_2015, by="game.bnum", type="left")
df_2016 <- join(pitch_2016, atbat_2016, by="game.bnum", type="left")
df_2017 <- join(pitch_2017, atbat_2017, by="game.bnum", type="left")

rm(list=ls(pattern="^atbat"))
rm(list=ls(pattern="^pitch"))

#Join runner info now
df_2008 <- join(df_2008, runner_2008, by="game.bnum", type="left")
df_2009 <- join(df_2009, runner_2009, by="game.bnum", type="left")
df_2010 <- join(df_2010, runner_2010, by="game.bnum", type="left")
df_2011 <- join(df_2011, runner_2011, by="game.bnum", type="left")
df_2012 <- join(df_2012, runner_2012, by="game.bnum", type="left")
df_2013 <- join(df_2013, runner_2013, by="game.bnum", type="left")
df_2014 <- join(df_2014, runner_2014, by="game.bnum", type="left")
df_2015 <- join(df_2015, runner_2015, by="game.bnum", type="left")
df_2016 <- join(df_2016, runner_2016, by="game.bnum", type="left")
df_2017 <- join(df_2017, runner_2017, by="game.bnum", type="left")

rm(list=ls(pattern="^runner"))

write.csv(df_2008, file=paste0(save_path, "df_2008.csv"), row.names=FALSE)
write.csv(df_2009, file=paste0(save_path, "df_2009.csv"), row.names=FALSE)
write.csv(df_2010, file=paste0(save_path, "df_2010.csv"), row.names=FALSE)
write.csv(df_2011, file=paste0(save_path, "df_2011.csv"), row.names=FALSE)
write.csv(df_2012, file=paste0(save_path, "df_2012.csv"), row.names=FALSE)
write.csv(df_2013, file=paste0(save_path, "df_2013.csv"), row.names=FALSE)
write.csv(df_2014, file=paste0(save_path, "df_2014.csv"), row.names=FALSE)
write.csv(df_2015, file=paste0(save_path, "df_2015.csv"), row.names=FALSE)
write.csv(df_2016, file=paste0(save_path, "df_2016.csv"), row.names=FALSE)
write.csv(df_2017, file=paste0(save_path, "df_2017.csv"), row.names=FALSE)
