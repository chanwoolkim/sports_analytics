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
save_path <- "/Volumes/huizinga/MLB/Chanwool/Basic Data/"

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


#Check to see if duplicated variables are matching ####
atbat_2008_dupvar <- atbat_2008 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2009_dupvar <- atbat_2009 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2010_dupvar <- atbat_2010 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2011_dupvar <- atbat_2011 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2012_dupvar <- atbat_2012 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2013_dupvar <- atbat_2013 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2014_dupvar <- atbat_2014 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2015_dupvar <- atbat_2015 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2016_dupvar <- atbat_2016 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)
atbat_2017_dupvar <- atbat_2017 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, score, game.bnum)

pitch_2008_dupvar <- pitch_2008 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2009_dupvar <- pitch_2009 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2010_dupvar <- pitch_2010 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2011_dupvar <- pitch_2011 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2012_dupvar <- pitch_2012 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2013_dupvar <- pitch_2013 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2014_dupvar <- pitch_2014 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2015_dupvar <- pitch_2015 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2016_dupvar <- pitch_2016 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)
pitch_2017_dupvar <- pitch_2017 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, id, game.bnum)

runner_2008_dupvar <- runner_2008 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2009_dupvar <- runner_2009 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2010_dupvar <- runner_2010 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2011_dupvar <- runner_2011 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2012_dupvar <- runner_2012 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2013_dupvar <- runner_2013 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2014_dupvar <- runner_2014 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2015_dupvar <- runner_2015 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2016_dupvar <- runner_2016 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)
runner_2017_dupvar <- runner_2017 %>%
  select(event_num, gameday_link, inning, inning_side, next_, num, url, event, id, score, game.bnum)

#Use at.bat as the one that gets merged to the other
pitch_atbat_dup <- function(pitch, atbat) {
  pitch_atbat <- left_join(pitch, atbat, by="game.bnum") %>%
    mutate(event_num_dup = event_num.x != event_num.y,
           inning_dup = inning.x != inning.y,
           inning_side_dup = inning_side.x != inning_side.y,
           next__dup = next_.x != next_.y,
           url_dup = url.x != url.y) %>%
    ungroup %>%
    summarise(event_num_count = sum(event_num_dup, na.rm=TRUE),
              inning_count = sum(inning_dup, na.rm=TRUE),
              inning_side_count = sum(inning_side_dup, na.rm=TRUE),
              next__count = sum(next__dup, na.rm=TRUE),
              url_count = sum(url_dup, na.rm=TRUE))
  return(pitch_atbat)
}

pitch_atbat_dup(pitch_2008_dupvar, atbat_2008_dupvar)
pitch_atbat_dup(pitch_2009_dupvar, atbat_2009_dupvar)
pitch_atbat_dup(pitch_2010_dupvar, atbat_2010_dupvar)
pitch_atbat_dup(pitch_2011_dupvar, atbat_2011_dupvar)
pitch_atbat_dup(pitch_2012_dupvar, atbat_2012_dupvar)
pitch_atbat_dup(pitch_2013_dupvar, atbat_2013_dupvar)
pitch_atbat_dup(pitch_2014_dupvar, atbat_2014_dupvar)
pitch_atbat_dup(pitch_2015_dupvar, atbat_2015_dupvar)
pitch_atbat_dup(pitch_2016_dupvar, atbat_2016_dupvar)
pitch_atbat_dup(pitch_2017_dupvar, atbat_2017_dupvar)

runner_atbat_dup <- function(runner, atbat) {
  runner_atbat <- left_join(runner, atbat, by="game.bnum") %>%
    mutate(event_dup = as.character(event.x) != as.character(event.y),
           score_dup = score.x != score.y,
           event_num_dup = event_num.x != event_num.y,
           inning_dup = inning.x != inning.y,
           inning_side_dup = inning_side.x != inning_side.y,
           next__dup = next_.x != next_.y,
           url_dup = url.x != url.y) %>%
    ungroup %>%
    summarise(event_count = sum(event_dup, na.rm=TRUE),
              score_count = sum(score_dup, na.rm=TRUE),
              event_num_count = sum(event_num_dup, na.rm=TRUE),
              inning_count = sum(inning_dup, na.rm=TRUE),
              inning_side_count = sum(inning_side_dup, na.rm=TRUE),
              next__count = sum(next__dup, na.rm=TRUE),
              url_count = sum(url_dup, na.rm=TRUE))
  return(runner_atbat)
}

runner_atbat_dup(runner_2008_dupvar, atbat_2008_dupvar)
runner_atbat_dup(runner_2009_dupvar, atbat_2009_dupvar)
runner_atbat_dup(runner_2010_dupvar, atbat_2010_dupvar)
runner_atbat_dup(runner_2011_dupvar, atbat_2011_dupvar)
runner_atbat_dup(runner_2012_dupvar, atbat_2012_dupvar)
runner_atbat_dup(runner_2013_dupvar, atbat_2013_dupvar)
runner_atbat_dup(runner_2014_dupvar, atbat_2014_dupvar)
runner_atbat_dup(runner_2015_dupvar, atbat_2015_dupvar)
runner_atbat_dup(runner_2016_dupvar, atbat_2016_dupvar)
runner_atbat_dup(runner_2017_dupvar, atbat_2017_dupvar)
