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

data_path <- "/Volumes/huizinga/MLB/Chanwool/"
save_path <- "/Volumes/huizinga/MLB/Chanwool/Scraped Data/"

year_list <- c(2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)

#Run if starting from scratch
# for (y in year_list) {
#   assign(paste0("d.", y, "i"),
#          src_sqlite(paste0(data_path, "d.", y, "i.sqlite3"), create=TRUE))
# }
# 
# scrape(start="2008-03-25",end="2008-09-30",suffix="inning/inning_all.xml",connect=d.2008i$con)
# scrape(start="2009-04-05",end="2009-10-06",suffix="inning/inning_all.xml",connect=d.2009i$con)
# scrape(start="2010-04-04",end="2010-10-03",suffix="inning/inning_all.xml",connect=d.2010i$con)
# scrape(start="2011-03-31",end="2011-09-28",suffix="inning/inning_all.xml",connect=d.2011i$con)
# scrape(start="2012-03-28",end="2012-10-01",suffix="inning/inning_all.xml",connect=d.2012i$con)
# scrape(start="2013-03-31",end="2013-09-30",suffix="inning/inning_all.xml",connect=d.2013i$con)
# scrape(start="2014-03-30",end="2014-09-28",suffix="inning/inning_all.xml",connect=d.2014i$con)
# scrape(start="2015-04-05",end="2015-10-01",suffix="inning/inning_all.xml",connect=d.2015i$con)
# scrape(start="2016-04-03",end="2016-10-02",suffix="inning/inning_all.xml",connect=d.2016i$con)
# scrape(start="2017-04-02",end="2017-10-01",suffix="inning/inning_all.xml",connect=d.2017i$con)

#Assuming that we have scraped the data already
for (y in year_list) {
  assign(paste0("d.", y, "i"),
         src_sqlite(paste0(data_path, "d.", y, "i.sqlite3")))
}

d1.2008p <- collect(tbl(d.2008i, "pitch"))
d1.2009p <- collect(tbl(d.2009i, "pitch"))
d1.2010p <- collect(tbl(d.2010i, "pitch"))
d1.2011p <- collect(tbl(d.2011i, "pitch"))
d1.2012p <- collect(tbl(d.2012i, "pitch"))
d1.2013p <- collect(tbl(d.2013i, "pitch"))
d1.2014p <- collect(tbl(d.2014i, "pitch"))
d1.2015p <- collect(tbl(d.2015i, "pitch"))
d1.2016p <- collect(tbl(d.2016i, "pitch"))
d1.2017p <- collect(tbl(d.2017i, "pitch"))

d1.2008b <- collect(tbl(d.2008i, "atbat"))
d1.2009b <- collect(tbl(d.2009i, "atbat"))
d1.2010b <- collect(tbl(d.2010i, "atbat"))
d1.2011b <- collect(tbl(d.2011i, "atbat"))
d1.2012b <- collect(tbl(d.2012i, "atbat"))
d1.2013b <- collect(tbl(d.2013i, "atbat"))
d1.2014b <- collect(tbl(d.2014i, "atbat"))
d1.2015b <- collect(tbl(d.2015i, "atbat"))
d1.2016b <- collect(tbl(d.2016i, "atbat"))
d1.2017b <- collect(tbl(d.2017i, "atbat"))

d1.2008r <- collect(tbl(d.2008i, "runner"))
d1.2009r <- collect(tbl(d.2009i, "runner"))
d1.2010r <- collect(tbl(d.2010i, "runner"))
d1.2011r <- collect(tbl(d.2011i, "runner"))
d1.2012r <- collect(tbl(d.2012i, "runner"))
d1.2013r <- collect(tbl(d.2013i, "runner"))
d1.2014r <- collect(tbl(d.2014i, "runner"))
d1.2015r <- collect(tbl(d.2015i, "runner"))
d1.2016r <- collect(tbl(d.2016i, "runner"))
d1.2017r <- collect(tbl(d.2017i, "runner"))

atbat_select <- function(df, year) {
  df <- df %>%
    mutate(season = year) %>%
    select(season,
           gameday_link,
           date,
           num,
           inning,
           inning_side,
           next_,
           start_tfs,
           start_tfs_zulu,
           pitcher,
           pitcher_name,
           p_throws,
           batter,
           batter_name,
           stand,
           b_height,
           b,
           s,
           o,
           event_num,
           atbat_des,
           event,
           event2,
           event3,
           event4,
           score,
           home_team_runs,
           away_team_runs,
           url)
}

d1.2008b <- atbat_select(d1.2008b, "2008")
d1.2009b <- atbat_select(d1.2009b, "2009")
d1.2010b <- atbat_select(d1.2010b, "2010")
d1.2011b <- atbat_select(d1.2011b, "2011")
d1.2012b <- atbat_select(d1.2012b, "2012")
d1.2013b <- atbat_select(d1.2013b, "2013")
d1.2014b <- atbat_select(d1.2014b, "2014")
d1.2015b <- atbat_select(d1.2015b, "2015")
d1.2016b <- atbat_select(d1.2016b, "2016")
d1.2017b <- atbat_select(d1.2017b, "2017")

pitch_select <- function(df, year) {
  df <- df %>%
    mutate(season = year) %>%
    select(season,
           gameday_link,
           num,
           inning,
           inning_side,
           next_,
           id,
           event_num,
           tfs,
           tfs_zulu,
           sv_id,
           count,
           on_1b,
           on_2b,
           on_3b,
           sz_top,
           sz_bot,
           type,
           des,
           pitch_type,
           type_confidence,
           start_speed,
           end_speed,
           y0,
           x0,
           z0,
           vx0,
           vy0,
           vz0,
           ax,
           ay,
           az,
           spin_dir,
           spin_rate,
           px,
           pz,
           x,
           y,
           zone,
           pfx_x,
           pfx_z,
           break_y,
           break_angle,
           break_length,
           nasty,
           cc,
           mt,
           url)
}

d1.2008p <- pitch_select(d1.2008p, "2008")
d1.2009p <- pitch_select(d1.2009p, "2009")
d1.2010p <- pitch_select(d1.2010p, "2010")
d1.2011p <- pitch_select(d1.2011p, "2011")
d1.2012p <- pitch_select(d1.2012p, "2012")
d1.2013p <- pitch_select(d1.2013p, "2013")
d1.2014p <- pitch_select(d1.2014p, "2014")
d1.2015p <- pitch_select(d1.2015p, "2015")
d1.2016p <- pitch_select(d1.2016p, "2016")
d1.2017p <- pitch_select(d1.2017p, "2017")

runner_select <- function(df, year) {
  df <- df %>%
    mutate(season = year) %>%
    select(season,
           gameday_link,
           num,
           event_num,
           inning,
           inning_side,
           next_,
           id,
           start,
           end,
           event,
           score,
           rbi,
           earned,
           url)
}

d1.2008r <- runner_select(d1.2008r, "2008")
d1.2009r <- runner_select(d1.2009r, "2009")
d1.2010r <- runner_select(d1.2010r, "2010")
d1.2011r <- runner_select(d1.2011r, "2011")
d1.2012r <- runner_select(d1.2012r, "2012")
d1.2013r <- runner_select(d1.2013r, "2013")
d1.2014r <- runner_select(d1.2014r, "2014")
d1.2015r <- runner_select(d1.2015r, "2015")
d1.2016r <- runner_select(d1.2016r, "2016")
d1.2017r <- runner_select(d1.2017r, "2017")

write.csv(d1.2008b, file=paste0(save_path, "At Bat Data/2008_atbat.csv"), row.names=FALSE)
write.csv(d1.2008p, file=paste0(save_path, "Pitch Data/2008_pitch.csv"), row.names=FALSE)
write.csv(d1.2008r, file=paste0(save_path, "Runner Data/2008_runner.csv"), row.names=FALSE)

write.csv(d1.2009b, file=paste0(save_path, "At Bat Data/2009_atbat.csv"), row.names=FALSE)
write.csv(d1.2009p, file=paste0(save_path, "Pitch Data/2009_pitch.csv"), row.names=FALSE)
write.csv(d1.2009r, file=paste0(save_path, "Runner Data/2009_runner.csv"), row.names=FALSE)

write.csv(d1.2010b, file=paste0(save_path, "At Bat Data/2010_atbat.csv"), row.names=FALSE)
write.csv(d1.2010p, file=paste0(save_path, "Pitch Data/2010_pitch.csv"), row.names=FALSE)
write.csv(d1.2010r, file=paste0(save_path, "Runner Data/2010_runner.csv"), row.names=FALSE)

write.csv(d1.2011b, file=paste0(save_path, "At Bat Data/2011_atbat.csv"), row.names=FALSE)
write.csv(d1.2011p, file=paste0(save_path, "Pitch Data/2011_pitch.csv"), row.names=FALSE)
write.csv(d1.2011r, file=paste0(save_path, "Runner Data/2011_runner.csv"), row.names=FALSE)

write.csv(d1.2012b, file=paste0(save_path, "At Bat Data/2012_atbat.csv"), row.names=FALSE)
write.csv(d1.2012p, file=paste0(save_path, "Pitch Data/2012_pitch.csv"), row.names=FALSE)
write.csv(d1.2012r, file=paste0(save_path, "Runner Data/2012_runner.csv"), row.names=FALSE)

write.csv(d1.2013b, file=paste0(save_path, "At Bat Data/2013_atbat.csv"), row.names=FALSE)
write.csv(d1.2013p, file=paste0(save_path, "Pitch Data/2013_pitch.csv"), row.names=FALSE)
write.csv(d1.2013r, file=paste0(save_path, "Runner Data/2013_runner.csv"), row.names=FALSE)

write.csv(d1.2014b, file=paste0(save_path, "At Bat Data/2014_atbat.csv"), row.names=FALSE)
write.csv(d1.2014p, file=paste0(save_path, "Pitch Data/2014_pitch.csv"), row.names=FALSE)
write.csv(d1.2014r, file=paste0(save_path, "Runner Data/2014_runner.csv"), row.names=FALSE)

write.csv(d1.2015b, file=paste0(save_path, "At Bat Data/2015_atbat.csv"), row.names=FALSE)
write.csv(d1.2015p, file=paste0(save_path, "Pitch Data/2015_pitch.csv"), row.names=FALSE)
write.csv(d1.2015r, file=paste0(save_path, "Runner Data/2015_runner.csv"), row.names=FALSE)

write.csv(d1.2016b, file=paste0(save_path, "At Bat Data/2016_atbat.csv"), row.names=FALSE)
write.csv(d1.2016p, file=paste0(save_path, "Pitch Data/2016_pitch.csv"), row.names=FALSE)
write.csv(d1.2016r, file=paste0(save_path, "Runner Data/2016_runner.csv"), row.names=FALSE)

write.csv(d1.2017b, file=paste0(save_path, "At Bat Data/2017_atbat.csv"), row.names=FALSE)
write.csv(d1.2017p, file=paste0(save_path, "Pitch Data/2017_pitch.csv"), row.names=FALSE)
write.csv(d1.2017r, file=paste0(save_path, "Runner Data/2017_runner.csv"), row.names=FALSE)

rm(list=ls(pattern="d1."))


#Summary table of players/umpires/coaches ####
#Run if starting from scratch
# for (y in year_list) {
#   assign(paste0("d.", y, "p"),
#          src_sqlite(paste0(data_path, "d.", y, "p.sqlite3"), create=TRUE))
# }
# 
# scrape(start="2008-03-25",end="2008-09-30",suffix="players.xml",connect=d.2008p$con)
# scrape(start="2009-04-05",end="2009-10-06",suffix="players.xml",connect=d.2009p$con)
# scrape(start="2010-04-04",end="2010-10-03",suffix="players.xml",connect=d.2010p$con)
# scrape(start="2011-03-31",end="2011-09-28",suffix="players.xml",connect=d.2011p$con)
# scrape(start="2012-03-28",end="2012-10-01",suffix="players.xml",connect=d.2012p$con)
# scrape(start="2013-03-31",end="2013-09-30",suffix="players.xml",connect=d.2013p$con)
# scrape(start="2014-03-30",end="2014-09-28",suffix="players.xml",connect=d.2014p$con)
# scrape(start="2015-04-05",end="2015-10-01",suffix="players.xml",connect=d.2015p$con)
# scrape(start="2016-04-03",end="2016-10-02",suffix="players.xml",connect=d.2016p$con)
# scrape(start="2017-04-02",end="2017-10-01",suffix="players.xml",connect=d.2017p$con)

#Assuming that we have scraped the data already
for (y in year_list) {
  assign(paste0("d.", y, "p"),
         src_sqlite(paste0(data_path, "d.", y, "p.sqlite3")))
}

#Extract id-name info
extract_id <- function(df, source) {
  if (source %in% c("player", "coach")) {
    return(df %>% mutate(full_name = paste(first, last)) %>%
      select(id, full_name))
  }
  if (source == "umpire") {
    return(df %>% mutate(full_name = name) %>%
      select(id, full_name))
  }
}

d1.2008p <- extract_id(collect(tbl(d.2008p, "player")), "player")
d1.2009p <- extract_id(collect(tbl(d.2009p, "player")), "player")
d1.2010p <- extract_id(collect(tbl(d.2010p, "player")), "player")
d1.2011p <- extract_id(collect(tbl(d.2011p, "player")), "player")
d1.2012p <- extract_id(collect(tbl(d.2012p, "player")), "player")
d1.2013p <- extract_id(collect(tbl(d.2013p, "player")), "player")
d1.2014p <- extract_id(collect(tbl(d.2014p, "player")), "player")
d1.2015p <- extract_id(collect(tbl(d.2015p, "player")), "player")
d1.2016p <- extract_id(collect(tbl(d.2016p, "player")), "player")
d1.2017p <- extract_id(collect(tbl(d.2017p, "player")), "player")

d1.2008c <- extract_id(collect(tbl(d.2008p, "coach")), "coach")
d1.2009c <- extract_id(collect(tbl(d.2009p, "coach")), "coach")
d1.2010c <- extract_id(collect(tbl(d.2010p, "coach")), "coach")
d1.2011c <- extract_id(collect(tbl(d.2011p, "coach")), "coach")
d1.2012c <- extract_id(collect(tbl(d.2012p, "coach")), "coach")
d1.2013c <- extract_id(collect(tbl(d.2013p, "coach")), "coach")
d1.2014c <- extract_id(collect(tbl(d.2014p, "coach")), "coach")
d1.2015c <- extract_id(collect(tbl(d.2015p, "coach")), "coach")
d1.2016c <- extract_id(collect(tbl(d.2016p, "coach")), "coach")
d1.2017c <- extract_id(collect(tbl(d.2017p, "coach")), "coach")

d1.2008u <- extract_id(collect(tbl(d.2008p, "umpire")), "umpire")
d1.2009u <- extract_id(collect(tbl(d.2009p, "umpire")), "umpire")
d1.2010u <- extract_id(collect(tbl(d.2010p, "umpire")), "umpire")
d1.2011u <- extract_id(collect(tbl(d.2011p, "umpire")), "umpire")
d1.2012u <- extract_id(collect(tbl(d.2012p, "umpire")), "umpire")
d1.2013u <- extract_id(collect(tbl(d.2013p, "umpire")), "umpire")
d1.2014u <- extract_id(collect(tbl(d.2014p, "umpire")), "umpire")
d1.2015u <- extract_id(collect(tbl(d.2015p, "umpire")), "umpire")
d1.2016u <- extract_id(collect(tbl(d.2016p, "umpire")), "umpire")
d1.2017u <- extract_id(collect(tbl(d.2017p, "umpire")), "umpire")

#Player table
player_table <- players
player_table <- rbind(player_table,
                       d1.2008p, d1.2009p, d1.2010p, d1.2011p, d1.2012p,
                       d1.2013p, d1.2014p, d1.2015p, d1.2016p, d1.2017p) %>%
  distinct(id, full_name)

player_table <- player_table[order(player_table$id),]

#Spread multiple full_names
player_table <- player_table %>% group_by(id) %>%
  mutate(n = row_number()) %>% ungroup %>%
  spread(key = n, value = full_name) %>%
  select(id,
         full_name_1 = "1",
         full_name_2 = "2",
         full_name_3 = "3",
         full_name_4 = "4") %>%
  filter(id != 0)

write.csv(player_table, file=paste0(save_path, "Summary/summary_player.csv"), row.names=FALSE)

#Coach table
coach_table <- rbind(d1.2008c, d1.2009c, d1.2010c, d1.2011c, d1.2012c,
                      d1.2013c, d1.2014c, d1.2015c, d1.2016c, d1.2017c) %>%
  distinct(id, full_name)

coach_table <- coach_table[order(coach_table$id),]

#Spread multiple full_names
coach_table <- coach_table %>% group_by(id) %>%
  mutate(n = row_number()) %>% ungroup %>%
  spread(key = n, value = full_name) %>%
  select(id,
         full_name_1 = "1",
         full_name_2 = "2",
         full_name_3 = "3")

write.csv(coach_table, file=paste0(save_path, "Summary/summary_coach.csv"), row.names=FALSE)

#Umpire table
umpire_table <- rbind(d1.2008u, d1.2009u, d1.2010u, d1.2011u, d1.2012u,
                     d1.2013u, d1.2014u, d1.2015u, d1.2016u, d1.2017u) %>%
  distinct(id, full_name)

umpire_table <- umpire_table[order(umpire_table$id),]

#Separate NAs
umpire_table_na <- umpire_table %>%
  filter(is.na(id) & full_name != "") %>%
  mutate(id_na = TRUE)

umpire_table <- umpire_table %>% filter(!is.na(id))

#Spread multiple full_names
umpire_table <- umpire_table %>% group_by(id) %>%
  mutate(n = row_number()) %>% ungroup %>%
  spread(key = n, value = full_name) %>%
  select(id,
         full_name_1 = "1",
         full_name_2 = "2",
         full_name_3 = "3") %>%
  filter(id != 0)

#See if any names missed ID before
#Checked to make sure no matching full_name_2 and 3
umpire_table <- full_join(umpire_table,
                          umpire_table_na %>% select(id_na, full_name),
                          by = c("full_name_1" = "full_name")) %>%
  select(id, id_na, full_name_1, full_name_2, full_name_3)

write.csv(umpire_table, file=paste0(save_path, "Summary/summary_umpire.csv"), row.names=FALSE)
