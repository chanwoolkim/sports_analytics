library(pitchRx)
library(plyr)
library(dplyr)
library(RSQLite)
library(devtools)
library(plm)
library(lmtest)
library(clubSandwich)
library(reshape)

data_path <- "/Volumes/huizinga/MLB/From Kyle/"
save_path <- "/Volumes/huizinga/MLB/Chanwool/Scraped Data/"

year_list <- c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)

#Run if starting from scratch
# for (y in year_list) {
#   assign(paste0("d.", y, "i"),
#          src_sqlite(paste0(data_path, "d.", y, "i.sqlite3"), create=TRUE))
# }
# 
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

d1.2010p <- collect(tbl(d.2010i, "pitch"))
d1.2011p <- collect(tbl(d.2011i, "pitch"))
d1.2012p <- collect(tbl(d.2012i, "pitch"))
d1.2013p <- collect(tbl(d.2013i, "pitch"))
d1.2014p <- collect(tbl(d.2014i, "pitch"))
d1.2015p <- collect(tbl(d.2015i, "pitch"))
d1.2016p <- collect(tbl(d.2016i, "pitch"))
d1.2017p <- collect(tbl(d.2017i, "pitch"))

d1.2010b <- collect(tbl(d.2010i, "atbat"))
d1.2011b <- collect(tbl(d.2011i, "atbat"))
d1.2012b <- collect(tbl(d.2012i, "atbat"))
d1.2013b <- collect(tbl(d.2013i, "atbat"))
d1.2014b <- collect(tbl(d.2014i, "atbat"))
d1.2015b <- collect(tbl(d.2015i, "atbat"))
d1.2016b <- collect(tbl(d.2016i, "atbat"))
d1.2017b <- collect(tbl(d.2017i, "atbat"))

d1.2010r <- collect(tbl(d.2010i, "runner"))
d1.2011r <- collect(tbl(d.2011i, "runner"))
d1.2012r <- collect(tbl(d.2012i, "runner"))
d1.2013r <- collect(tbl(d.2013i, "runner"))
d1.2014r <- collect(tbl(d.2014i, "runner"))
d1.2015r <- collect(tbl(d.2015i, "runner"))
d1.2016r <- collect(tbl(d.2016i, "runner"))
d1.2017r <- collect(tbl(d.2017i, "runner"))

atbat_select <- function(data, year) {
  data <- data %>%
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

d1.2010b <- atbat_select(d1.2010b, "2010")
d1.2011b <- atbat_select(d1.2010b, "2011")
d1.2012b <- atbat_select(d1.2010b, "2012")
d1.2013b <- atbat_select(d1.2010b, "2013")
d1.2014b <- atbat_select(d1.2010b, "2014")
d1.2015b <- atbat_select(d1.2010b, "2015")
d1.2016b <- atbat_select(d1.2010b, "2016")
d1.2017b <- atbat_select(d1.2010b, "2017")

pitch_select <- function(data, year) {
  data <- data %>%
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

d1.2010p <- pitch_select(d1.2010p, "2010")
d1.2011p <- pitch_select(d1.2010p, "2011")
d1.2012p <- pitch_select(d1.2010p, "2012")
d1.2013p <- pitch_select(d1.2010p, "2013")
d1.2014p <- pitch_select(d1.2010p, "2014")
d1.2015p <- pitch_select(d1.2010p, "2015")
d1.2016p <- pitch_select(d1.2010p, "2016")
d1.2017p <- pitch_select(d1.2010p, "2017")

runner_select <- function(data, year) {
  data <- data %>%
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

d1.2010r <- runner_select(d1.2010r, "2010")
d1.2011r <- runner_select(d1.2010r, "2011")
d1.2012r <- runner_select(d1.2010r, "2012")
d1.2013r <- runner_select(d1.2010r, "2013")
d1.2014r <- runner_select(d1.2010r, "2014")
d1.2015r <- runner_select(d1.2010r, "2015")
d1.2016r <- runner_select(d1.2010r, "2016")
d1.2017r <- runner_select(d1.2010r, "2017")

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
