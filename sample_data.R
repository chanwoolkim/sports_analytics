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
save_path <- "/Volumes/huizinga/MLB/Chanwool/Sample Data/"

#Run if starting from scratch
# d.2012i <- src_sqlite(paste0(data_path, "d.2012i.sqlite3"), create=TRUE)
# scrape(start="2012-03-28",end="2012-10-01",suffix="inning/inning_all.xml",connect=d.2012i$con)

#Assuming that we have scraped the data already
d.2012i <- src_sqlite(paste0(data_path, "d.2012i.sqlite3"))

d1.2012p <- collect(tbl(d.2012i, "pitch"))
d1.2012b <- collect(tbl(d.2012i, "atbat"))
d1.2012r <- collect(tbl(d.2012i, "runner"))

#Choose three games
pitch.sample <- d1.2012p %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

atbat.sample <- d1.2012b %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

runner.sample <- d1.2012r %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

write.csv(pitch.sample, file=paste0(save_path, "pitch_sample2012.csv"), row.names=FALSE)
write.csv(atbat.sample, file=paste0(save_path, "at.bat_sample2012.csv"), row.names=FALSE)
write.csv(runner.sample, file=paste0(save_path, "runner_sample2012.csv"), row.names=FALSE)
