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

#At-Bat, Pitch, and Runner data ####

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


#Other data ####

#PO and Action
d1.2012po <- collect(tbl(d.2012i, "po"))
d1.2012a <- collect(tbl(d.2012i, "action"))

po.sample <- d1.2012po %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

action.sample <- d1.2012a %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

write.csv(po.sample, file=paste0(save_path, "po_sample2012.csv"), row.names=FALSE)
write.csv(action.sample, file=paste0(save_path, "action_sample2012.csv"), row.names=FALSE)

#Game and Media
#Run if starting from scratch
# d.2012m <- src_sqlite(paste0(data_path, "d.2012m.sqlite3"), create=TRUE)
# scrape(start="2012-03-28",end="2012-10-01",suffix="miniscoreboard.xml",connect=d.2012m$con)

#Assuming that we have scraped the data already
d.2012m <- src_sqlite(paste0(data_path, "d.2012m.sqlite3"))

d1.2012g <- collect(tbl(d.2012m, "game"))
d1.2012m <- collect(tbl(d.2012m, "media"))

game.sample <- d1.2012g %>%
  filter(gameday_link %in% c("2012_03_28_anamlb_cinmlb_1",
                             "2012_04_02_cinmlb_clemlb_1",
                             "2012_04_08_miamlb_cinmlb_1"))

media.sample <- head(d1.2012m)

write.csv(game.sample, file=paste0(save_path, "game_sample2012.csv"), row.names=FALSE)
write.csv(media.sample, file=paste0(save_path, "media_sample2012.csv"), row.names=FALSE)

#Player, Coach, and Umpire
#Run if starting from scratch
# d.2012p <- src_sqlite(paste0(data_path, "d.2012p.sqlite3"), create=TRUE)
# scrape(start="2012-03-28",end="2012-10-01",suffix="players.xml",connect=d.2012p$con)

#Assuming that we have scraped the data already
d.2012p <- src_sqlite(paste0(data_path, "d.2012p.sqlite3"))

d1.2012p <- collect(tbl(d.2012p, "player"))
d1.2012c <- collect(tbl(d.2012p, "coach"))
d1.2012u <- collect(tbl(d.2012p, "umpire"))

player.sample <- d1.2012p %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

coach.sample <- d1.2012c %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

umpire.sample <- d1.2012u %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

write.csv(player.sample, file=paste0(save_path, "player_sample2012.csv"), row.names=FALSE)
write.csv(coach.sample, file=paste0(save_path, "coach_sample2012.csv"), row.names=FALSE)
write.csv(umpire.sample, file=paste0(save_path, "umpire_sample2012.csv"), row.names=FALSE)

#HIP
#Run if starting from scratch
# d.2012h <- src_sqlite(paste0(data_path, "d.2012h.sqlite3"), create=TRUE)
# scrape(start="2012-03-28",end="2012-10-01",suffix="inning/inning_hit.xml",connect=d.2012h$con)

#Assuming that we have scraped the data already
d.2012h <- src_sqlite(paste0(data_path, "d.2012h.sqlite3"))

d1.2012h <- collect(tbl(d.2012h, "hip"))

hip.sample <- d1.2012h %>%
  filter(gameday_link %in% c("gid_2012_03_28_anamlb_cinmlb_1",
                             "gid_2012_04_02_cinmlb_clemlb_1",
                             "gid_2012_04_08_miamlb_cinmlb_1"))

write.csv(hip.sample, file=paste0(save_path, "hip_sample2012.csv"), row.names=FALSE)
