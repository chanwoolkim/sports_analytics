library(pitchRx)
library(plyr)
library(dplyr)
library(RSQLite)
library(devtools)
library(plm)
library(lmtest)
library(clubSandwich)
library(reshape)

d.2010i <- src_sqlite("d.2010i.sqlite3", create=TRUE)
d.2011i <- src_sqlite("d.2011i.sqlite3", create=TRUE)
d.2012i <- src_sqlite("d.2012i.sqlite3", create=TRUE)
d.2013i <- src_sqlite("d.2013i.sqlite3", create=TRUE)
d.2014i <- src_sqlite("d.2014i.sqlite3", create=TRUE)
d.2015i <- src_sqlite("d.2015i.sqlite3", create=TRUE)
d.2016i <- src_sqlite("d.2016i.sqlite3", create=TRUE)
d.2017i <- src_sqlite("d.2017i.sqlite3", create=TRUE)

scrape(start="2010-04-04",end="2010-10-03",suffix="inning/inning_all.xml",connect=d.2010i$con)
scrape(start="2011-03-31",end="2011-09-28",suffix="inning/inning_all.xml",connect=d.2011i$con)
scrape(start="2012-03-28",end="2012-10-01",suffix="inning/inning_all.xml",connect=d.2012i$con)
scrape(start="2013-03-31",end="2013-09-30",suffix="inning/inning_all.xml",connect=d.2013i$con)
scrape(start="2014-03-30",end="2014-09-28",suffix="inning/inning_all.xml",connect=d.2014i$con)
scrape(start="2015-04-05",end="2015-10-01",suffix="inning/inning_all.xml",connect=d.2015i$con)
scrape(start="2016-04-03",end="2016-10-02",suffix="inning/inning_all.xml",connect=d.2016i$con)
scrape(start="2017-04-02",end="2017-10-01",suffix="inning/inning_all.xml",connect=d.2017i$con)

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

d1.2010b <- d1.2010b %>%
  mutate(season = "2010") %>%
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

d1.2011b <- d1.2011b %>%
  mutate(season = "2011") %>%
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

d1.2012b <- d1.2012b %>%
  mutate(season = "2012") %>%
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

d1.2013b <- d1.2013b %>%
  mutate(season = "2013") %>%
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

d1.2014b <- d1.2014b %>%
  mutate(season = "2014") %>%
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

d1.2015b <- d1.2015b %>%
  mutate(season = "2015") %>%
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

d1.2016b <- d1.2016b %>%
  mutate(season = "2016") %>%
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

d1.2017b <- d1.2017b %>%
  mutate(season = "2017") %>%
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

d1.2010p <- d1.2010p %>%
  mutate(season = "2010") %>%
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

d1.2011p <- d1.2011p %>%
  mutate(season = "2011") %>%
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

d1.2012p <- d1.2012p %>%
  mutate(season = "2012") %>%
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

d1.2013p <- d1.2013p %>%
  mutate(season = "2013") %>%
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

d1.2014p <- d1.2014p %>%
  mutate(season = "2014") %>%
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

d1.2015p <- d1.2015p %>%
  mutate(season = "2015") %>%
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

d1.2016p <- d1.2016p %>%
  mutate(season = "2016") %>%
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

d1.2017p <- d1.2017p %>%
  mutate(season = "2017") %>%
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

d1.2010r <- d1.2010r %>%
  mutate(season = "2010") %>%
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

d1.2011r <- d1.2011r %>%
  mutate(season = "2011") %>%
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

d1.2012r <- d1.2012r %>%
  mutate(season = "2012") %>%
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

d1.2013r <- d1.2013r %>%
  mutate(season = "2013") %>%
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

d1.2014r <- d1.2014r %>%
  mutate(season = "2014") %>%
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

d1.2015r <- d1.2015r %>%
  mutate(season = "2015") %>%
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

d1.2016r <- d1.2016r %>%
  mutate(season = "2016") %>%
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

d1.2017r <- d1.2017r %>%
  mutate(season = "2017") %>%
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

write.csv(d1.2010b, file="data/2010_atbat.csv", row.names=FALSE)
write.csv(d1.2010p, file="data/2010_pitch.csv", row.names=FALSE)
write.csv(d1.2010r, file="data/2010_runner.csv", row.names=FALSE)

write.csv(d1.2011b, file="data/2011_atbat.csv", row.names=FALSE)
write.csv(d1.2011p, file="data/2011_pitch.csv", row.names=FALSE)
write.csv(d1.2011r, file="data/2011_runner.csv", row.names=FALSE)

write.csv(d1.2012b, file="data/2012_atbat.csv", row.names=FALSE)
write.csv(d1.2012p, file="data/2012_pitch.csv", row.names=FALSE)
write.csv(d1.2012r, file="data/2012_runner.csv", row.names=FALSE)

write.csv(d1.2013b, file="data/2013_atbat.csv", row.names=FALSE)
write.csv(d1.2013p, file="data/2013_pitch.csv", row.names=FALSE)
write.csv(d1.2013r, file="data/2013_runner.csv", row.names=FALSE)

write.csv(d1.2014b, file="data/2014_atbat.csv", row.names=FALSE)
write.csv(d1.2014p, file="data/2014_pitch.csv", row.names=FALSE)
write.csv(d1.2014r, file="data/2014_runner.csv", row.names=FALSE)

write.csv(d1.2015b, file="data/2015_atbat.csv", row.names=FALSE)
write.csv(d1.2015p, file="data/2015_pitch.csv", row.names=FALSE)
write.csv(d1.2015r, file="data/2015_runner.csv", row.names=FALSE)

write.csv(d1.2016b, file="data/2016_atbat.csv", row.names=FALSE)
write.csv(d1.2016p, file="data/2016_pitch.csv", row.names=FALSE)
write.csv(d1.2016r, file="data/2016_runner.csv", row.names=FALSE)

write.csv(d1.2017b, file="data/2017_atbat.csv", row.names=FALSE)
write.csv(d1.2017p, file="data/2017_pitch.csv", row.names=FALSE)
write.csv(d1.2017r, file="data/2017_runner.csv", row.names=FALSE)