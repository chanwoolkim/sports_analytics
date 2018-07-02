library(pitchRx)
library(plyr)
library(dplyr)
library(RSQLite)
library(devtools)
library(plm)
library(lmtest)
library(clubSandwich)
library(reshape)
memory.limit(size=50000)

###################################################################
# 1. Use PitchRx to extract data
###################################################################

# workaround to get 2017 data
devtools::install_github("pitchRx", "cpsievert")

d.2010i <- src_sqlite("d.2010i.sqlite3")
d.2011i <- src_sqlite("d.2011i.sqlite3")
d.2012i <- src_sqlite("d.2012i.sqlite3")
d.2013i <- src_sqlite("d.2013i.sqlite3")
d.2014i <- src_sqlite("d.2014i.sqlite3")
d.2015i <- src_sqlite("d.2015i.sqlite3")
d.2016i <- src_sqlite("d.2016i.sqlite3")
d.2017i <- src_sqlite("d.2017i.sqlite3")

scrape(start="2010-04-04",end="2010-10-03",suffix="inning/inning_all.xml",connect=d.2010i$con)
scrape(start="2011-03-31",end="2011-09-28",suffix="inning/inning_all.xml",connect=d.2011i$con)
scrape(start="2012-03-28",end="2012-10-01",suffix="inning/inning_all.xml",connect=d.2012i$con)
scrape(start="2013-03-31",end="2013-09-30",suffix="inning/inning_all.xml",connect=d.2013i$con)
scrape(start="2014-03-30",end="2014-09-28",suffix="inning/inning_all.xml",connect=d.2014i$con)
scrape(start="2015-04-05",end="2015-10-01",suffix="inning/inning_all.xml",connect=d.2015i$con)
scrape(start="2016-04-03",end="2016-10-02",suffix="inning/inning_all.xml",connect=d.2016i$con)
scrape(start="2017-04-02",end="2017-10-01",suffix="inning/inning_all.xml",connect=d.2017i$con)


###################################################################
# 2. Create tables for pitch, atbat, runner
###################################################################

# pitch data
d1.2010p <- collect(tbl(d.2010i, "pitch"))
d1.2011p <- collect(tbl(d.2011i, "pitch"))
d1.2012p <- collect(tbl(d.2012i, "pitch"))
d1.2013p <- collect(tbl(d.2013i, "pitch"))
d1.2014p <- collect(tbl(d.2014i, "pitch"))
d1.2015p <- collect(tbl(d.2015i, "pitch"))
d1.2016p <- collect(tbl(d.2016i, "pitch"))
d1.2017p <- collect(tbl(d.2017i, "pitch"))

# exclude all the PITCHf/x pitch description columns for data management
cbind(names(d1.2010p[,c(1:9,36:49)]),names(d1.2011p[,c(1:9,36:49)]),names(d1.2012p[,c(1:9,36:49)]),
      names(d1.2013p[,c(1:9,36:49)]),names(d1.2014p[,c(1:9,36:49)]),names(d1.2015p[,c(1:9,36:49)]),
      names(d1.2016p[,c(1:9,36:47,49:50)]),names(d1.2017p[,c(1:9,36:47,49:50)]))
d2.pitch <- data.frame(rbind(d1.2010p[,c(1:9,36:49)],d1.2011p[,c(1:9,36:49)],d1.2012p[,c(1:9,36:49)],
                             d1.2013p[,c(1:9,36:49)],d1.2014p[,c(1:9,36:49)],d1.2015p[,c(1:9,36:49)],
                             d1.2016p[,c(1:9,36:47,49:50)],d1.2017p[,c(1:9,36:47,49:50)]))
rm(d1.2010p,d1.2011p,d1.2012p,d1.2013p,d1.2014p,d1.2015p,d1.2016p,d1.2017p)

#write.csv(d2.pitch,"d2.pitch.csv")

# atbat data
d1.2010b <- collect(tbl(d.2010i, "atbat"))
d1.2011b <- collect(tbl(d.2011i, "atbat"))
d1.2012b <- collect(tbl(d.2012i, "atbat"))
d1.2013b <- collect(tbl(d.2013i, "atbat"))
d1.2014b <- collect(tbl(d.2014i, "atbat"))
d1.2015b <- collect(tbl(d.2015i, "atbat"))
d1.2016b <- collect(tbl(d.2016i, "atbat"))
d1.2017b <- collect(tbl(d.2017i, "atbat"))


# exclude some columns for data management
cbind(names(d1.2010b[,c(1:6,9,11,12,14:29)]),names(d1.2011b[,c(1:6,9,11,12,14:29)]),names(d1.2012b[,c(1:6,9,11,12,14:29)]),
      names(d1.2013b[,c(1:6,9,11,12,14:29)]),names(d1.2014b[,c(1:6,9,11,12,14:29)]),names(d1.2015b[,c(1:6,9,11,12,14:29)]),
      names(d1.2016b[,c(1:6,9,11,12,14:28,30)]),names(d1.2017b[,c(1:6,9,11,12,14:28,30)]))
d2.atbat <- data.frame(rbind(d1.2010b[,c(1:6,9,11,12,14:29)],d1.2011b[,c(1:6,9,11,12,14:29)],d1.2012b[,c(1:6,9,11,12,14:29)],
                             d1.2013b[,c(1:6,9,11,12,14:29)],d1.2014b[,c(1:6,9,11,12,14:29)],d1.2015b[,c(1:6,9,11,12,14:29)],
                             d1.2016b[,c(1:6,9,11,12,14:28,30)],d1.2017b[,c(1:6,9,11,12,14:28,30)]))
rm(d1.2010b,d1.2011b,d1.2012b,d1.2013b,d1.2014b,d1.2015b,d1.2016b,d1.2017b)

#write.csv(d2.atbat,"d2.atbat.csv")

# runner data
d1.2010r <- collect(tbl(d.2010i, "runner"))
d1.2011r <- collect(tbl(d.2011i, "runner"))
d1.2012r <- collect(tbl(d.2012i, "runner"))
d1.2013r <- collect(tbl(d.2013i, "runner"))
d1.2014r <- collect(tbl(d.2014i, "runner"))
d1.2015r <- collect(tbl(d.2015i, "runner"))
d1.2016r <- collect(tbl(d.2016i, "runner"))
d1.2017r <- collect(tbl(d.2017i, "runner"))

# exclude all the runnerf/x runner description columns for data management
d2.runner <- data.frame(rbind(d1.2010r,d1.2011r,d1.2012r,
                              d1.2013r,d1.2014r,d1.2015r,
                              d1.2016r,d1.2017r))
rm(d1.2010r,d1.2011r,d1.2012r,d1.2013r,d1.2014r,d1.2015r,d1.2016r,d1.2017r)

table(d2.runner$event)
#write.csv(d2.runner,"d2.runner.csv")



###################################################################
# 3. Calculate some basic columns
###################################################################

d2.pitch$event_num <- as.numeric(d2.pitch$event_num)
d2.atbat$event_num <- as.numeric(d2.atbat$event_num)

# link  between tables
d2.pitch$game.bnum <- paste(d2.pitch$gameday_link,d2.pitch$num,sep=".")
d2.atbat$game.bnum <- paste(d2.atbat$gameday_link,d2.atbat$num,sep=".")

# create outs at start of at-bat
d2.atbat$bnum_prev <- d2.atbat$num-1
d2.atbat$game.bnum_prev <- paste(d2.atbat$gameday_link,d2.atbat$bnum_prev,sep=".")
outs.match <- d2.atbat[,c(25,6)]
outs.match2 <- data.frame(game.bnum_prev=outs.match$game.bnum,outs=outs.match$o)
rm(outs.match)
d2a.atbat <- left_join(d2.atbat,outs.match2,by="game.bnum_prev", type="left", match="first")
test <- d2a.atbat[which(d2a.atbat$gameday_link=="gid_2010_04_04_nyamlb_bosmlb_1"),c(1:8,24:28)]
table(d2a.atbat$outs)
d2a.atbat$outs2 <- ifelse(d2a.atbat$outs==3,0,ifelse(is.na(d2a.atbat$outs),0,d2a.atbat$outs))
table(d2a.atbat$outs2)

#write.csv(d2.atbat,"d2.atbat_pre-merge.csv")


# create runs at start of at-bat
# outs method does not work - fill back in using ave formula

# re-order atbat table
d2.atbat <- d2.atbat[order(d2.atbat$gameday_link,d2.atbat$num),]
d2.atbat$bat_team_runs <- as.numeric(d2.atbat$bat_team_runs)

d2.atbat$runs_in_atbat = ave(as.numeric(d2.atbat$bat_team_runs),
                       d2.atbat$gameday_link, d2.atbat$inning, d2.atbat$inning_side,
                       FUN = function(x) c(0, diff(x)))
d2.atbat$runs_inning = ave(as.numeric(d2.atbat$runs_in_atbat),
                     d2.atbat$gameday_link, d2.atbat$inning, d2.atbat$inning_side,FUN = sum)
d2.atbat$cum.runs_inning = ave(as.numeric(d2.atbat$runs_in_atbat),
                         d2.atbat$gameday_link, d2.atbat$inning, d2.atbat$inning_side,FUN = cumsum)
d2.atbat$cum.runs_start_atbat <- d2.atbat$cum.runs_inning - d2.atbat$runs_in_atbat
test <- d2.atbat[which(d2.atbat$gameday_link=="gid_2010_04_04_nyamlb_bosmlb_1"),c(1:12,28:33)]

table(d2.atbat$runs_in_atbat)

cbind(d2.atbat$runs_inning,d2.atbat$total.runs.scored)

# create runners on base
d2.pitch$on.base1 <- ifelse(is.na(d2.pitch$on_1b)==TRUE,0,1)
d2.pitch$on.base2 <- ifelse(is.na(d2.pitch$on_2b)==TRUE,0,1)
d2.pitch$on.base3 <- ifelse(is.na(d2.pitch$on_3b)==TRUE,0,1)
d2.pitch$on.base <- paste0(d2.pitch$on.base1,d2.pitch$on.base2,d2.pitch$on.base3)
levels(d2.pitch$on.base) <- c("000", "100", "010", "001","110", "101", "011","111")
table(d2.pitch$on.base)

#write.csv(d2.pitch,"d2.pitch_pre-merge.csv")

d2.atbat$home_team_runs <- as.numeric(d2.atbat$home_team_runs)
d2.atbat$away_team_runs <- as.numeric(d2.atbat$away_team_runs)
d2.atbat$season <- substr(d2.atbat$date, 1, 4)

cbind(names(d2.atbat))
cbind(names(d2.pitch))

# join necessary columns
d2 <- join(d2.pitch[,c(3,20,28,24)],d2.atbat[,c(3,7,9,11:12,14:15,17:20,22,24,28,30,31:33,25)],by="game.bnum", type="left", match="first")
test2 <- d2[which(d2$gameday_link=="gid_2010_04_04_nyamlb_bosmlb_1"),]
d2 <- join(d2,d2.atbat[,c(25,34)],by="game.bnum", type="left", match="first")

# calculate runs scored to end half inning
d2$runs.to.end.inning <- d2$runs_inning - d2$cum.runs_start_atbat

table(d2$runs.to.end.inning)

d2star <- d2[-which(is.na(d2$runs.to.end.inning)),]
aggregate(d2star$runs.to.end.inning, by = c(list(d2star$on.base),list(d2star$outs2),list(d2star$season)), mean)



#write.csv(d2,"d2.5-16.1301.csv")

##############################################################
# calculate SLG+BB, FIP
##############################################################

# table allows to calculate stats
event <- read.csv("event.code.csv")

# batters
batter.piv2 <- aggregate(d2.atbat$batter_name, by = c(list(d2.atbat$batter_name),list(d2.atbat$event),list(d2.atbat$season)), length)
colnames(batter.piv2)[2] <- "batter_name"
colnames(batter.piv2)[1] <- "event"
colnames(batter.piv2)[3] <- "season"
colnames(batter.piv2)[4] <- "count"

batter.piv2 <- join(batter.piv2,event,by="event", type="left", match="first")
head(batter.piv2)
batter.piv2$AB <- batter.piv2$count*batter.piv2$AB.flag
batter.piv2$PA <- batter.piv2$count*batter.piv2$PA.flag
batter.piv2$SLG_BB <- batter.piv2$count*batter.piv2$SLG.mult
batter.piv2$Walks <- batter.piv2$count*batter.piv2$Walk.flag
batter.piv2$Singles <- batter.piv2$count*batter.piv2$Single.flag
batter.piv2$XBHs <- batter.piv2$count*batter.piv2$XBH.flag

batter.piv2$AB.total = ave(batter.piv2$AB, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$PA.total = ave(batter.piv2$PA, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$SLG_BB.sum = ave(batter.piv2$SLG_BB, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$Walks.sum = ave(batter.piv2$Walks, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$Singles.sum = ave(batter.piv2$Singles, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$XBHs.sum = ave(batter.piv2$XBHs, batter.piv2$batter_name, batter.piv2$season, FUN = sum)
batter.piv2$SLG_BB.pct = batter.piv2$SLG_BB.sum/batter.piv2$PA.total
batter.piv2$Walks.pct = batter.piv2$Walks.sum/batter.piv2$PA.total
batter.piv2$Singles.pct = batter.piv2$Singles.sum/batter.piv2$PA.total
batter.piv2$XBHs.pct = batter.piv2$XBHs.sum/batter.piv2$PA.total
batter.piv2$batter_name.season <- paste(batter.piv2$batter_name,batter.piv2$season,sep=".")

plot(batter.piv2$SLG_BB.pct~batter.piv2$PA.total)

plot(batter.piv2$SLG_BB.pct~batter.piv2$PA.total, ylim=c(0,2),
     ylab="SLG+BB by batter and season", xlab="Plate appearance by batter and season",
     main="Batter SLG+BB by Plate Appearances", pch=16,col="black", cex=0.5)
abline(v=100)


abline(v=100)
hist(batter.piv1[which(batter.piv1$PA>100),]$SLG_BB.pct,
     col=grey(0.7),main="Batter SLG+BB (PA>100)", 
     ylab="Count of batter and season", xlab = "SLG+BB")
hist(batter.piv1a$SLG_BB.pct,
     col=grey(0.7),main="Batter SLG+BB (PA>100)", 
     xlab = "SLG+BB by batter and season")


# batter/season for mean calculation
batter.piv1 <- aggregate(d2.atbat$batter_name, by = c(list(d2.atbat$batter_name),list(d2.atbat$season)), length)
colnames(batter.piv1)[1] <- "batter_name"
colnames(batter.piv1)[2] <- "season"
colnames(batter.piv1)[3] <- "count"
batter.piv1$batter_name.season <- paste(batter.piv1$batter_name,batter.piv1$season,sep=".")
batter.piv1 <- join(batter.piv1, batter.piv2[,c(20,25:29)], by="batter_name.season", type="left", match="first")
batter.piv1a <- batter.piv1[which(batter.piv1$PA.total>100),]

batter.mean <- data.frame("SLG_BB.pct"=mean(batter.piv1a$SLG_BB.pct),"Walks.pct"=mean(batter.piv1a$Walks.pct),
                     "Singles.pct"=mean(batter.piv1a$Singles.pct),"XBHs.pct"=mean(batter.piv1a$XBHs.pct))





# pitchers
pitcher.piv2 <- aggregate(d2.atbat$pitcher_name, by = c(list(d2.atbat$pitcher_name),list(d2.atbat$event),list(d2.atbat$season)), length)
colnames(pitcher.piv2)[1] <- "pitcher_name"
colnames(pitcher.piv2)[2] <- "event"
colnames(pitcher.piv2)[3] <- "season"
colnames(pitcher.piv2)[4] <- "count"
pitcher.piv2 <- join(pitcher.piv2,event,by="event", type="left", match="first")
pitcher.piv2$BF <- pitcher.piv2$count
pitcher.piv2$Outs <- pitcher.piv2$count*pitcher.piv2$Outs.mult
pitcher.piv2$IP <- pitcher.piv2$Outs/3
pitcher.piv2$FIP <- pitcher.piv2$count*pitcher.piv2$FIP.mult

pitcher.piv2$IP.total = ave(pitcher.piv2$IP, pitcher.piv2$pitcher_name, pitcher.piv2$season, FUN = sum)
pitcher.piv2$FIP.sum = ave(pitcher.piv2$FIP, pitcher.piv2$pitcher_name, pitcher.piv2$season, FUN = sum)
pitcher.piv2$FIP.avg = pitcher.piv2$FIP.sum/pitcher.piv2$IP.total
pitcher.piv2$FIP.avg = pitcher.piv2$FIP.avg + 3.11
pitcher.piv2$pitcher_name.season <- paste(pitcher.piv2$pitcher_name,pitcher.piv2$season,sep=".")

pitcher.piv2[which(pitcher.piv2$pitcher_name=="Brett Anderson"),]

plot(pitcher.piv1$FIP.avg~pitcher.piv1$IP.total, ylim=c(-1,20),
     ylab="FIP by pitcher and season", xlab="Innings pitched by pitcher and season",
     main="Pitcher FIP by Innings Pitched", pch=16,col="black", cex=0.5)
abline(v=15)


hist(pitcher.piv1a$FIP,
     col=grey(0.7),main="Pitcher FIP (IP>15)", 
     xlab = "FIP", ylab="Count of pitcher and season", xlim=c(0,10))

hist(pitcher.piv1$IP.total,
     col=grey(0.7),main="Innings Pitched by Pitcher and Season", 
     ylab="Count of pitcher and season", xlab = "IP", ylim=c(0,2000))


# batter/season for mean calculation
pitcher.piv1 <- aggregate(d2.atbat$pitcher_name, by = c(list(d2.atbat$pitcher_name),list(d2.atbat$season)), length)
colnames(pitcher.piv1)[1] <- "pitcher_name"
colnames(pitcher.piv1)[2] <- "season"
colnames(pitcher.piv1)[3] <- "count"
pitcher.piv1$pitcher_name.season <- paste(pitcher.piv1$pitcher_name,pitcher.piv1$season,sep=".")
pitcher.piv1 <- join(pitcher.piv1, pitcher.piv2[,c(17,19,20)], by="pitcher_name.season", type="left", match="first")
pitcher.piv1a <- pitcher.piv1[which(pitcher.piv1$IP.total>15),]

pitcher.mean <- data.frame("FIP.avg"=mean(pitcher.piv1a$FIP.avg))

cbind(names(pitcher.piv2))

# match column
d2star$pitcher_name.season <- paste(d2star$pitcher_name,d2star$season,sep=".")
d2star$batter_name.season <- paste(d2star$batter_name,d2star$season,sep=".")

# merge
d2star <- join(d2star,pitcher.piv2[,c(17,19:20)],by="pitcher_name.season", type="left", match="first")
d2star <- join(d2star,batter.piv2[,c(20,25:29)],by="batter_name.season", type="left", match="first")

d2star$FIPu <- d2star$FIP.avg - pitcher.mean$FIP.avg
d2star$SLG_BBu <- d2star$SLG_BB.pct - batter.mean$SLG_BB.pct


###############################################################################
# Data Flag
###############################################################################
d2star$data.flag <- ifelse(abs(d2star$inning)<7,"Clean","Questionable")
d2star$data.flag[which(is.na(d2star$pitcher_name))] <- "Questionable"
d2star$data.flag[which(is.na(d2star$batter_name))] <- "Questionable"
d2star$data.flag[which(is.na(d2star$FIP))] <- "Questionable"
d2star$data.flag[which(is.na(d2star$wBSA))] <- "Questionable"
d2star$data.flag[which(d2star$PA<100)] <- "Questionable"
d2star$data.flag[which(d2star$IP<15)] <- "Questionable"
table(d2star$data.flag)

d2c <- d2star[which(d2star$data.flag=="Clean"),]

names(d2c)
###############################################################################
# A few extra things for final data set
###############################################################################

# forgot pitcher throw var, add this to add in
d2.atbat.throw <- data.frame(rbind(d1.2010b[,c(11,3,27)],d1.2011b[,c(11,3,27)],d1.2012b[,c(11,3,27)],
                                   d1.2013b[,c(11,3,27)],d1.2014b[,c(11,3,27)],d1.2015b[,c(11,3,27)],
                                   d1.2016b[,c(11,3,27)],d1.2017b[,c(11,3,27)]))
d2.atbat.throw$game.bnum <- paste(d2.atbat.throw$gameday_link,d2.atbat.throw$num,sep=".")
head(d2.atbat.throw)

# add pitcher throws data
d2c <- join(d2c,d2.atbat.throw[,c(1,4)],by="game.bnum", type="left", match="first")
rm(d2.atbat.throw)

# add pitcher throws data permanent to d2.atbat
d2.atbat <- join(d2.atbat,d2.atbat.throw[,c(1,4)],by="game.bnum", type="left", match="first")
rm(d2.atbat.throw)
d2.atbat$pbrl <- paste(d2.atbat$p_throws,d2.atbat$stand,sep="/")
table(d2.atbat$pbrl)



# add throws/stance var
d2c$pbrl <- paste0(d2c$p_throws,d2c$stand)
table(d2c$pbrl)

# calculate next batter
# table already sorted by bnum, now will be sorted by all home team then all away team
# this is WRONG for the last batter in the game for each side but we are excluding these anyway
d2.atbat.next <- d2.atbat[order(d2.atbat$inning_side),]
lg <- function(x)c(x[2:(length(x))],NA)
d2.atbat.next$next1_name <- lg(d2.atbat.next$batter_name)
d2.atbat.next$next1_name.season <- paste(d2.atbat.next$next1_name,d2.atbat.next$season,sep=".")

d2c <- join(d2c,d2.atbat.next[,c(25,35,36)],by="game.bnum", type="left", match="first")
batter.piv2$next1_name.season <- batter.piv2$batter_name.season
d2c <- join(d2c,batter.piv2[,c(30,20,25,28)],by="next1_name.season", type="left", match="first")
head(d2c)
colnames(d2c)[41] <- "next1_PA.total"
colnames(d2c)[42] <- "next1_SLG_BBu"
d2c$next1_SLG_BBu <- d2c$next1_SLG_BBu - batter.mean$SLG_BB.pct
d2c$next1_SLG_BBux01outs <- d2c$next1_SLG_BBu* ifelse(d2c$outs2<2,1,0)

d2c$data.flag <- "Clean"
d2c$data.flag[which(is.na(d2c$FIPu))] <- "Questionable"
d2c$data.flag[which(is.na(d2c$SLG_BBu))] <- "Questionable"
d2c$data.flag[which(d2c$next1_PA.total<100)] <- "Questionable"
d2c$data.flag[which(is.na(d2c$next1_SLG_BBux01outs))] <- "Questionable"
table(d2c$data.flag)

d2c <- d2c[which(d2c$data.flag=="Clean"),]

plot(runs.to.end.inning ~ SLG_BBu, data=d2c[which(d2c$data.flag[1:100000]=="Clean"),])


# find batting team
d2c$awayteam <- substr(d2c$gameday_link, 16, 18)
d2c$hometeam <- substr(d2c$gameday_link, 23, 25)
d2c$defteam <- ifelse(d2c$inning_side=="top",d2c$hometeam,d2c$awayteam)
d2c$defteam.season <- paste(d2c$defteam,d2c$season,sep=".")

cbind(table(d2c$awayteam),table(d2c$hometeam))
aggregate(d2c$season, by = c(list(d2c$hometeam),list(d2c$season)), length)

defense <- read.csv("def.uzr.csv")
d2c <- join(d2c,defense[,4:5],by="defteam.season", type="left", match="first")
summary(d2c$UZR)

hist(defense$UZR,
     col=grey(0.7),main="Fielding UZR", 
     xlab = "UZR", ylab="Count of team and season", xlim=c(-70,70))


# missing for allstar games
d2c$data.flag[which(is.na(d2c$UZR))] <- "Questionable"
table(d2c$data.flag)
d2c <- d2c[which(d2c$data.flag=="Clean"),]


# other histograms
hist1 <- data.frame(table(d2.pitch$count))
barplot(height=hist1$Freq, horiz=FALSE, names=hist1$Var1, 
        cex.names=0.9, cex.axis = .9, xlab="Count", 
        ylab="Count of pitches",main="Count")

hist2 <- data.frame(table(d2.atbat$outs2))
barplot(height=hist2$Freq, horiz=FALSE, names=hist2$Var1, 
        cex.names=0.9, cex.axis = .9, ylim=c(0,500000),
        xlab="Outs at start of at bat", ylab="Count of at bats",main="Outs by At Bats")

hist3 <- data.frame(table(d2.atbat$on.base))
hist3
hist3a <- hist3[c(1,5,3,2,7,6,4,8),]
barplot(height=hist3a$Freq, horiz=FALSE, names=hist3a$Var1, 
        cex.names=0.9, cex.axis = .9, xlab="Runners on base at start of at bat", 
        ylab="Count of at bats", main="Runners On Base by At Bats")

hist4 <- data.frame(table(d2.atbat$pbrl))
hist4 <- hist4[c(4,3,2,1),]
barplot(height=hist4$Freq, horiz=FALSE, names=hist4$Var1, 
        cex.names=0.9, cex.axis = .9, xlab="Pitcher throw/ Batter stance", 
        ylim=c(0,600000), ylab="Count of at bats",main="Pitcher throw/Batter stance by At Bats")

hist5 <- data.frame(table(d2$season))
barplot(height=hist5$Freq, horiz=FALSE, names=hist5$Var1, 
        cex.names=0.9, cex.axis = .9, xlab="Season", 
        ylim=c(0,800000), ylab="Count of pitches",
        main="Pitches Thrown by Season")

hist(runs$total.runs.scored,
     col=grey(0.7),main="Total Runs Scored by Half Inning", 
     xlab = "Runs in half inning", ylab="Count of Half Inning", 
     xlim=c(-1,12))

hist(d2.pitch$count,
     col=grey(0.7),main="Count by Number of Pitches", 
     xlab = "Count")

rbind(quantile(batter.piv1a$SLG_BB.pct,c(0.05,0.25,0.5,0.75,0.95)))
rbind(quantile(pitcher.piv1a$FIP,c(0.05,0.25,0.5,0.75,0.95)))
rbind(quantile(defense$UZR,c(0.05,0.25,0.5,0.75,0.95)))

names(d2.pitch)
d2$game.inning.half <- paste(d2$gameday_link,d2$inning,d2$inning_side,sep=".")
bnum.max <- aggregate(d2$num,by=list(d2$game.inning.half),max)
colnames(bnum.max)[1] <- "game.inning.half"
colnames(bnum.max)[2] <- "bnum.max"
bnum.max$game.bnum <- paste(substr(bnum.max$game.inning.half, 1, 30),bnum.max$bnum.max,sep=".")

# pitcher in last pitch of half inning
bnum.max <- join(bnum.max,d2[,c(2,16)],by="game.bnum", type="left", match="first")
colnames(bnum.max)[4] <- "Inn.End.P"
head(bnum.max)

bnum.min <- aggregate(d2$num,by=list(d2$game.inning.half),min)
colnames(bnum.min)[1] <- "game.inning.half"
colnames(bnum.min)[2] <- "bnum.min"
bnum.min$game.bnum <- paste(substr(bnum.min$game.inning.half, 1, 30),bnum.min$bnum.min,sep=".")

# pitcher in first pitch of half inning
bnum.min <- join(bnum.min,d2[,c(2,16)],by="game.bnum", type="left", match="first")
colnames(bnum.min)[4] <- "Inn.Start.P"
inning.onepitcher <- join(bnum.max,bnum.min[,c(2,4)],by="game.inning.half", type="left", match="first")
inning.onepitcher$one <- ifelse(inning.onepitcher$Inn.End.P==inning.onepitcher$Inn.Start.P,1,0)
colnames(inning.onepitcher)[1] <- "game.inning.halftext"
head(inning.onepitcher)
inning.onepitcher$game.inning.half <- ifelse(nchar(inning.onepitcher$game.inning.halftext)==36,
                                         substr(inning.onepitcher$game.inning.halftext, 1, 32), 
                                         paste0(substr(inning.onepitcher$game.inning.halftext, 1, 33),5))
runs <- join(runs,inning.onepitcher[,c(4,6,7)],by="game.inning.half", type="left", match="first")
runs$pitcher_name.season <- paste(runs$Inn.End.P,runs$season,sep=".")
runs <- join(runs,pitcher.piv1a[,c(1,6)],by="pitcher_name.season", type="left", match="first")
runs$FIPround <- round(runs$FIP.avg,0)
runs <- join(runs,d2.atbat[,c(2,35)],by="game.inning.half",type="left",match="first")

runs.pitch <- runs[which(runs$one==1),]
runs.pitch <- runs.pitch[!is.na(runs.pitch$FIP.avg),]
runs.pitch <- runs.pitch[!is.na(runs.pitch$total.runs.scored),]
head(runs.pitch)
hist(runs.pitch$total.runs.scored)
names(d2.atbat)

# seems like runs_inning is correct and total.runs.inning is wrong
runs.pitch <- join(runs.pitch,d2.atbat[,c(2,35)],by="game.inning.half",type="left",match="first")
avg1 <- aggregate(runs.pitch$total.runs.scored,by=list(runs.pitch$FIPround),mean)
avg1$x2 <- avg1$x/2
barplot(height=avg1$x2, horiz=FALSE, names=avg1$Group.1, 
        cex.names=0.9, cex.axis = .9, xlab="FIP (rounded to nearest 1)", 
        ylim=c(0,1.4), ylab="Average runs scored per inning",
        main="Avg Runs per Half Inning by Pitcher FIP (IP>15)")
aggregate(runs.pitch$total.runs.scored,by=list(runs.pitch$Inn.End.P),mean)

avg1a <- aggregate(runs.pitch$runs_inning,by=list(runs.pitch$FIPround),mean)
barplot(height=avg1a$x, horiz=FALSE, names=avg1a$Group.1, 
        cex.names=0.9, cex.axis = .9, xlab="FIP (rounded to nearest 1)", 
        ylim=c(0,0.7), ylab="Average runs scored per inning",
        main="Avg Runs per Half Inning by Pitcher FIP (IP>15)")
aggregate(runs.pitch$total.runs.scored,by=list(runs.pitch$Inn.End.P),mean)


# uzr plot
runs$awayteam <- substr(runs$game.inning.half, 16, 18)
runs$hometeam <- substr(runs$game.inning.half, 23, 25)
runs$season <- substr(runs$game.inning.half, 5, 8)
runs$defteam <- ifelse(nchar(runs$game.inning.half)==32,d2c$hometeam,d2c$awayteam)
runs$defteam.season <- paste(runs$defteam,runs$season,sep=".")

runs <- join(runs,defense[,c(4,5)],by="defteam.season", type="left", match="first")
runs$UZRround <- ifelse(runs$UZR<(-30),"<-30",ifelse(runs$UZR<(-10),"-30 to -10",ifelse(runs$UZR<10,"-10 to 10",
                        ifelse(runs$UZR<30,"10 to 30", ">30")))) 
runs.defense <- runs[!is.na(runs$runs_inning),]
avg2 <- aggregate(runs.defense$runs_inning,by=list(runs.defense$UZRround),mean)
avg2 <- avg2[c(3,2,1,5,4),]
barplot(height=avg2$x, horiz=FALSE, names=avg2$Group.1, 
        cex.names=0.9, cex.axis = .9, xlab="UZR group", 
        ylim=c(0,0.7),ylab="Average runs scored per inning",
        main="Avg Runs per Half Inning by Defense UZR")
hist(runs.defense$UZR)

avg2 <- aggregate(runs.defense$total.runs.scored,by=list(runs.defense$defteam.season),sum)
colnames(avg2)[1] <- "defteam.season"
avg2 <- join(avg2,defense[,c(4,5)],by="defteam.season",type="left",match="first")
avg2$UZRround <- ifelse(avg2$UZR<(-30),"<-30",ifelse(avg2$UZR<(-10),"-30 to -10",ifelse(avg2$UZR<10,"-10 to 10",
                                                                                        ifelse(avg2$UZR<30,"10 to 30", ">30")))) 
table(runs$defteam.season)
sum(runs.defense$total.runs.scored)

d2.atbat$inninghalf <- ifelse(d2.atbat$inning_side=="bottom",d2.atbat$inning+0.5,d2.atbat$inning)
d2.atbat$game.inning.half <- paste(d2.atbat$gameday_link,d2.atbat$inninghalf,sep=".")
d2.atbat <- join(d2.atbat,runs[,c(3,6)],by="game.inning.half",type="left",match="first")
cbind(names(d2.atbat))

d2.atbat$batter_name.season <- paste(d2.atbat$batter_name,d2.atbat$season,sep=".")
d2.atbat <- join(d2.atbat, batter.piv2[,c(29,25)], by="batter_name.season", type="left", match="first")
d2.atbat$SLG_BB.round <- round(d2.atbat$SLG_BB.pct,1)

avg3 <- aggregate(d2.atbat[which(!is.na(d2.atbat$total.runs.scored)),]$total.runs.scored,
                  by=list(d2.atbat[which(!is.na(d2.atbat$total.runs.scored)),]$SLG_BB.round),mean)

rbind(mean(runs.defense$runs_inning),sd(runs.defense$runs_inning),
      batter.mean$SLG_BB.pct,sd(batter.piv1a$SLG_BB.pct),
      0,sd(batter.piv1a$SLG_BB.pct),
      pitcher.mean$FIP.avg,sd(pitcher.piv1a$FIP.avg),
      0,sd(pitcher.piv1a$FIP.avg),
      mean(defense$UZR),sd(defense$UZR))
summary(runs.defense$total.runs.scored)

###############################################################################
# Add in steals
###############################################################################

d2.steals <- read.csv("steals_events.csv")
colnames(d2.steals)[6] <- "on_1b.name"
names(d2.steals)
d2.steals$game.id <- paste(d2.steals$gameday_link,d2.steals$id,sep=".")
d2$game.id <- paste(d2$gameday_link,d2$id,sep=".")
d2 <- join(d2,d2.steals[,c(6:8)],by="game.id", type="left", match="first")
d2.steals$awayteam <- substr(d2.steals$gameday_link, 16, 18)
d2.steals$hometeam <- substr(d2.steals$gameday_link, 23, 25)
d2.steals$offteam <- ifelse(d2.steals$inning_side=="bottom",d2.steals$hometeam,d2.steals$awayteam)
d2.steals$defteam <- ifelse(d2.steals$inning_side=="top",d2.steals$hometeam,d2.steals$awayteam)
d2.steals$offteam.season <- paste(d2.steals$offteam,d2.steals$season,sep=".")
d2.steals$defteam.season <- paste(d2.steals$defteam,d2.steals$season,sep=".")

table(d2.steals$offteam.season,d2.steals$event_steal)
table(d2.steals$defteam.season,d2.steals$event_steal)

d2 <- join(d2,d2.steals[,c(6:8)],by="game.id", type="left", match="first")
table(d2[which(d2$on.base=="100"),]$count,d2[which(d2$on.base=="100"),]$event_steal)


table(d2[which(d2$on.base=="100"),]$outs2,d2[which(d2$on.base=="100"),]$event_steal)
d2$batter_name.season <- paste(d2$batter_name,d2$season,sep=".")
d2 <- join(d2,d2.steals[,c(6:8)],by="game.id", type="left", match="first")
d2 <- join(d2, batter.piv2[,c(20,25,29)], by="batter_name.season", type="left", match="first")

d2$SLG_BB.round <- ifelse(d2$PA<100,NA,round(d2$SLG_BB.pct,1))
table(d2[which(d2$on.base=="100"),]$SLG_BB.round,d2[which(d2$on.base=="100"),]$event_steal)
table(d2$SLG_BB.round)

table(d2$event_steal,d2$on.base)
d2$runner1B_attempt <- ifelse(d2$on.base=="100",ifelse(is.na(d2$event_steal),0,1),NA)
table(d2$runner1B_attempt)
table(d2$on.base)
table(d2$count,d2$event_steal)
names(d2)

# aggregate over at bat
d2.steals.atbat <- d2[which(d2$runner1B_attempt==1),c(1,2,5,26:28)]
d2.atbat <- join(d2.atbat,d2.steals.atbat[,c(2,4:6)],by="game.bnum", type="left", match="first")

# need starting on base for every atbat
d2.atbat <- join(d2.atbat,d2[,c(2,5)],by="game.bnum", type="left", match="first")
table(d2.atbat$on.base,d2.atbat$event_steal)
d2.atbat$runner1B_attempt <- ifelse(d2.atbat$on.base=="100",ifelse(is.na(d2.atbat$event_steal),0,1),999)
d2.atbat$runner1B_success <- ifelse(d2.atbat$on.base=="100",ifelse(is.na(d2.atbat$event_steal),"no attempt",
                             ifelse(d2.atbat$event_steal=="Stolen Base 2B","success","fail")),"not on.base100")
table(d2.atbat$runner1B_success)
table(d2.atbat$season,d2.atbat$runner1B_attempt)
table(d2.atbat$season,d2.atbat$runner1B_success)
names(d2.atbat)

d2.atbat$year_month <- paste(d2.atbat$season,substr(d2.atbat$date, 6, 7),sep="_")
steals.100.yearmo <-  table(d2.atbat$year_month,d2.atbat$runner1B_success)

names(d2.atbat)

# look up good base stealers (10 or more attempts)
d2.steals$season <- substr(d2.steals$gameday_link, 5, 8)
d2.steals$success <- ifelse(d2.steals$event_steal=="Stolen Base 2B",1,0)
d2.steals$attempt <- 1

steal.piv1 <- aggregate(d2.steals$attempt, by = c(list(d2.steals$on_1b.name),list(d2.steals$season)), sum)

colnames(steal.piv1)[1] <- "on_1b.name"
colnames(steal.piv1)[2] <- "season"
colnames(steal.piv1)[3] <- "attempts"
hist(steal.piv1$attempts)
summary(steal.piv1$attempts)
steal.piv1$ten.attempts <- ifelse(steal.piv1$attempts>9,"10+","<10")
steal.piv1$on_1bname.season <- paste(steal.piv1$on_1b.name,steal.piv1$season,sep=".")

# need to know who is on first all the time
# add on_1b code from pitch table
d2.atbat <- join(d2.atbat,d2.pitch[,c(24,17)],by="game.bnum", type="left", match="first")

# convert to on_1b.name
colnames(d2.atbat.next)[2]<-"on_1b"
d2.atbat <- join(d2.atbat,d2.atbat.next[,c(2,19)],by="on_1b", type="left", match="first")
colnames(d2.atbat.next)[2]<-"batter"
colnames(d2.atbat)[46]<-"on_1b.name"
colnames(d2.atbat)[37]<-"stealer_1b.name"
d2.atbat$on_1bname.season <- paste(d2.atbat$on_1b.name,d2.atbat$season,sep=".")
d2.atbat <- join(d2.atbat,steal.piv1[,c(3:5)],by="on_1bname.season", type="left", match="first")
names(d2.atbat)
d2.atbat$steal.outcome.skill <- paste(d2.atbat$runner1B_success,d2.atbat$ten.attempts,sep=".")
table(d2.atbat$runner1B_success,d2.atbat$ten.attempts)

steals.ob100 <- data.frame(table(d2.atbat$year_month,d2.atbat$steal.outcome.skill))
write.csv(steals.ob100,"steals.csv")

head(d1.game)
d1.game$season <-  substr(d1.game$id, 1, 4)
d1.game$month <- substr(d1.game$id,5,6)
d1.game$yrmo <-  substr(d1.game$id, 1, 7)
cbind(table(d1.game$yrmo))



###############################################################################
# Regression
###############################################################################

write.csv(d2c,"d2c.5-27.2112.csv")
cbind(names(d2c))
write.csv(d2c[,c(3,6,7,8,14,22,27,37,38,40,48,49)],"d2c.6-1.0902.csv")

memory.limit(size=50000)
options(scipen = 999)





rm(r1)
r1 <- lm(runs.to.end.inning~  on.base*as.factor(outs2) + count + 
            FIPu + SLG_BBu + SLG_BBu*on.base + pbrl + UZR + inning_side, 
           data=d2c)
summary(r1)
object.size(r1)

d2c$game.inning.half <- paste(d2c$gameday_link,d2c$inning,d2c$inning_side,sep=".")
r1.cluster1 <- coef_test(r1, vcov="CR1", cluster=d2c$game.inning.half, test="z")


r1.cluster1

??coef_test

