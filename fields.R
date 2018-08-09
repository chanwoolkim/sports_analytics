library(pitchRx)
library(xlsx)

save_path <- "/Volumes/huizinga/MLB/Chanwool/"

varlist <- c("action", "atbat", "coach", "game", "hip", "media", "pitch", "player", "po", "runner", "umpire")

table <- fields

for (v in varlist) {
  assign(v,
         data.frame(var = names(table[[v]]),
                    type = unname(table[[v]])))
}

write.xlsx(action, file=paste0(save_path, "fields.xlsx"), sheetName="action", row.names=FALSE)
write.xlsx(atbat, file=paste0(save_path, "fields.xlsx"), sheetName="atbat", row.names=FALSE, append=TRUE)
write.xlsx(coach, file=paste0(save_path, "fields.xlsx"), sheetName="coach", row.names=FALSE, append=TRUE)
write.xlsx(game, file=paste0(save_path, "fields.xlsx"), sheetName="game", row.names=FALSE, append=TRUE)
write.xlsx(hip, file=paste0(save_path, "fields.xlsx"), sheetName="hip", row.names=FALSE, append=TRUE)
write.xlsx(media, file=paste0(save_path, "fields.xlsx"), sheetName="media", row.names=FALSE, append=TRUE)
write.xlsx(pitch, file=paste0(save_path, "fields.xlsx"), sheetName="pitch", row.names=FALSE, append=TRUE)
write.xlsx(player, file=paste0(save_path, "fields.xlsx"), sheetName="player", row.names=FALSE, append=TRUE)
write.xlsx(po, file=paste0(save_path, "fields.xlsx"), sheetName="po", row.names=FALSE, append=TRUE)
write.xlsx(runner, file=paste0(save_path, "fields.xlsx"), sheetName="runner", row.names=FALSE, append=TRUE)
write.xlsx(umpire, file=paste0(save_path, "fields.xlsx"), sheetName="umpire", row.names=FALSE, append=TRUE)