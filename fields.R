library(pitchRx)
library(xlsx)

varlist <- c("action", "atbat", "coach", "game", "hip", "media", "pitch", "player", "po", "runner", "umpire")

table <- fields

for (v in varlist) {
  assign(v,
         data.frame(var = names(table[[v]]),
                    type = unname(table[[v]])))
}

write.xlsx(action, file="fields.xlsx", sheetName="action", row.names=FALSE)
write.xlsx(atbat, file="fields.xlsx", sheetName="atbat", row.names=FALSE, append=TRUE)
write.xlsx(coach, file="fields.xlsx", sheetName="coach", row.names=FALSE, append=TRUE)
write.xlsx(game, file="fields.xlsx", sheetName="game", row.names=FALSE, append=TRUE)
write.xlsx(hip, file="fields.xlsx", sheetName="hip", row.names=FALSE, append=TRUE)
write.xlsx(media, file="fields.xlsx", sheetName="media", row.names=FALSE, append=TRUE)
write.xlsx(pitch, file="fields.xlsx", sheetName="pitch", row.names=FALSE, append=TRUE)
write.xlsx(player, file="fields.xlsx", sheetName="player", row.names=FALSE, append=TRUE)
write.xlsx(po, file="fields.xlsx", sheetName="po", row.names=FALSE, append=TRUE)
write.xlsx(runner, file="fields.xlsx", sheetName="runner", row.names=FALSE, append=TRUE)
write.xlsx(umpire, file="fields.xlsx", sheetName="umpire", row.names=FALSE, append=TRUE)