csv_converter <- function(filepath) {
  pattern <- "^[[:digit:]]+ P-[-]?[[:alnum:]] P-[-]?[[:alnum:]][*]*"
  fh_read <- file(filepath, "r")
  fh_write<- file("log.csv", "w")
  while (TRUE) {
    line = readLines(fh_read, n = 1)
    if (length(line) == 0) {
      break
    }
    if (!grepl(pattern, line))
      next
    for (x in 1:3) {
      line <- sub("\\s+", ";", line)
    }
    writeLines(line, fh_write)
  }
  close(fh_read)
  close(fh_write)
}

seq_message_converter <- function(logs) {
  fh_write <- file("sequence.txt", "w")
  
  writeLines("msc {", fh_write)
  actors <- unique(sort(c (logs$V2, logs$V3)))
  actors <- sapply(actors, function(x) {
			paste(c("\"", x, "\""), collapse = "")})
  writeLines(paste(actors, collapse = ","),
             fh_write, sep = ";\n")

  for (i in 2:nrow(logs)) {
    log <- logs[i,]
    writeLines(sprintf("\"%s\"<=\"%s\" [label=\"%s\"];",
                       log[2], log[3], log[4]),
               fh_write)
  }
  writeLines("}", fh_write)
  close(fh_write)
}


args <- commandArgs(trailingOnly = T)
if (length(args) > 0) {
  csv_converter(args[1])
  data = read.csv("log.csv", sep=";",
                  header = FALSE,
                  stringsAsFactors = F)
  index = with(data, order(V1))
  data <- data[index,]
  seq_message_converter(data)
}
