data <- read.csv("~/git/py/twint2/output_users copy.csv", header =TRUE)
distinct_data <- unique(data)
write.csv(distinct_data,"~/git/py/twint2/output_users_distinct.csv")