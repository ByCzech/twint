agg_df <- read.csv("~/git/py/twint2/data/output/tweets/aggregated.csv", header =TRUE)
user_table <- read.csv("~/git/py/twint2/data/updated_input.csv", header =TRUE)

colnames(user_table)[colnames(user_table) == "output_user_id"] = "user_id"

library(sqldf)
joined = sqldf("SELECT * FROM agg_df INNER JOIN user_table USING(user_id)")

joined[]

write.csv(joined, "~/git/py/twint2/data/output/tweets/joined.csv")


joined <- read.csv("~/git/py/twint2/data/output/tweets/joined.csv", header =TRUE)

control <- subset(joined, group == "CONTROL")
blocked <- subset(joined, group == "BLOCKED")

control_not_blocked <- subset(control, !(output_username %in% blocked$output_username) )

nrow(control_not_blocked)

install.packages("data.table")                  # Install & load data.table
library("data.table")
data_concat <- rbindlist(list(blocked, control_not_blocked))

write.csv(data_concat, "~/git/py/twint2/data/output/tweets/joined_fix.csv")


war <- read.csv("~/git/py/twint2/data/output/tweets/war-outbreak.csv", header =TRUE)
war_sub <- subset(war, select=c("username", "created_at", "tweet"))
write.csv(war_sub, "~/git/py/twint2/data/output/tweets/war_sub.csv")

#create avg value columns
joined$replies_count_avg <- with(joined, replies_count / count)
joined$retweets_count_avg <- with(joined, retweets_count / count)
joined$likes_count_avg <- with(joined, likes_count / count)
joined$quotes_count_avg <- with(joined, quotes_count / count)
joined$impressions_count_avg <- with(joined, impressions_count / count)

#fiter out 
joined_filtered <- subset(joined, output_follower_count %in% 100:1000)
joined_filtered <- subset(joined_filtered, month != "2023-04")
joined_filtered <- subset(joined_filtered, blocked_date != "01.04.2023")

#group by blocked_earlier
joined_filtered <- subset(joined, output_follower_count %in% 100:1000)
joined_filtered <- subset(joined_filtered, month != "2023-04")
joined_be <- subset(joined_filtered, group != "CONTROL")
joined_be$blocked_feb <- with(joined_be, blocked_date != "01.04.2023")

allowed_months <- c("2023-01", "2023-03")
joined_be <- subset(joined_be, month %in% allowed_months)

data_likes = MeanByGroup(joined_be,c("month","blocked_feb"),"likes_count_avg")

TEcolors = scale_fill_manual(values=c("#a9a9a9", "#555555"),labels=c("BLOCKED 2023-02","BLOCKED 2023-04"))
ggplot(data_likes, aes(x =month, y = mean, fill = blocked_feb)) + 
  geom_bar(stat="identity", position = position_dodge(), col="black") +
    labs(x = "Month", fill = "Group", y = "Likes per tweet") +
   coord_cartesian() + TEcolors +
    commonTheme + theme(axis.title.y = element_text(size=15))

data_impressions = MeanByGroup(joined_be,c("month","blocked_feb"),"impressions_count_avg")
ggplot(data_impressions, aes(x =month, y = mean, fill = blocked_feb)) + 
  geom_bar(stat="identity", position = position_dodge(), col="black") +
    labs(x = "Month", fill = "Group", y = "Impressions") +
   coord_cartesian() + TEcolors +
    commonTheme + theme(axis.title.y = element_text(size=15))

#aggregate
df = pd.read_csv("~/git/py/twint2/data/output/tweets/tweet_data.csv", low_memory=False)
agg_df <- df.groupby(['user_id', 'month', 'is_reply']).agg({
    'id': ['count'],
    'replies_count': ['sum'],
    'retweets_count': ['sum'],
    'likes_count': ['sum'],
    'quotes_count': ['sum'],
    'impressions_count': ['sum']
})


#plots

data_likes = MeanByGroup(joined_filtered,c("month","group"),"likes_count_avg")
TEcolors = scale_fill_manual(values=c("#a9a9a9", "#aed8e5"),labels=c("BLOCKED","CONTROL"))

ggplot(data_likes, aes(x =month, y = mean, fill = group)) + 
  geom_bar(stat="identity", position = position_dodge(), col="black") +
    labs(x = "Month", fill = "Group", y = "Likes per tweet") +
   coord_cartesian() + TEcolors +
    commonTheme + theme(axis.title.y = element_text(size=15))

data_impressions =MeanByGroup(joined_filtered,c("month","group"), "impressions_count_avg") 
ggplot(data_impressions, aes(x =month, y = mean, fill = group)) + 
  geom_bar(stat="identity", position = position_dodge(), col="black") +
    labs(x = "Month", fill = "Group", y = "Views per tweet") +
   coord_cartesian() + TEcolors +
    commonTheme + theme(axis.title.y = element_text(size=10))

data_retweets =MeanByGroup(joined_filtered,c("month","group"), "retweets_count_avg") 
ggplot(data_retweets, aes(x =month, y = mean, fill = group)) + 
  geom_bar(stat="identity", position = position_dodge(), col="black") +
    labs(x = "Month", fill = "Group", y = "Retweets per tweet") +
   coord_cartesian() + TEcolors +
    commonTheme + theme(axis.title.y = element_text(size=10))

SumByGroup <- function(f_data,f_groupCol,f_targetCol){
f_preserveColNames = c(f_groupCol,f_targetCol)
f_targetColNames = c(f_targetCol)

f_data = select_at(f_data, f_preserveColNames)

  f_result = f_data %>%
    group_by_(.dots = f_groupCol) %>%
    summarise_at(vars(one_of(f_targetColNames)),funs(length,sum))

print(f_result %>% as.data.frame())
  return(f_result %>% as.data.frame())
}


MeanByGroup <- function(f_data,f_groupCol,f_targetCol){
f_preserveColNames = c(f_groupCol,f_targetCol)
f_targetColNames = c(f_targetCol)

f_data = select_at(f_data, f_preserveColNames)


  f_result = f_data %>%
    group_by_(.dots = f_groupCol) %>%
    summarise_at(vars(one_of(f_targetColNames)),funs(length,mean,sd))

    print(f_result %>% as.data.frame())
  return(f_result %>% as.data.frame())
}