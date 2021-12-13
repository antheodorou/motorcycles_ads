#Load data
setwd("~/MScBA/3. Winter Semester I/Big Data Systems and Architectures/0. Assignments/2. REDIS & MongoDB/Deliverable")
#setwd( "C:/Users/konst/OneDrive/Υπολογιστής/RECORDED_ACTIONS")

emails_sent <- read.csv("emails_sent.csv", sep =",", stringsAsFactors=FALSE)
modified_listing <- read.csv("modified_listings.csv", sep =",", stringsAsFactors=FALSE)

# Load the library
library("redux")

# Create a connection to the local instance of REDIS
r <- redux::hiredis(redux::redis_config(host = "127.0.0.1", port = "6379"))

#Q.1
data <- modified_listing[modified_listing$MonthID ==1, ]

for (i in 1: nrow(data)){
  if (data$ModifiedListing[i]==1){
    r$SETBIT("ModificationsJanuary",data$UserID[i] ,"1")
  }
}

r$BITCOUNT("ModificationsJanuary")

#Q.2
r$BITOP("NOT","results",c("ModificationsJanuary"))
r$BITCOUNT("results")

9969+10031

length(unique(data$UserID))

#Q.3
library(dplyr)
#Create a data-frame which groups by and counts for each user and each month the times, 
#that each one of them had received an email
group_emails <- emails_sent %>% count(UserID, MonthID)

#Set one bitmap per month
for (i in 1:nrow(group_emails)) {
  if (group_emails$MonthID[i] == 1){
    r$SETBIT("EmailsJanuary",group_emails$UserID[i],"1")
  } else if (group_emails$MonthID[i] == 2){
    r$SETBIT("EmailsFebruary",group_emails$UserID[i],"1")
  } else {
    r$SETBIT("EmailsMarch",group_emails$UserID[i],"1")
  }
}

#Calculate how many users received at least one e-mail per month 
r$BITOP("AND","q3",c("EmailsJanuary","EmailsFebruary", "EmailsMarch"))
r$BITCOUNT("q3")

#Q.4
r$BITOP("NOT","EmailsNotFebruary",c("EmailsFebruary"))
r$BITOP("AND","q4_fin",c("EmailsNotFebruary", "EmailsJanuary", "EmailsMarch"))
r$BITCOUNT("q4_fin")

#Q.5
#keep only January
data05 <- emails_sent[emails_sent$MonthID ==1, ]

for (i in 1: nrow(data05)){
  if (data05$EmailOpened[i]==0){
    r$SETBIT("EmailsNotOpenedJanuary",data05$UserID[i] ,"1")
  }
}

r$BITCOUNT("EmailsNotOpenedJanuary")

r$BITOP("AND","EmailsOpenedJanuary",c("ModificationsJanuary","EmailsNotOpenedJanuary"))
r$BITCOUNT("EmailsOpenedJanuary")

#Q.6
for (i in 1: nrow(emails_sent)){
  if (emails_sent$EmailOpened[i]==0 && emails_sent$MonthID[i]== 2 ){
    r$SETBIT("EmailsNotOpenedFebruary",emails_sent$UserID[i] ,"1")
  }
  else if (emails_sent$EmailOpened[i]==0 && emails_sent$MonthID[i]== 3 ){
    r$SETBIT("EmailsNotOpenedMarch",emails_sent$UserID[i] ,"1")
  }
}

for (i in 1: nrow(modified_listing)){
  if (modified_listing$ModifiedListing[i]==1 && modified_listing$MonthID[i]==2 ){
    r$SETBIT("ModificationsFebruary",modified_listing$UserID[i] ,"1")
  } else if (modified_listing$ModifiedListing[i]==1 && modified_listing$MonthID[i]==3 ){
    r$SETBIT("ModificationsMarch",modified_listing$UserID[i] ,"1")
  }
}

r$BITOP("AND","EmailsOpenedFebruary",c("ModificationsFebruary","EmailsNotOpenedFebruary"))
r$BITOP("AND","EmailsOpenedMarch",c("ModificationsMarch","EmailsNotOpenedMarch"))

r$BITOP("OR","EmailsOpened",c("EmailsOpenedJanuary","EmailsOpenedFebruary","EmailsOpenedMarch"))
r$BITCOUNT("EmailsOpened")

#Q.7
round(7221*100/length(emails_sent$EmailID),2) #the percentage of those who received an email, do not opened it and despite that they updated their listing

r$BITOP("NOT","NotModificationsJanuary", c("ModificationsJanuary"))
r$BITOP("NOT","NotModificationFebruary", c("ModificationsFebruary"))
r$BITOP("NOT","NotModificationsMarch", c("ModificationsMarch"))
r$BITOP("NOT","OpenedJanuary", c("EmailsNotOpenedJanuary"))
r$BITOP("NOT","OpenedFebruary", c("EmailsNotOpenedFebruary"))
r$BITOP("NOT","OpenedMarch", c("EmailsNotOpenedMarch"))

r$BITOP("AND","ej",c("NotModificationsJanuary","OpenedJanuary"))
r$BITOP("AND","ef",c("NotModificationFebruary","OpenedFebruary"))
r$BITOP("AND","em",c("NotModificationsMarch","OpenedMarch"))

r$BITOP("OR","etot",c("ej","ef","em"))
r$BITCOUNT("etot")
round(14706*100/length(emails_sent$EmailID),2) #those who received an email, opened it and did not updated their listing

r$BITOP("AND","ej2",c("ModificationsJanuary","OpenedJanuary"))
r$BITOP("AND","ef2",c("ModificationsFebruary","OpenedFebruary"))
r$BITOP("AND","em2",c("ModificationsMarch","OpenedMarch"))
r$BITOP("OR","etot2",c("ej2","ef2","em2"))
r$BITCOUNT("etot2")

round(14688*100/length(emails_sent$EmailID),2) #those who received an email, opened it and updated their listing