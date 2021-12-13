rm(list = ls())
#Working Directory
setwd("~/MScBA/3. Winter Semester I/Big Data Systems and Architectures/0. Assignments/2. REDIS & MongoDB")
# Load jsonlite
library("jsonlite")
#Set as Working Directory that in which the json files are located
setwd("~/MScBA/3. Winter Semester I/Big Data Systems and Architectures/0. Assignments/2. REDIS & MongoDB/BIKES")
#Text file where the paths of json files are located
f <- read.table("files_list.txt", header = T, sep = "\n", dec = "\"", fileEncoding="UTF-16LE")
colnames(f) <- "files_list"
f$files_list <- gsub("\\\\", "/", f$files_list) #take the form we want
head(f)
#The path in my computer
direction <- "~/MScBA/3. Winter Semester I/Big Data Systems and Architectures/0. Assignments/2. REDIS & MongoDB/BIKES/"
fin_dir <- paste(direction, f[1:nrow(f),], sep = "") #the final directories
#Create list with all the json data
json_data <- lapply(fin_dir, fromJSON)
# Have a look at the data
head(json_data)
str(json_data)

##################################################
############## C L E A N I N G ###################
##################################################
library(stringr)
js <- json_data
i <- 1 #the iterator
while (i <= length(json_data)) {
  js[i][[1]]$description <- NULL #useless information, just a description of the bike in text format
  js[i][[1]]$query$last_processed <- NULL #a meaningless field
  js[i][[1]]$query$type <- NULL #it is the same for all the bikes
  js[i][[1]]$ad_data$`Make/Model` <- NULL #this information is duplicated. There are separately in "metadata" ("brand" and "model")
  js[i][[1]]$ad_data$`Classified number` <- NULL #it is the same as "ad_id"
  #Convert the mileage into numeric type
  js[i][[1]]$ad_data$Mileage <- str_replace_all(js[i][[1]]$ad_data$Mileage, c("\\," = "", " km" = ""))
  js[i][[1]]$ad_data$Mileage <- as.numeric(js[i][[1]]$ad_data$Mileage)
  js[i][[1]]$ad_data$Telephone <- NULL #no needed information
  js[i][[1]]$ad_data$`Times clicked` <- as.numeric(js[i][[1]]$ad_data$`Times clicked`) #convert this field into numeric type
  if (length(js[i][[1]]$extras) == 0) js[i][[1]]$extras <- FALSE #where this field is empty put the boolean "false"
  if (length(js[i][[1]]$ad_seller) == 0) js[i][[1]]$ad_seller <- FALSE #the same as above
  #Some bikes do not have price and the seller has put to ask him for further information, so it is created a new field with this
  if(substr(js[i][[1]]$ad_data$Price, 1, 1) != "€" | is.null(js[i][[1]]$ad_data$Price) | is.na(js[i][[1]]$ad_data$Price)){
    js[i][[1]]$ad_data$AskForPrice <- TRUE #the observations which were "Askforprice"
  } else {
    #On the other hand the price has been converted to numeric type
      js[i][[1]]$ad_data$AskForPrice <- FALSE
      js[i][[1]]$ad_data$Price <- str_replace_all(js[i][[1]]$ad_data$Price, c("€" = "", "\\." = ""))
      js[i][[1]]$ad_data$Price <- as.numeric(js[i][[1]]$ad_data$Price)
      if (js[i][[1]]$ad_data$Price <= 40){
        #If the price of a bike is unexpected too small we have order it in the field "AskForPrice"
        js[i][[1]]$ad_data$AskForPrice <- TRUE 
      }
    }
  i <- i+1 #move to next record
}
data <- c() #create a vector in order to insert the records into mongodb
for (i in 1:length(js)) {
  j <- toJSON(js[i], auto_unbox = TRUE)
  
  j <- gsub('^\\[|\\]$', '', j) #the first and last character of the list contained the symbols "[]". 
  #So, they had to be removed in order the system to recognize the list as combination of json files 
  data <- c(data, j)}

#############################################
############# M O N G O D B #################
#############################################
#Load mongolite
library("mongolite")
#Open a connection to MongoDB
m <- mongo(collection = "bikes",  db = "mongo_assignment", url = "mongodb://localhost")
#Insert this JSON object to MongoDB
m$insert(data)
#Check if it has been inserted
m$find('{}')
#Q.2
m$count()
#Q.3
m$aggregate('[{ "$match" : {"ad_data.Price" : { "$exists" : true } }}, 
            {"$group":{"_id": null, "AvgPrice": {"$avg":"$ad_data.Price"}, 
            "count": {"$sum" : 1 }}}]')
#Q.4
m$aggregate('[{"$match" : {"$and": [{"ad_data.AskForPrice" : {"$eq": false}}, {"ad_data.Price": {"$gte": 40}}]}},
            {"$group" : {"_id" : "null",
              "Maximum_Price" : {"$max" : "$ad_data.Price"},
              "Minimum_Price" : {"$min" : "$ad_data.Price"}}},
            {"$project" : {"_id": 0, "Maximum Price": "$Maximum_Price", "Minimum Price": "$Minimum_Price"}}]')
#Q.5 
m$aggregate('[{"$match" : {"metadata.model" : {"$regex" : "Negotiable", "$options" : "i"} }},
            {"$group" :  {"_id" : null, "Negotiable Models" : { "$sum" : 1}}},
            {"$project": {"_id": 0, "Negotiable Models" : 1} }]')
#Q.6
m$aggregate('[ {"$group" :  {"_id" : "$metadata.brand", "numb" : { "$sum" : 1}}}, 
            {"$out" : "totbrand"}]')

m2 <- mongo(collection = "totbrand",  db = "mongo_assignment", url = "mongodb://localhost")

m$aggregate('[{"$match" : {"metadata.model" : {"$regex" : "Negotiable", "$options" : "i"} }},
              {"$group" :  {"_id" : "$metadata.brand",
              "NegotiableBr" : { "$sum" : 1}}},
            {"$lookup":{
                  "from": "totbrand",
                  "localField": "_id",
                  "foreignField": "_id",
                  "as": "brandjoin"}}, 
            {"$unwind": "$brandjoin"},
            {"$project": {"BrandName": "$_id", "_id": 0, 
              "PercNeg": {"$round": [{"$multiply": [{"$divide": ["$NegotiableBr", "$brandjoin.numb"]}, 100]}, 2]}}}]')
#Q.7
m$aggregate('[{"$group" :  {"_id" : "$metadata.brand", "avg_price" : {"$avg": "$ad_data.Price"}}},
            {"$sort" : {"avg_price": -1}},
            {"$limit": 1},
            {"$project": {"_id": 0, "BrandName": "$_id", "MaxAvg": "$avg_price"}}]')
#Q.8
m$aggregate('[{"$project": {"models": "$metadata.model", "reg": {"$split": ["$ad_data.Registration", "/"]}}}, 
            {"$project": {"_id": 0, "models": "$models", "year": {"$toInt": {"$trim": {"input": {"$last": "$reg"}}}}}},
            {"$project": {"_id": 0, "models": 1, "age": {"$subtract": [2021, "$year"]}}},
            {"$group" :  {"_id" : "$models", "avg_age" : {"$avg": "$age"}}},
            {"$project": {"ModelName": "$_id", "_id": 0, "Avg_Age": {"$round": ["$avg_age", 1]}}},
            {"$sort" : {"Avg_Age": -1}},
            {"$limit": 10}]')
#Q.9
m$aggregate('[{"$match" : {"extras" : {"$regex" : "ABS", "$options" : "i"} }},
            {"$group" :  {"_id" : null,"ABSModels" : { "$sum" : 1}}},
            {"$project": {"_id": 0, "BikesWithABS" : "$ABSModels"} }]')