#title: 1.Connect to Dropbox
#author: "Nicholas Cardamone"
#date: "5/9/2024"

#Goal: Use 2006-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination (‘molecule’) of orally administered drugs in each year after NME approva

# What this code does:
# 1. Connects to online Dropbox database via unique account token.
# 2. Finds all formulary files **only** downloads those files (they all start with the phrase "basic drugs formulary").


# Notes:
# * I had to manually unzip all of the zip files and extract the .txt files within them because for an unknown reason when R was unzipping and reading the files, it was omitting portions of the data.

# Next edits:
# * Extract quantity limits data from formulary files.
# * Extract step therapy from formulary files.


# Load necessary packages
library(rdrop2) # connect to dropbox
library(httr)
library(stringr)
library(lubridate)
library(haven)
library(dplyr)
library(tidyverse)
library(xfun)
library(data.table)


#Dropbox Connect Function
## This code alters a function in rdrop2 that will give us a refresh token every time the access token expires.
## Source: https://stackoverflow.com/questions/71393752/get-a-refresh-token-for-dropbox-api-using-rdrop2-and-drop-auth

.dstate <- new.env(parent = emptyenv())

drop_auth_RT <- function (new_user = FALSE, key = "mmhfsybffdom42w", secret = "l8zeqqqgm1ne5z0", cache = TRUE, rdstoken = NA) 
{
  if (new_user == FALSE & !is.na(rdstoken)) {
    if (file.exists(rdstoken)) {
      .dstate$token <- readRDS(rdstoken)
    }
    else {
      stop("token file not found")
    }
  }
  else {
    if (new_user && file.exists(".httr-oauth")) {
      message("Removing old credentials...")
      file.remove(".httr-oauth")
    }
    dropbox <- httr::oauth_endpoint(authorize = "https://www.dropbox.com/oauth2/authorize?token_access_type=offline",
                                    access = "https://api.dropbox.com/oauth2/token")
    # added "?token_access_type=offline" to the "authorize" parameter so that it can return an access token as well as a refresh token
    dropbox_app <- httr::oauth_app("dropbox", key, secret)
    dropbox_token <- httr::oauth2.0_token(dropbox, dropbox_app, 
                                          cache = cache)
    if (!inherits(dropbox_token, "Token2.0")) {
      stop("something went wrong, try again")
    }
    .dstate$token <- dropbox_token
  }
}

refreshable_token <- drop_auth_RT()


## Set up Dropbox Token ##
# Authenticate your Dropbox account (you only need to do this once)
#token <- drop_auth()
saveRDS(refreshable_token, "my-token.rds")

drop_auth(rdstoken = "my-token.rds") 
token <- readRDS("my-token.rds")

## Search Dropbox for relevant files

# Define the root folder path in Dropbox
root_folder_path <- "GENCO"
drop_acc() %>% data.frame()
#drop_acc() %>% data.frame()
# List all folders and files in the root folder
all_files <- drop_dir(root_folder_path, dtoken= token)
# Filter for .dta files that start with "basic drugs formulary"
x <- drop_search("^basic drugs formulary")


## Download relevant files locally ##
## Download files from GENCO dropbox:
download_function <- function(match_item) {
  drop_download(match_item$metadata$path_lower, local_path = "C://Users//Nick//Desktop//VA//GENCO", overwrite = TRUE)
}

# Apply the function to each element in the list using lapply
lapply(x$matches, download_function)

# Note: Some files' names needed to be edited slightly.
# I downloaded and uploaded the 2006-06 data manually because it was different dataframe than the others.