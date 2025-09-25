# Title: 1. Connect to Dropbox and Download Formulary Files
# Author: Nicholas Cardamone
# Created: 5/9/2024

# Goal:
# Use 2007-2023 Part D formulary files for stand-alone Medicare Part D and 
# Medicare Advantage prescription drug plans to identify plans’ use of prior 
# authorization, quantity limits, and step therapy for each unique 
# brand-generic-dose-formulary combination of orally administered drugs 
# in each year after NME approval.

# Overview:
# 1. Connects to Dropbox via account token.
# 2. Searches for formulary files (files starting with "basic drugs formulary").
# 3. Downloads the relevant files locally.

# Notes:
# * Manual unzipping of files is required—R's unzip was omitting data from .txt files.

# --- Load Required Packages ---
library(rdrop2)      # For Dropbox connection
library(httr)        # For web scraping and authentication
library(stringr)     # For string processing
library(lubridate)   # For date handling
library(haven)       # For reading files of various formats
library(dplyr)       # For data manipulation
library(tidyverse)   # General data manipulation tools
library(xfun)        # Miscellaneous functions
library(data.table)  # Efficient handling of large data

# --- Dropbox Authentication with Refreshable Token ---
# Custom function to generate and load a refreshable Dropbox token.
# Source: https://stackoverflow.com/questions/71393752/get-a-refresh-token-for-dropbox-api-using-rdrop2-and-drop-auth

.dstate <- new.env(parent = emptyenv())

drop_auth_RT <- function(
  new_user = FALSE, 
  key = "TOKEN KEY", 
  secret = "TOKEN SECRET", 
  cache = TRUE, 
  rdstoken = NA
) {
  if (!new_user && !is.na(rdstoken)) {
    # Load existing token from RDS if available
    if (file.exists(rdstoken)) {
      .dstate$token <- readRDS(rdstoken)
    } else {
      stop("Token file not found.")
    }
  } else {
    # If new_user=TRUE or no token, start OAuth flow
    if (new_user && file.exists(".httr-oauth")) {
      message("Removing old credentials...")
      file.remove(".httr-oauth")
    }
    dropbox <- httr::oauth_endpoint(
      authorize = "https://www.dropbox.com/oauth2/authorize?token_access_type=offline",
      access = "https://api.dropbox.com/oauth2/token"
    )
    # The 'offline' access type enables both access and refresh tokens
    dropbox_app <- httr::oauth_app("dropbox", key, secret)
    dropbox_token <- httr::oauth2.0_token(dropbox, dropbox_app, cache = cache)
    if (!inherits(dropbox_token, "Token2.0")) {
      stop("OAuth authentication failed. Try again.")
    }
    .dstate$token <- dropbox_token
  }
}

# Generate a refreshable Dropbox token
refreshable_token <- drop_auth_RT()

# Save the Dropbox token locally for future sessions
saveRDS(refreshable_token, "my-token.rds")

# Authenticate Dropbox session using the saved token
drop_auth(rdstoken = "my-token.rds")
token <- readRDS("my-token.rds")

# --- Search Dropbox for Relevant Files ---

# Set the root Dropbox folder to search
root_folder_path <- "GENCO"

# Optional: Check Dropbox account info
drop_acc() %>% data.frame()

# List all files and folders in the specified root directory
all_files <- drop_dir(root_folder_path, dtoken = token)

# Search Dropbox for files with names starting with "basic drugs formulary"
# Returns a list of matching files (metadata)
formulary_files <- drop_search("^basic drugs formulary")

# --- Download Relevant Files Locally ---

# Define the download function for each matched file
download_function <- function(match_item) {
  # 'local_path' should be updated to your desired local directory
  drop_download(
    match_item$metadata$path_lower, 
    local_path = "YOUR PATH", 
    overwrite = TRUE
  )
}

# Apply the download function to all matching files
lapply(formulary_files$matches, download_function)

# Note:
# - Some filenames might need minor manual edits after download.
