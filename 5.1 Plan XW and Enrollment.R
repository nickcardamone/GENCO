# =====================================================
# Title: 5.1 Plan XW and Enrollment
# Author: Nicholas Cardamone
# Date: 2025-05-16
# Last Updated: 2025-05-30
# =====================================================

# =====================================================
# OVERVIEW & INSTRUCTIONS
# =====================================================
# GOAL:
#   - Scrape Medicare Part D monthly plan enrollment data.
#   - Download plan-formulary-period files.
#   - Crosswalk plan enrollment to prior authorization dataset.
#   - Aggregate plan-level prior auth and enrollment into a single dataset.
#
# HIGH-LEVEL STEPS:
#   1. Download and combine plan-formulary-period files from Dropbox.
#   2. Clean and standardize plan-formulary crosswalk data.
#   3. Webscrape CMS for plan enrollment data by month.
#   4. Clean, aggregate, and summarize enrollment.
#   5. Join enrollment to plan-formulary data for further analysis.
#
# INSTRUCTIONS:
#   - Set your custom file paths where indicated ("YOUR PATH").
#   - Ensure all required packages are installed.
#   - For Dropbox access, authenticate and set your token as needed.
#   - Some manual steps (e.g. unzipping files) are flagged below.
# =====================================================

# =====================================================
# 0. Load Required Packages
# =====================================================
library(httr)          # Web requests
library(stringr)       # String processing
library(lubridate)     # Dates handling
library(haven)         # Reading various file formats
library(dplyr)         # Data wrangling
library(tidyverse)     # Data wrangling (broad)
library(xfun)          # Miscellaneous functions
library(data.table)    # Big data handling
library(arrow)         # Working with Parquet files
library(pbapply)       # Progress bars
library(readr)         # Reading in data
library(tools)         # File utilities
library(devtools)      # Development tools

# =====================================================
# 1. DOWNLOAD PLAN-FORMULARY FILES FROM DROPBOX
# =====================================================
# -- Prerequisite: Dropbox authentication and token setup required.

# Set Dropbox root folder path
root_folder_path <- "GENCO"

# Confirm Dropbox connection (prints account info)
drop_acc() %>% data.frame()

# List all files under root
all_files <- drop_dir(root_folder_path, dtoken = token)

# Search for files of interest (e.g., starts with "plan information")
x <- drop_search("^plan information")

# Define downloader function for each match
download_function <- function(match_item) {
  # Set your local download directory here:
  local_file_path <- file.path("YOUR PATH", basename(match_item$metadata$path_lower))
  if (!file.exists(local_file_path)) {
    tryCatch({
      drop_download(match_item$metadata$path_lower, local_path = local_file_path, overwrite = TRUE)
      cat("Downloaded:", local_file_path, "\n")
    }, error = function(e) {
      cat("Error downloading", match_item$metadata$path_lower, ":", e$message, "\n")
    })
  } else {
    cat("File already exists, skipping download:", local_file_path, "\n")
  }
}

# Download all search results
lapply(x$matches, download_function)

# =====================================================
# 2. PROCESS & CLEAN PLAN-FORMULARY CROSSWALK DATA
# =====================================================
# -- Place downloaded files in your "plan" directory.

folder_path <- "inputs/plan"  # <-- Set your directory here

# List all .dta, .txt, .zip files ending with "with drug names"
all_files <- list.files(folder_path, pattern = ".(dta|txt|zip)$", full.names = TRUE)

# Extract dates from filenames for later merging
names_df <- data.frame(
  name = basename(all_files)
) %>%
  mutate(
    date = case_when(
      str_detect(name, "\\d{8}") ~ str_extract(name, "\\d{8}"),
      str_detect(name, "\\d{4}-12") ~ str_replace(str_extract(name, "\\d{4}-12"), "-12", "1231"),
      str_detect(name, "\\d{4}") ~ paste0(str_extract(name, "\\d{4}"), "0630"),
      TRUE ~ NA_character_
    )
  )

# ----
# Process function: reads each file, standardizes columns, adds date
process_file <- function(file_path, date_var) {
  cat("Processing file:", basename(file_path), "\n")
  file_extension <- file_ext(file_path)
  if (file_extension == "zip") {
    temp_dir <- tempdir()
    unzip(file_path, exdir = temp_dir)
    txt_files <- list.files(temp_dir, pattern = "\\.txt$", full.names = TRUE)
    if (length(txt_files) == 0) stop("No .txt file found in the zip archive")
    file_path <- txt_files[1]
    file_extension <- "txt"
  }
  if (file_extension == "dta") {
    data <- read_dta(file_path)
  } else if (file_extension == "txt") {
    data <- read_delim(file_path, delim = "|", col_types = cols(), 
                       col_names = c("CONTRACT_ID", "PLAN_ID", "SEGMENT_ID", "CONTRACT_NAME", "PLAN_NAME", 
                                     "FORMULARY_ID", "PREMIUM", "DEDUCTIBLE", "ICL", "MA_REGION_CODE", 
                                     "PDP_REGION_CODE", "STATE", "COUNTY_CODE", "SNP"))  
  } else {
    stop("Unsupported file type")
  }
  names(data) <- tolower(names(data))
  data <- data %>%
    mutate(
      plan_id = as.character(plan_id),
      formulary_id = as.character(formulary_id),
      premium = as.character(premium),
      deductible = as.character(deductible),
      segment_id = as.character(segment_id),
      snp = as.character(snp),
      icl = as.character(icl)
    )
  date_var <- as.Date(date_var, format = "%Y%m%d")
  data_selected <- data %>%
    select(contract_id, plan_id, segment_id, contract_name, plan_name, formulary_id, premium, deductible, icl,
           ma_region_code, pdp_region_code, state, county_code, snp) %>%
    mutate(date = date_var)
  return(data_selected)
}

# Apply to all files and combine into one dataframe
combined_plan_data <- mapply(
  process_file, 
  all_files, 
  names_df$date, 
  SIMPLIFY = FALSE
) %>% bind_rows()

# Standardize and clean plan ID fields for merging
combined_plan_data <- combined_plan_data %>% 
  select(contract_id, plan_id, formulary_id, segment_id, date) %>% 
  mutate(plan_id = str_pad(plan_id, 3, pad = "0")) %>%
  distinct()

# Append new (2025) to old (2024) plan-formulary data if needed
combined_plan_data_old <- read.csv("inputs/combined_plan_data_n=134198-10-31-2024.csv")
combined_plan_data_new <- read.csv("inputs/combined_plan_data_n=134198-5-30-2025.csv")

combined_plan_data <- rbind(combined_plan_data_old, combined_plan_data_new) %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>%
  distinct()

# Final cleaning and export to parquet
combined_plan_data <- combined_plan_data %>% 
  filter(segment_id != "SEGMENT_ID") %>%
  mutate(segment_id = as.factor(as.numeric(segment_id)),
         plan_id = as.factor(as.numeric(plan_id))) %>%
  select(formulary_id, contract_id, plan_id, segment_id, date) %>%
  transmute(plan_id = as.factor(plan_id), 
            date = as.Date(date),
            formulary_id = str_pad(formulary_id, 8, pad = "0")) %>%
  distinct()

write_parquet(combined_plan_data, "parquet/combined_plan_data.parquet")

# =====================================================
# 3. WEBSCRAPE CMS ENROLLMENT DATA (BY PLAN, MONTHLY)
# =====================================================
# 1) Scrape main CMS page to get links to monthly enrollment files.
# 2) Visit each link and extract the .zip download link.
# 3) Download the zipped data files.
# 4) (Manual) Unzip .txt files for ingestion.

main_url <- "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan"

# Read HTML and parse links
main_page <- read_html(main_url, user_agent = "Mozilla/5.0")
links <- main_page %>%
  html_nodes("table a") %>%
  html_attr("href") %>%
  unique()

# Helper: get subsequent zip link from each detail page
safe_get_subsequent_link <- function(link, retries = 3, sleep_time = 1) {
  full_url <- url_absolute(link, main_url)
  attempt <- 1
  while (attempt <= retries) {
    tryCatch({
      message("Attempt ", attempt, " for link: ", full_url)
      page <- read_html(GET(full_url, timeout(30), user_agent = "Mozilla/5.0"))
      subsequent_link <- page %>%
        html_nodes("li.field__item > a:nth-child(1)") %>%
        html_attr("href") %>%
        unique() %>%
        .[1]
      return(subsequent_link)
    }, error = function(e) {
      message("Error in attempt ", attempt, " for link: ", full_url, " - ", e$message)
      attempt <- attempt + 1
      Sys.sleep(sleep_time)
    })
  }
  message("All attempts failed for link: ", full_url)
  return(NA)
}

# Get zip download links for all months
subsequent_links <- map(links, ~ safe_get_subsequent_link(.x))
result <- data.frame(main_page_link = links, subsequent_page_link = subsequent_links)

# Download all zip files to local directory
root_url <- "https://www.cms.gov"
full_subsequent_links <- paste0(root_url, subsequent_links)
local_directory <- "inputs/enrollment/"

if (!dir.exists(local_directory)) dir.create(local_directory, recursive = TRUE)

download_zip_file_with_timeout <- function(zip_url, save_dir, timeout_secs = 300) {
  if (is.na(zip_url)) {
    message("Skipping NA link")
    return(NULL)
  }
  file_name <- str_extract(basename(zip_url), "[^?]+")
  zip_path <- file.path(save_dir, file_name)
  tryCatch({
    response <- GET(zip_url, timeout(timeout_secs), write_disk(zip_path, overwrite = TRUE))
    if (response$status_code == 200) {
      message("Downloaded: ", zip_url, " as ", file_name)
    } else {
      message("Failed to download: ", zip_url, " - Status code: ", response$status_code)
    }
  }, error = function(e) {
    message("Error downloading: ", zip_url, " - ", e$message)
  })
  return(zip_path)
}
downloaded_files <- map(full_subsequent_links, ~ download_zip_file_with_timeout(.x, local_directory))

# MANUAL STEP: Unzip all .txt files (e.g., using 7-Zip) into "inputs/enrollment/"

# =====================================================
# 4. CLEAN & AGGREGATE ENROLLMENT DATA
# =====================================================
# -- Process all unzipped monthly enrollment CSVs

folder_path <- "inputs/enrollment/"
all_csv_files <- list.files(folder_path, 
                            pattern = "^(Monthly_Report_By_Plan|monthly report by plan).*\\.csv$", 
                            full.names = TRUE, 
                            recursive = TRUE)

# Extract dates from filenames in various formats
names_df <- data.frame(
  name = basename(all_csv_files)
) %>%
  mutate(
    date = case_when(
      str_detect(name, "\\d{4}_\\d{2}") ~ str_extract(name, "\\d{4}_\\d{2}"),
      str_detect(name, "\\w{3}\\s\\d{4}_\\d{8}") ~ str_extract(name, "\\d{8}"),
      TRUE ~ NA_character_
    ),
    date = case_when(
      str_detect(date, "^\\d{4}_\\d{2}$") ~ ymd(paste0(date, "_01")),
      str_detect(date, "^\\d{8}$") ~ ymd(date),
      TRUE ~ NA_Date_
    )
  )

# Function to process each .csv file and add standardized date
process_enrollment_file <- function(file_path, date_var) {
  cat("Processing file:", basename(file_path), "\n")
  data <- read_csv(file_path)
  names(data) <- tolower(names(data))
  data <- data %>%
    mutate(date = date_var,
           plan_id = as.character(`plan id`)) %>%
    select(-`plan id`)
  return(data)
}

all_data <- purrr::map2_dfr(all_csv_files, names_df$date, process_enrollment_file)

# Append new to old enrollment data if needed
enrollment_old <- read.csv("inputs/part3-all-enrollment-data-1-7-2025.csv") %>% 
  mutate(plan_id = as.character(plan.id)) %>% 
  select(-plan.id)
enrollment_new <- read.csv("inputs/part3-all-enrollment-data-5-30-2025.csv")
enrollment <- rbind(enrollment_old, enrollment_new)

# Filter and clean: keep only Part D, drop missing, standardize fields
enrollment <- enrollment %>% 
  filter(offers.part.d == "Yes") %>% 
  drop_na(plan_id) %>%
  transmute(enrollment, 
            contract_id = contract.number,  
            plan_id, month = as.Date(date)) %>%
  arrange(month, plan_id) %>%
  distinct()

# HIPAA: Replace "*" (masked counts <=10) with random number [1,10]
enrollment$enrollment <- sapply(enrollment$enrollment, function(x) ifelse(x == "*", sample(1:10, 1), x))
enrollment$enrollment <- as.numeric(enrollment$enrollment)

# Aggregate monthly enrollment, then summarize into 6-month periods
enrollment_monthly <- enrollment %>%
  group_by(plan_id, month) %>%
  summarise(enrollment = sum(enrollment), .groups = 'drop') %>%
  group_by(month) %>%
  mutate(total_enrollment = sum(enrollment)) %>%
  ungroup() %>%
  mutate(date = case_when(
    month(month) %in% 1:6 ~ as.Date(paste0(year(month), "-06-30")),
    month(month) %in% 7:12 ~ as.Date(paste0(year(month), "-12-31"))
  ))

# Calculate average enrollment per plan per 6-month period
enrollment_period <- enrollment_monthly %>%
  group_by(plan_id, date) %>%
  summarise(enrollment = mean(enrollment, na.rm = TRUE),
            total_enrollment = mean(total_enrollment, na.rm = TRUE), .groups = 'drop')

# Optional: plot enrollment over time
ggplot(enrollment_period, aes(x = date, y = enrollment, fill = plan_id)) +
  geom_col(position = "stack") + theme(legend.position = "none")

# Save result
write_parquet(enrollment_period, 'parquet/enrollment_period.parquet')



