# Title: 5.Plan XW and Enrollment
# Author: "Nicholas Cardamone"
# Date: "5/16/2025"
# Last updated: 5/30/2025

# Goals: Webscrape plan enrollment by month files, 
# then download formulary-plan-period files to crosswalk enrollment to prior auth 
# by formulary Medicare Part D dataset from Part 2. Aggregate a dataset with prior auth of plans 
# by application number by period with plan enrollment numbers.

# What this code does:
#1. Download formulary by plan by period files from Dropbox - save to a directory folder called "plan".
#2. Combine all formulary by plan by period files into one dataframe.
#3. Webscrape plan enrollment by month data from CMS website.
#4. Combine all monthly plan enrollment files into one dataframe, summarize by January - June / July - December.
#5. Extract and monthly enrollment data 2007-2003 and aggregate to 6-month periods (plan level) by taking the average monthly enrollment
#proportion (among all plans) for a plan by period (i.e. in period X, Plan A has 100 people but there are 400 enrollees across all plans, then Plan A receives a weight of 0.25). 
#6. Join enrollment data to plan-formulary crosswalk dataset at 6-month period. Now that enrollment data is now organized at the formulary level with the sum enrollment weight (e.g. formulary is covered by Plan A and Plan C which have 25% and 14% of all monthly enrollees for period X, so 39% of all average monthly enrollees).
#7. Join formulary-level enrollment data to formulary-level prior authorization data. The data is now organized as ingredient > application number (branded/generic) > RxCUI > NDC > formulary > period.


# Load necessary packages
library(httr) # webscraping
library(stringr) # process string variables
library(lubridate) # process date variables
library(haven) # read files of variosu formats
library(dplyr) # data manipulation
library(tidyverse) # data manipulation
library(xfun) # misc functions
library(data.table) # working with big data
library(arrow) # working with big data
library(pbapply) # progress bar
library(readr) # reading in different data types
library(tools) # utility 
library(devtools) # utility

## Search Dropbox for relevant files
# Define the root folder path in Dropbox
root_folder_path <- "GENCO"
drop_acc() %>% data.frame()
#drop_acc() %>% data.frame()
# List all folders and files in the root folder
all_files <- drop_dir(root_folder_path, dtoken= token)
# Filter for .dta files that start with "basic drugs formulary"
x <- drop_search("^plan information")

## Download relevant files locally
# Create the download function with checks for existing files
download_function <- function(match_item) {
  # Define the local path where the file will be downloaded
  local_file_path <- file.path("YOUR PATH", basename(match_item$metadata$path_lower))
  
  # Check if the file already exists
  if (!file.exists(local_file_path)) {
    # Try downloading the file, handle any potential errors
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

## Apply the function to each element in the list using lapply
lapply(x$matches, download_function)

## Process Plan - Formulary Crosswalk Data Files
# Define the path to the folder containing the .dta, .txt, and .zip files
folder_path <- "YOUR PATH"

## List all .dta, .txt, and .zip files in the folder that end with "with drug names"
all_files <- list.files(folder_path, pattern = ".(dta|txt|zip)$", full.names = TRUE)

### Switch this to process by quarters

## Process the list of drug files and extract dates
names_df <- data.frame(
  name = basename(all_files)
) %>%
  mutate(
    date = case_when(
      # Extract exact date in YYYYMMDD format
      str_detect(name, "\\d{8}") ~ str_extract(name, "\\d{8}"),
      
      # If the name contains a format like YYYY-12, convert it to YYYY1231
      str_detect(name, "\\d{4}-12") ~ str_replace(str_extract(name, "\\d{4}-12"), "-12", "1231"),
      
      # If no date is found, extract the year and append 0630
      str_detect(name, "\\d{4}") ~ paste0(str_extract(name, "\\d{4}"), "0630"),
      
      # Default NA if no date or year is found
      TRUE ~ NA_character_
    )
  )

## Function to process each file based on the extracted date, with a sample of 50 observations
process_file <- function(file_path, date_var) {
  # Print the name of the file being processed
  cat("Processing file:", basename(file_path), "\n")
  
  # Detect the file extension
  file_extension <- file_ext(file_path)
  
  # Unzip if the file is a .zip and get the contained .txt file
  if (file_extension == "zip") {
    temp_dir <- tempdir()
    unzip(file_path, exdir = temp_dir)
    txt_files <- list.files(temp_dir, pattern = "\\.txt$", full.names = TRUE)
    
    if (length(txt_files) == 0) {
      stop("No .txt file found in the zip archive")
    }
    
    file_path <- txt_files[1]  # Assuming you want to process the first .txt file found
    file_extension <- "txt"  # Update the extension to .txt for further processing
  }
  
  # Read the file based on its extension
  if (file_extension == "dta") {
    data <- read_dta(file_path)
  } else if (file_extension == "txt") {
    # Adjusting to read the specified headers
    data <- read_delim(file_path, delim = "|", col_types = cols(), 
                       col_names = c("CONTRACT_ID", "PLAN_ID", "SEGMENT_ID", "CONTRACT_NAME", "PLAN_NAME", 
                                     "FORMULARY_ID", "PREMIUM", "DEDUCTIBLE", "ICL", "MA_REGION_CODE", 
                                     "PDP_REGION_CODE", "STATE", "COUNTY_CODE", "SNP"))  
  } else {
    stop("Unsupported file type")
  }
  
  # Convert all column names to lowercase for consistency
  names(data) <- tolower(names(data))
  
  # Ensure consistent data types
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
  
  # Convert the date string to Date type
  date_var <- as.Date(date_var, format = "%Y%m%d")
  
  # Select the required columns and add the "date" column
  data_selected <- data %>%
    select(contract_id, plan_id, segment_id, contract_name, plan_name, formulary_id, premium, deductible, icl,
           ma_region_code, pdp_region_code, state, county_code, snp) %>%
    mutate(date = date_var)
  
  return(data_selected)
}

## Run function on all plan files and combine
# Apply the function to all files and combine the results into one data frame

combined_plan_data <- mapply(
  process_file, 
  all_files, 
  names_df$date, 
  SIMPLIFY = FALSE
) %>% bind_rows()


## Clean Plan Data
# Importantly - pad the plan_id with 0s to eventually join it to formulary-level data.
combined_plan_data <- combined_plan_data %>% 
  select(contract_id, plan_id, formulary_id, segment_id, date) %>% 
  mutate(plan_id = str_pad(plan_id, 3, pad = "0")) %>% 
  distinct()

#write.csv(combined_plan_data, "combined_plan_data_n=134198-5-30-2025.csv")

# Load Plan-Formulary Data
# We added 2024 data in June 2025 so instead of re-downloading everything, just download, process, and tack on new data to old dataset.

combined_plan_data_old <- read.csv("input/combined_plan_data_n=134198-10-31-2024.csv")
combined_plan_data_new <- read.csv("combined_plan_data_n=134198-5-30-2025.csv")

combined_plan_data <- rbind(combined_plan_data_old, combined_plan_data_new) %>% distinct()

combined_plan_data <- combined_plan_data %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>% distinct()

## Clean Plan-Formulary Data
combined_plan_data <- combined_plan_data %>% filter(segment_id != "SEGMENT_ID")
combined_plan_data$segment_id <- as.factor(as.numeric(combined_plan_data$segment_id))
combined_plan_data$plan_id <- as.factor(as.numeric(combined_plan_data$plan_id))

combined_plan_data <- combined_plan_data %>% 
  select(formulary_id, contract_id, plan_id, segment_id, date) %>% 
  transmute(plan_id = as.factor(plan_id), 
            date = as.Date(date),
            formulary_id = str_pad(formulary_id, 8, pad = "0")) %>% 
  distinct() %>% write_parquet("parquet/combined_plan_data.parquet")

## Webscraping enrollment data:
# 1.) Webscrape all links from list of monthly enrollment number webpages.
# 2.) Open all such links and click the hyperlink to download the respective zip file.
# 3.) Manually unzip all the .txt files using 7-ZIP.
# 4.) Read .txt files and combine like we do above.

# Step 1: Read the main page and extract the links from the table
main_url <- "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan"  # Replace with the URL of the main page

# Read the HTML of the main page
main_page <- read_html(main_url, user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")

# Extract all the links in the table
links <- main_page %>%
  html_nodes("table a") %>%  # Replace "table a" with the correct CSS selector for your table links
  html_attr("href") %>%
  unique()  # Ensure unique links

# Step 2: Create a function to extract the subsequent link from each page
safe_get_subsequent_link <- function(link, retries = 3, sleep_time = 1) {
  full_url <- url_absolute(link, main_url)
  attempt <- 1  # Initialize attempt counter
  
  while (attempt <= retries) {
    # Try to read the HTML and extract the link
    tryCatch({
      message("Attempt ", attempt, " for link: ", full_url)
      
      # Fetch the page with a user-agent and timeout
      page <- read_html(GET(full_url, timeout(30), user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"))
      
      # Extract the link from the subsequent page
      subsequent_link <- page %>%
        html_nodes("li.field__item > a:nth-child(1)") %>%  # Use your CSS selector
        html_attr("href") %>%  # Adjust this if it's not an <a> tag
        unique() %>%
        .[1]  # Assuming you want the first link found, adjust as necessary
      
      # If successful, return the extracted link
      return(subsequent_link)
    }, error = function(e) {
      message("Error in attempt ", attempt, " for link: ", full_url, " - ", e$message)
      
      # Increment the attempt counter and sleep before retrying
      attempt <- attempt + 1
      Sys.sleep(sleep_time)  # Sleep for specified time between retries
    })
  }
  
  # If all retries fail, return NA to indicate the failure
  message("All attempts failed for link: ", full_url)
  return(NA)
}

# Step 2: Apply the retry logic to each link
subsequent_links <- map(links, ~ safe_get_subsequent_link(.x))

#Combine links and subsequent links into a data frame for easier inspection
result <- data.frame(main_page_link = links, subsequent_page_link = subsequent_links)

## Download zipped up .txt files from the list of links.
# Define the root URL
root_url <- "https://www.cms.gov"

# Prepend the root URL to all links in subsequent_links
full_subsequent_links <- paste0(root_url, subsequent_links)

local_directory <- "input//enrollment//"

# Function to download the zip files with a timeout setting
download_zip_file_with_timeout <- function(zip_url, save_dir, timeout_secs = 300) {
  if (is.na(zip_url)) {
    message("Skipping NA link")
    return(NULL)
  }
  
  # Define a simple filename based on the basename of the URL, removing query strings
  file_name <- str_extract(basename(zip_url), "[^?]+")
  
  # Define the local file path for the zip file
  zip_path <- file.path(save_dir, file_name)
  
  # Download the file with a timeout using httr::GET and write to a file
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

# Create the directory if it does not exist
if (!dir.exists(local_directory)) {
  dir.create(local_directory, recursive = TRUE)
}

# Apply the function to each link and download the files with a timeout
downloaded_files <- map(full_subsequent_links, ~ download_zip_file_with_timeout(.x, local_directory))

# Use 7-zip to extract all enrollment numbers by plan files - save to a directory called "enrollment".

# Define the path to the folder containing the .csv files
folder_path <- "input//enrollment//"

# List all .csv files in the folder and subfolders that start with either "Monthly_Report_By_Plan" or "monthly report by plan"
all_csv_files <- list.files(folder_path, 
                            pattern = "^(Monthly_Report_By_Plan|monthly report by plan).*\\.csv$", 
                            full.names = TRUE, 
                            recursive = TRUE)

# Extract dates from the filenames in both YYYY_MM and Dec 2008_12082008 formats
names_df <- data.frame(
  name = basename(all_csv_files)
) %>%
  mutate(
    # Extract date in the format YYYY_MM
    date = case_when(
      # Format YYYY_MM
      str_detect(name, "\\d{4}_\\d{2}") ~ str_extract(name, "\\d{4}_\\d{2}"),
      
      # Format with month name and date like "Dec 2008_12082008"
      str_detect(name, "\\w{3}\\s\\d{4}_\\d{8}") ~ str_extract(name, "\\d{8}"),
      
      # Default NA if no date is found
      TRUE ~ NA_character_
    ),
    
    # Convert extracted date to Date format (YYYY_MM is assumed to be the first day of the month)
    date = case_when(
      str_detect(date, "^\\d{4}_\\d{2}$") ~ ymd(paste0(date, "_01")),
      str_detect(date, "^\\d{8}$") ~ ymd(date),  # Handle exact dates like 12082008
      TRUE ~ NA_Date_
    )
  )

# Function to process each .csv file based on the extracted date
process_file <- function(file_path, date_var) {
  # Print the name of the file being processed
  cat("Processing file:", basename(file_path), "\n")
  
  # Read the .csv file with specified column names
  data <- read_csv(file_path)
  
  # Convert all column names to lowercase for consistency
  names(data) <- tolower(names(data))
  
  # Add the "date" column
  data <- data %>%
    mutate(date = date_var,
           plan_id = as.character(`plan id`)) %>% select(-`plan id`)
  
  
  
  return(data)
}

# Process each file and combine the results into a single data frame
all_data <- purrr::map2_dfr(all_csv_files, names_df$date, process_file)

#write.csv(all_data, "part3-all-enrollment-data-5-30-2025.csv")

# We added 2024 data in June 2025 so instead of re-downloading everything, just download, process, and tack on new data to old dataset.
enrollment_old <- read.csv("input//part3-all-enrollment-data-1-7-2025.csv") %>% 
  mutate(plan_id = as.character(plan.id)) %>% 
  select(-plan.id)
enrollment_new <- read.csv("input//part3-all-enrollment-data-5-30-2025.csv")

enrollment <- rbind(enrollment_old, enrollment_new)

## Clean Enrollment Data & Create indicator for 6 month period
enrollment <- enrollment %>% 
  filter(offers.part.d == "Yes") %>% 
  drop_na(plan_id) %>% 
  transmute(enrollment, 
            contract_id = contract.number,  
            plan_id, month = as.Date(date)
  ) %>% 
  arrange(month, plan_id) %>% 
  distinct()

#  Note: The privacy laws of HIPAA have been interpreted to prohibit publishing 
#  enrollment data with values of 10 or less. Data rows with enrollment values
#  of 10 or less have been removed from this file. The complete file that includes
#  these removed rows but with blanked out enrollment data is also available for
#  download. 

enrollment$enrollment <- sapply(enrollment$enrollment, function(x) ifelse(x == "*", sample(1:10, 1), x))

# Now that the astericks are removed, convert the variable to numeric.
enrollment$enrollment <- as.numeric(enrollment$enrollment)

enrollment_monthly <- enrollment %>% 
  group_by(plan_id, month) %>% 
  summarise(enrollment = sum(enrollment)) %>%
  group_by(month) %>% 
  mutate(total_enrollment = sum(enrollment)) %>% 
  ungroup() %>% 
  mutate(date = case_when(
             month(month) %in% 1:6 ~ as.Date(paste0(year(month), "-06-30")), # Summarize by first 6 months of year and
             month(month) %in% 7:12 ~ as.Date(paste0(year(month), "-12-31")) # last 6 months of the year.
           )) 

# Aggregate at 6-month period and create enrollment weights
# Group by contract_num, plan_id, and the 6-month period, then summarize enrollment
# * For each plan we have monthly enrollment numbers, we want 6-month chunks to align with the prior auth data.

enrollment_period <- enrollment_monthly %>% 
  group_by(plan_id, date) %>% 
  summarise(enrollment = mean(enrollment, na.rm = T),
            total_enrollment = mean(total_enrollment, na.rm = T))
  
ggplot(enrollment_period, aes(x = date, y = enrollment, fill = plan_id)) +
  geom_col(position = "stack") + theme(legend.position = "none")

write_parquet(enrollment_period, 'parquet/enrollment_period.parquet')




