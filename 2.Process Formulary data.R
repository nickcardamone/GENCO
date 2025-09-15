#Title: 2.Process Formulary Data
#Author: "Nicholas Cardamone"
#Created: "5/9/2025"

#Goal: Use 2007-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans 
# to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination 
# (‘molecule’) of orally administered drugs in each year after NME approval

# What this code does:
# 1. The files have different formats (.dta vs. .txt vs. .zip) so the code will clean them in different ways depending on what format they're in. Then it will smash the datasets together.

# Load necessary packages
library(rdrop2) # connect to dropbox
library(httr) # webscraping
library(stringr) # process string variables
library(lubridate) # process date variables
library(haven) # read files of variosu formats
library(dplyr) # data manipulation
library(tidyverse) # data manipulation
library(xfun) # misc functions
library(data.table) # working with big data
library(arrow) # working with big data

## Part D data upload}
# Define the path to the folder containing the .dta, .txt, and .zip files
folder_path <- "P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//part1//formulary_data//"

# List all .dta, .txt, and .zip files in the folder that end with "with drug names"
all_files <- list.files(folder_path, pattern = ".(dta|txt|zip)$", full.names = TRUE)

# Process the list of drug files and extract dates
names_df <- data.frame(
  file_path = all_files,
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
  ) %>% arrange(desc(date))

# Function to convert characters to numeric and back
convert_to_numeric <- function(x) {
  as.numeric(as.factor(x))
}

convert_to_character <- function(x, original_values) {
  levels <- levels(as.factor(original_values))
  return(levels[x])
}

# Function to process each file based on the extracted date
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
    data <- read_delim(file_path, delim = "|", col_types = cols())  # Reading with pipe delimiter
  } else {
    stop("Unsupported file type")
  }
  
  # Convert all column names to lowercase
  names(data) <- tolower(names(data))
  
  # Check if 'rxcui', 'drugname', and 'labelname' columns exist, if not, add them with NA values
  if (!"rxcui" %in% names(data)) {
    data$rxcui <- NA
  }
  if (!"drugname" %in% names(data)) {
    data$drugname <- NA
  }
  
  # Convert to data.table if not already
  setDT(data)
  
  # Ensure consistent data types
  data[, `:=`(
    formulary_id = as.character(formulary_id),
    rxcui = as.character(rxcui),
    drugname = as.character(drugname),
    prior_authorization_yn = fifelse(as.character(prior_authorization_yn) == "Y", 1L, 0L),
    ndc_raw = as.character(ndc),
    ndc11_str = str_pad(ndc, 11, pad = "0"),
    ndc = as.numeric(as.character(ndc)))
    ]

  # Cleaning the NDC variable to make it compatible for later joining.
  data[, `:=`(ndc_trimmed = substr(ndc, 1, nchar(ndc) - 2))] # Remove the last two digits from ndc11
  data[, `:=`(seg2 = substr(ndc_trimmed, nchar(ndc_trimmed) - 3, nchar(ndc_trimmed)))] # Extract the last 4 digits as seg2 
  data[, `:=`(seg1 = substr(ndc_trimmed, 1, nchar(ndc_trimmed) - 4))] # Extract the remaining part as seg1
  data[, `:=`(seg1_padded = fifelse(nchar(seg1) < 5, sprintf("%05d", as.numeric(seg1)), as.character(seg1)))] # String pad seg1 to 4 characters if it is less than 5 characters
  data[, `:=`(PRODUCTNDC = paste(seg1_padded, seg2, sep = "-"))] # paste together segments to create the PRODUCTNDC variable
  
  # Convert the date string to Date type
  date_var <- as.Date(date_var, format = "%Y%m%d")
  
  # Select the required columns and add the "date" column
  data_selected <- data[, .(
    drugname,
    formulary_id,
    rxcui,
    ndc11_str,
    ndc_raw,
    PRODUCTNDC,
    prior_authorization_yn,
    tier_level_value,
    step_therapy_yn,
    quantity_limit_yn,
    quantity_limit_amount,
    quantity_limit_days,
    date = date_var
  )]
}

setwd("YOUR DIRECTORY")

# Apply the function to all files and row bind the results to one data frame:
combined_data <- mapply(
  process_file, # the function
  names_df$file_path, # the files
  names_df$date, # date 12/31 or 06/30
  SIMPLIFY = FALSE
) %>% bind_rows() %>% 
  distinct(drugname, 
           date, 
           formulary_id, 
           ndc11_str, 
           ndc_raw, 
           PRODUCTNDC, 
           rxcui, 
           prior_authorization_yn,     
           tier_level_value,
           step_therapy_yn,
           quantity_limit_yn,
           quantity_limit_amount,
           quantity_limit_days) %>% 
  write_parquet("parquet/combined_data.parquet")




