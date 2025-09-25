# Title: 2.Process Formulary Data
# Author: "Nicholas Cardamone"
# Created: "5/9/2025"

# Goal: Use 2007-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans 
# to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination 
# (‘molecule’) of orally administered drugs in each year after NME approval

# This script:
# 1. Handles multiple input formats (.dta, .txt, .zip) and processes them according to type
# 2. Standardizes and cleans the data, then combines all years into a single dataset for downstream analysis

# Load dependencies for file IO, data manipulation, and string/date handling
library(rdrop2)        # Interface with Dropbox
library(httr)          # For web scraping (not explicitly used here)
library(stringr)       # String manipulation functions
library(lubridate)     # Date and time processing
library(haven)         # Read files in various formats, especially Stata (.dta)
library(dplyr)         # Data manipulation
library(tidyverse)     # Collection of core data science packages
library(xfun)          # Miscellaneous utility functions
library(data.table)    # Fast data manipulation, especially for large datasets
library(arrow)         # Efficient file storage (parquet format)

## --- Part D Data Upload ---

# Define the local directory where the input files are stored
folder_path <- "part1//formulary_data//"

# List all input files ending in .dta, .txt, or .zip that reference 'drug names'
all_files <- list.files(folder_path, pattern = ".(dta|txt|zip)$", full.names = TRUE)

# Create a dataframe with file paths and extracted metadata (including the date)
names_df <- data.frame(
  file_path = all_files,
  name = basename(all_files)
) %>%
  mutate(
    # Extract date information from filenames using regex
    date = case_when(
      str_detect(name, "\\d{8}") ~ str_extract(name, "\\d{8}"),                 # Use YYYYMMDD if found
      str_detect(name, "\\d{4}-12") ~ str_replace(str_extract(name, "\\d{4}-12"), "-12", "1231"), # Convert YYYY-12 to YYYY1231
      str_detect(name, "\\d{4}") ~ paste0(str_extract(name, "\\d{4}"), "0630"), # Use June 30 if only year is found
      TRUE ~ NA_character_   # Assign NA if date not found
    )
  ) %>% arrange(desc(date)) # Sort by date descending

# --- Helper functions ---

# Convert categorical to numeric (using factor levels)
convert_to_numeric <- function(x) {
  as.numeric(as.factor(x))
}

# Convert numeric back to original categorical values using supplied reference
convert_to_character <- function(x, original_values) {
  levels <- levels(as.factor(original_values))
  return(levels[x])
}

# --- Main file processing function ---

process_file <- function(file_path, date_var) {
  # Print progress for tracking
  cat("Processing file:", basename(file_path), "\n")
  
  # Get file extension/type
  file_extension <- file_ext(file_path)
  
  # If file is a .zip, unzip and grab the first .txt file inside
  if (file_extension == "zip") {
    temp_dir <- tempdir()
    unzip(file_path, exdir = temp_dir)
    txt_files <- list.files(temp_dir, pattern = "\\.txt$", full.names = TRUE)
    if (length(txt_files) == 0) {
      stop("No .txt file found in the zip archive")
    }
    file_path <- txt_files[1]   # Use first .txt file found
    file_extension <- "txt"     # Update extension for next steps
  }
  
  # Read the file according to type
  if (file_extension == "dta") {
    data <- read_dta(file_path)
  } else if (file_extension == "txt") {
    data <- read_delim(file_path, delim = "|", col_types = cols())  # Pipe-delimited text
  } else {
    stop("Unsupported file type")
  }
  
  # Standardize all column names to lower case for consistency
  names(data) <- tolower(names(data))
  
  # If certain columns are missing, add them as NA for uniformity
  if (!"rxcui" %in% names(data)) {
    data$rxcui <- NA
  }
  if (!"drugname" %in% names(data)) {
    data$drugname <- NA
  }
  
  # Convert to data.table (if not already), for fast efficient manipulation
  setDT(data)
  
  # Enforce consistent data types, handle missing columns, and create standardized NDC variables
  data[, `:=`(
    formulary_id = as.character(formulary_id),
    rxcui = as.character(rxcui),
    drugname = as.character(drugname),
    prior_authorization_yn = fifelse(as.character(prior_authorization_yn) == "Y", 1L, 0L), # Convert Yes/No to 1/0
    ndc_raw = as.character(ndc),
    ndc11_str = str_pad(ndc, 11, pad = "0"),   # Pad NDC to 11 digits
    ndc = as.numeric(as.character(ndc))
  )]
  
  # Clean and split NDC as needed for future joins
  data[, `:=`(ndc_trimmed = substr(ndc, 1, nchar(ndc) - 2))]  # Remove last two digits for 9-digit NDC
  data[, `:=`(seg2 = substr(ndc_trimmed, nchar(ndc_trimmed) - 3, nchar(ndc_trimmed)))]  # Last 4 digits (segment 2)
  data[, `:=`(seg1 = substr(ndc_trimmed, 1, nchar(ndc_trimmed) - 4))]                   # First digits (segment 1)
  data[, `:=`(seg1_padded = fifelse(nchar(seg1) < 5, sprintf("%05d", as.numeric(seg1)), as.character(seg1)))] # Pad seg1
  data[, `:=`(PRODUCTNDC = paste(seg1_padded, seg2, sep = "-"))]                        # Standard PRODUCTNDC format
  
  # Convert the extracted date (string) to Date object
  date_var <- as.Date(date_var, format = "%Y%m%d")
  
  # Select relevant columns for the output, adding the processed date
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

# Set working directory to user-defined location (edit as needed)
setwd("YOUR DIRECTORY")

# Process all files, combine their output, deduplicate, and write to parquet
combined_data <- mapply(
  process_file,                     # The function to apply
  names_df$file_path,               # File paths (argument 1)
  names_df$date,                    # Dates parsed from filenames (argument 2)
  SIMPLIFY = FALSE
) %>%
  bind_rows() %>%                   # Combine all processed data frames
  distinct(
    drugname, 
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
    quantity_limit_days
  ) %>%                             # Remove duplicate rows based on these key columns
  write_parquet("parquet/combined_formulary_data.parquet")  # Save as parquet for efficient storage and downstream use

