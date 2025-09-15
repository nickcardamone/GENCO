#title: 2.Process Formulary Data
#author: "Nicholas Cardamone"
#date: "5/9/2024"

#Goal: Use 2006-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination (‘molecule’) of orally administered drugs in each year after NME approva

# What this code does:
# 1. The files have different formats (.dta vs. .txt vs. .zip) so the code will clean them in different ways depending on what format they're in. Then it will smash the datasets together.
# 2. There is one very weird dataset, 2006-06, which had to be cleaned manually then bound with the others.
# 3. Output of full_data.csv is all Part D files from 2006-06 to 2023-12.

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
library(arrow)


## Part D data upload}
# Define the path to the folder containing the .dta, .txt, and .zip files
folder_path <- "P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//part1//formulary_data//"

# List all .dta, .txt, and .zip files in the folder that end with "with drug names"
all_files <- list.files(folder_path, pattern = ".(dta|txt|zip)$", full.names = TRUE)

# Process the list of drug files and extract dates
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

# Function to convert characters to numeric and back
convert_to_numeric <- function(x) {
  as.numeric(as.factor(x))
}

convert_to_character <- function(x, original_values) {
  levels <- levels(as.factor(original_values))
  return(levels[x])
}

# Function to process each file based on the extracted date, with a sample of 50 observations
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
    # step_therapy_yn,
    # quantity_limit_yn,
    date = date_var
  )]
}
setwd("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//")

# Apply the function to all files and combine the results into one data frame
combined_data <- mapply(
  process_file, 
  all_files, 
  names_df$date, 
  SIMPLIFY = FALSE
) %>% bind_rows() %>% 
  distinct(drugname, date, formulary_id, ndc11_str, ndc_raw, PRODUCTNDC, rxcui, prior_authorization_yn) %>% 
  write_parquet("parquet/combined_data.parquet")

combined_data <- combined_data %>% collect()

combined_data %>% select(ndc11_str) %>% distinct() %>% write.csv("unique_ndc11s.csv", row.names = FALSE, quote = FALSE)




#setwd("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//")

#write_parquet(combined_data, "parquet/combined_data.parquet")
combined_data <- open_dataset("parquet/combined_data.parquet") #%>% nrow()


# Test if the same rxcui has different prior auth values for the same formulary in the same period.
# Drop rows with NA in ndc, group by rxcui and formulary_id, and calculate mean
pa_test = combined_data %>% 
  filter(!is.na(PRODUCTNDC)) %>% collect() #%>% 
  #group_by(rxcui, date, formulary_id) %>% 
  #mutate(mpa = mean(prior_authorization_yn, na.rm = TRUE)) %>% 
  #collect()


## Count if there's a difference in prior auth in the first 9 digits of NDC vs. full, 11-digit, NDC. There's about 7000 rows with a different prior_auth for two different ndc11 (from the same ndc9 stem) in the same formulary in the same 6-month period.
## NDC resources
# https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions
# https://health.maryland.gov/phpa/OIDEOR/IMMUN/Shared%20Documents/Handout%203%20-%20NDC%20conversion%20to%2011%20digits.pdf
# https://forums.ohdsi.org/t/how-to-determine-whether-a-drug-is-brand-or-generics/5745/5

## Test:

check2009 <- readstata13::read.dta13("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//part1//formulary_data//test//basic drugs formulary file  20071231 with drug names.dta")
check2019 <- read_delim("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//part1//formulary_data//basic drugs formulary file  20191231.txt", delim = "|", col_types = cols(.default = col_character())) 

setDT(check2009)
check2009[, `:=`(ndc11 = sprintf("%011d", as.integer(ndc)))] # Remove the last two digits from ndc11
check2009[, `:=`(ndc11_str = str_pad(ndc, 11, pad = "0"))] # Remove the last two digits from ndc11

check2009_test <- check2009 %>% select(ndc11) %>% distinct() %>% as.data.frame()
check2009_test2 <- check2009 %>% mutate(ndc = as.integer(as.numeric(ndc))) %>% select(ndc) %>% distinct() %>% as.data.frame()
check2009_test3 <- check2009 %>% select(ndc11_str) %>% distinct() %>% as.data.frame()


test = check2009_test2 %>% inner_join(check2009_test, by = c("ndc" = "ndc11"))

check2009_na = check2009 %>% filter(is.null(ndc11))


alik_hemi2009 = check2009 %>% filter(grepl("aliskiren|tekturna", drugname, ignore.case = T))
alik_hemi2009_processed = combined_data %>% filter(grepl("aliskiren|tekturna", drugname, ignore.case = T)) %>% collect()


alik_hemi2019 = check2019 %>% filter(grepl("66993", NDC, ignore.case = T))




```{r}
combined_data1 %>% distinct(date, ingredient, formulary_id, prior_authorization_yn) %>% group_by(date, ingredient) %>% 
  summarise(num_nopauth = sum(prior_authorization_yn == 0, na.rm = TRUE),  # Formularies with at least 1 NDC with pauth = 0.
            total_formularies = n_distinct(formulary_id),
            prop_pauth_free = ifelse(total_formularies > 0, round(num_nopauth / total_formularies, 2), NA)) %>% ggplot(aes(x=as.Date(date), y= ingredient, alpha = prop_pauth_free)) + geom_tile(fill = "blue1") + labs(y="Ingredient", x= "Date") + scale_x_date() +theme_minimal()

```

```{r}
combined_data1 %>% distinct(date, ingredient, formulary_id, prior_authorization_yn, ndc) %>% filter(ingredient %in% c("SEMAGLUTIDE", "ABIRATERONE ACETATE", "ABALOPARATIDE")) %>% group_by(date, ingredient, ndc) %>% 
  summarise(num_nopauth = sum(prior_authorization_yn == 0, na.rm = TRUE),  # Formularies with at least 1 NDC with pauth = 0.
            total_formularies = n_distinct(formulary_id),
            prop_pauth_free = ifelse(total_formularies > 0, round(num_nopauth / total_formularies, 2), NA)) %>% ggplot(aes(x=as.Date(date), y= interaction(ndc, ingredient), alpha = prop_pauth_free)) + geom_tile(fill = "blue1") + labs(y="Ingredient", x= "Date") + scale_y_discrete(guide = "axis_nested") + scale_x_date() +theme_minimal()

```


```{r}
# Sample a subset
#merged_subset <- merged_dt[1:5000]

# Perform the join safely
combined_data1 <- combined_data[merged_dt_test, on = "PRODUCTNDC", allow.cartesian = TRUE]
```




```{r}
combined_data1 %>% distinct(date, appl_no, approval_date1, ingredient, formulary_id, prior_authorization_yn, generics_presence) %>% group_by(date, approval_date1, ingredient) %>% 
  summarise(num_nopauth = sum(prior_authorization_yn == 0, na.rm = TRUE),  # Formularies with at least 1 NDC with pauth = 0.
            total_formularies = n_distinct(formulary_id),
            prop_pauth_free = ifelse(total_formularies > 0, round(num_nopauth / total_formularies, 2), NA)) %>% ggplot(aes(x=as.Date(date), y = reorder(ingredient, as.Date(approval_date1)), alpha = prop_pauth_free)) + geom_point(aes(x=as.Date(approval_date1), y = reorder(ingredient, as.Date(approval_date1))), color = "red3") + geom_tile(fill = "blue1") + labs(y="Ingredient", x= "Date") + scale_x_date() +theme_minimal()

```

```{r}
combined_data1  %>% distinct(date, approval_date1, ingredient, formulary_id, prior_authorization_yn, generics_presence, appl_no) %>% group_by(date, approval_date1, ingredient, appl_no) %>% 
  summarise(num_nopauth = sum(prior_authorization_yn == 0, na.rm = TRUE),  # Formularies with at least 1 NDC with pauth = 0.
            total_formularies = n_distinct(formulary_id),
            prop_pauth_free = ifelse(total_formularies > 0, round(num_nopauth / total_formularies, 2), NA)) %>% ggplot(aes(x=as.Date(date), y = interaction(appl_no, ingredient), alpha = prop_pauth_free)) + geom_point(aes(x=as.Date(approval_date1), y = interaction(appl_no, ingredient)), color = "red3") + scale_y_discrete(guide = "axis_nested") + geom_tile(fill = "blue1") + labs(y="Ingredient", x= "Date") + scale_x_date() +  labs(subtitle = "Formularies free of Prior Auth across all covered NDCs", fill = "Prop Formularies No Prior Auth") +  # set legend title
  theme(legend.position = "bottom") 

```

```{r}
combined_data1  %>% distinct(date, approval_date1, ingredient, formulary_id, prior_authorization_yn, generics_presence, appl_no, PRODUCTNDC, ndc) %>% group_by(date, approval_date1, ingredient, appl_no, ndc) %>% 
  summarise(num_nopauth = sum(prior_authorization_yn == 0, na.rm = TRUE),  # Formularies with at least 1 NDC with pauth = 0.
            total_formularies = n_distinct(formulary_id),
            prop_pauth_free = ifelse(total_formularies > 0, round(num_nopauth / total_formularies, 2), NA)) %>% ggplot(aes(x=as.Date(date), y = interaction(ndc, appl_no, ingredient), alpha = prop_pauth_free)) + geom_point(aes(x=as.Date(approval_date1), y = interaction(ndc, appl_no, ingredient)), color = "red3") + scale_y_discrete(guide = "axis_nested") + geom_tile(fill = "blue1") + labs(y="Ingredient", x= "Date") + scale_x_date() +  labs(subtitle = "Formularies free of Prior Auth across all covered NDCs", fill = "Prop Formularies No Prior Auth") +  # set legend title
  theme(legend.position = "bottom") 

```

```{r}
test_cd <- combined_data %>% select(seg1_padded, PRODUCTNDC, date, prior_authorization_yn) %>% filter(seg1_padded %in% c("0169", "47335", "57894", "77205", "60505", "0378", "0143")) %>%drop_na() %>%  distinct()

```

```{r}

combined_data1 %>% filter(ingredient == 57894015012) %>% ggplot(aes(x = date, y = formulary_id, fill = as.factor(prior_authorization_yn))) + geom_tile(alpha = 0.5)
```


```{r}
combined_data1 %>% filter(ingredient == "ABIRATERONE ACETATE") %>% distinct(date, prior_authorization_yn, appl_no, formulary_id) %>% ggplot(aes(x = date, fill = as.factor(prior_authorization_yn))) + geom_bar() + facet_grid(rows = vars(appl_no))
```


