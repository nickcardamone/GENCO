# Title: 3. Query RXCUI
# Author: Nicholas Cardamone
# Date: 5/9/2025
# Last updated: 5/30/2025

# ----------------------------------------------------------------------------------
# GOAL:
# Use 2006-2023 Part D formulary files for stand-alone Medicare Part D and Medicare 
# Advantage prescription drug plans to identify plans’ use of prior authorization, 
# quantity limits, and step therapy for each unique brand-generic-dose-formulary 
# combination of orally administered drugs in each year after NME approval.
# ----------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------
# LOAD PACKAGES
# ----------------------------------------------------------------------------------
library(rdrop2)      # Dropbox connection
library(httr)        # HTTP tools
library(stringr)     # String manipulation
library(lubridate)   # Date handling
library(haven)       # Read/write data from/to statistical packages
library(tidyverse)   # Data science tools (includes dplyr, ggplot2, etc.)
library(xfun)        # Miscellaneous functions
library(data.table)  # Fast data manipulation
library(arrow)       # Parquet file support
library(jsonlite)    # JSON support
library(dplyr)       # Data manipulation (explicitly loaded)
library(pbapply)     # Progress bar apply functions

# Define 'not in' operator
'%!in%' <- function(x, y) !('%in%'(x, y))

# ----------------------------------------------------------------------------------
# LOAD DATASETS
# ----------------------------------------------------------------------------------
combined_data <- open_dataset("parquet/combined_data.parquet")                # Main formulary data
long_mol      <- read.csv("input/long_mol10-30-2024.csv")                     # Molecule-level data

# ----------------------------------------------------------------------------------
# RXMIX CROSSWALK: Link NDCs to NDA/ANDA and marketing categories
# ----------------------------------------------------------------------------------

# Read in crosswalk file and filter out header rows
ndc_nda_xw <- read_delim("xw_ndc_nda.txt") %>%
  filter(ndc != "ndc11_str")

# Retrieve unique NDCs queried
ndc_ndc <- ndc_nda_xw %>%
  select(F1_ndc11) %>%
  distinct()

# Extract marketing category (NDA, ANDA, BLA, NDA Authorized Generics, etc.)
ndc_mc <- ndc_nda_xw %>%
  filter(F2_propName == "MARKETING_CATEGORY") %>%
  mutate(conceptName = F1_conceptName,
         marketing_cat = F2_propValue) %>%
  select(F1_ndc11, conceptName, marketing_cat) %>%
  distinct()

# Review distribution of marketing categories
table(ndc_mc$marketing_cat)

# Extract application numbers for relevant marketing categories
ndc_appl <- ndc_nda_xw %>%
  filter(F2_propName %in% c("ANDA", "NDA", "NDA_AUTHORIZED_GENERIC")) %>%
  mutate(appl_no = F2_propValue) %>%
  select(F1_ndc11, appl_no) %>%
  distinct()

# ----------------------------------------------------------------------------------
# CLEAN AND JOIN DATA
# ----------------------------------------------------------------------------------

# Combine marketing category and application numbers, only keep valid matches
ndc_nda_xw_clean <- ndc_ndc %>%
  left_join(ndc_mc, by = "F1_ndc11") %>%
  left_join(ndc_appl, by = "F1_ndc11") %>%
  mutate(
    ndc11_str    = F1_ndc11,
    appl_str     = ifelse(grepl("^NDA", appl_no), "NDA", 
                          ifelse(grepl("^ANDA", appl_no), "ANDA", NA)),
    marketing_str= ifelse(grepl("^NDA", marketing_cat), "NDA", 
                          ifelse(grepl("^ANDA", marketing_cat), "ANDA", NA)),
    match        = if_else(marketing_str == appl_str, 1, 0)
  ) %>%
  filter(is.na(match) | match == 1) %>%
  select(-F1_ndc11, -match, -marketing_str, -appl_str)

# Check for duplicate NDCs after cleaning (should all be unique)
ndc_nda_xw_clean_count <- ndc_nda_xw_clean %>%
  group_by(ndc11_str) %>%
  summarize(n = n()) # Should all be 1

# Preview final marketing categories
table(ndc_nda_xw_clean$marketing_cat)

# Just take distinct NDA/ANDA and ingredient name combos
long_mol_distinct <- long_mol %>%
  select(appl_no, ingredient, approval_date1) %>%
  distinct()

# Join ingredient data to NDCs, preserving NDCs
ndc_nda_ingredient_xw <- ndc_nda_xw_clean %>%
  left_join(long_mol_distinct, by = "appl_no")

# ----------------------------------------------------------------------------------
# FINAL DATASET: Join to Formulary Data and Clean
# ----------------------------------------------------------------------------------

# Add NDC-NDA-ingredient data to formulary data, and filter for valid ingredients
combined_data1 <- combined_data %>%
  select(drugname, date, formulary_id, ndc11_str, rxcui, 
         prior_authorization_yn, tier_level_value, 
         step_therapy_yn, quantity_limit_yn, 
         quantity_limit_amount, quantity_limit_days) %>%
  collect() %>%
  left_join(ndc_nda_ingredient_xw, by = "ndc11_str") %>%
  drop_na(ingredient)

# Check that FDA approval date is before appearance in formulary data
check <- combined_data1 %>%
  drop_na(ingredient) %>%
  select(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>%
  distinct() %>%
  group_by(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>%
  dplyr::summarise(
    approval_after_appearance = if_else(approval_date1 > date, 1, 0)
  ) %>%
  filter(approval_after_appearance != 1)

# Remove VANSPAR outlier where approval date is after formulary appearance
combined_data1 <- combined_data1 %>%
  filter(conceptName != "buspirone hydrochloride 7.5 MG Oral Tablet [Vanspar]") %>%
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0"))

# Save cleaned, joined dataset
combined_data1 %>% write_parquet('parquet/combined_formulary_data_with_NDA.parquet')

# ----------------------------------------------------------------------------------
# SUMMARY STATISTICS
# ----------------------------------------------------------------------------------
# Count distinct ingredients in each data source
n_long_mol_ingredients <- long_mol %>% select(ingredient) %>% distinct() %>% nrow()         # e.g., 474 in sample
n_combined_data1_ingredients <- combined_data1 %>% select(ingredient) %>% distinct() %>% nrow() # e.g., 370 in Medicare Part D

# ----------------------------------------------------------------------------------
# HANDLE EXCLUDED INGREDIENTS
# ----------------------------------------------------------------------------------

# Read list of excluded ingredients (not joined via RxMix crosswalk)
excl_ing_rg <- read.csv("input/excl_ing5-30-2025_rg.csv")

# Filter out excluded ingredients from long_mol
long_mol_clean <- long_mol %>%
  anti_join(excl_ing_rg, by = "ingredient")

# Review remaining unique ingredients
long_mol_clean %>% select(ingredient) %>% distinct()

# Prepare trade name and ingredient name for manual validation
long_mol_tn <- long_mol %>%
  group_by(ingredient) %>%
  filter(grepl("^NDA", appl_no)) %>%
  summarise(
    names = paste(unique(trade_name), collapse = "; "),
    .groups = "drop"
  )

# Join trade names to excluded ingredients and save for review
excl_ing_rg %>%
  left_join(long_mol_tn, by = "ingredient") %>%
  write.csv("output/excl_ing6-03-2025_rg_nc.csv")

# ----------------------------------------------------------------------------------
# END OF SCRIPT
# ----------------------------------------------------------------------------------
