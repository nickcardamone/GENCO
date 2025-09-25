# Title: 4. Aggregate ingredient data
# Author: Nicholas Cardamone
# Date: "5/16/2025"

# Description: 
# This script processes and aggregates Drugs@FDA data at the ingredient level.
# It cleans source data, identifies reformulations, distinguishes between branded and generic approvals, 
# and summarizes key metrics for each ingredient. Outputs are saved as parquet files for efficient storage and analysis.

# --- Load Required Packages ---
library(httr)        # For web scraping (not used directly in this script, but may be required for related scripts)
library(stringr)     # String processing functions
library(lubridate)   # Handling and formatting date variables
library(haven)       # Reading files in various formats
library(dplyr)       # Data manipulation and transformation
library(tidyverse)   # Includes dplyr and other data science tools
library(xfun)        # Miscellaneous utility functions
library(data.table)  # Efficient handling of large data sets
library(arrow)       # Reading and writing parquet files

# --- Define Utility Functions ---
# Create a 'not in' operator for easier filtering
'%!in%' <- function(x, y) !('%in%'(x, y))

# --- Set Working Directory ---
# Adjust this path as needed for your environment
setwd("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//")

# --- Load Input Data ---
# long_mol: Core dataset containing drug approval data
# excl_ing_rg: List of ingredients to exclude from some analyses
long_mol <- read.csv("input/long_mol10-30-2024.csv")
excl_ing_rg <- read.csv("input/excl_ing5-30-2025_rg.csv")

# --- Identify and Aggregate Reformulations ---
# Flag rows as reformulations if 'doc_type1' indicates such, 
# then count cumulative reformulations per ingredient and approval date
reform <- long_mol %>% 
  mutate(
    reform = if_else(doc_type1 %in% c("12", "13", "15"), 1, 0)
  ) %>%
  arrange(ingredient, approval_date1) %>%
  group_by(ingredient, approval_date1) %>%
  mutate(reform = cumsum(reform)) %>%
  transmute(ingredient, date = approval_date1, reform)

# Save reformulation summary as a parquet file
reform %>% write_parquet('parquet/reform.parquet')

# --- Summarize Generic Approvals ---
# Filter for ANDA (generic) applications, group by ingredient, and summarize:
# - Exclude ingredients in 'excl_ing_rg'
# - Count generics, first generic approval date
long_mol_generic <- long_mol %>% 
  filter(grepl("^ANDA", appl_no)) %>%
  group_by(ingredient) %>%
  summarize(
    generic = 1,
    generic_approvals = n(),
    first_generic_approval_date = as.Date(min(approval_date1, na.rm = TRUE))
  ) %>%
  anti_join(excl_ing_rg, by = "ingredient")

# --- Summarize Branded Approvals and Combine with Generic Data ---
# Filter for NDA (branded) applications, group by ingredient, and summarize:
# - First approval date, total branded approvals, and max combination number
# Merge in generic summary from above
long_mol <- long_mol %>%
  filter(grepl("^NDA", appl_no)) %>%
  group_by(ingredient) %>%
  arrange(ingredient, approval_date1) %>%
  summarize(
    first_approval_date = as.Date(min(approval_date1, na.rm = TRUE)),
    branded_approvals = n(),
    combination_totalnumber = max(combination_totalnumber, na.rm = TRUE)
  ) %>%
  left_join(long_mol_generic, by = "ingredient")

# --- Derive Approval Periods for Branded and Generic Versions ---
# For each ingredient, assign the 6-month period in which branded and generic approvals occurred
long_mol <- long_mol %>%
  mutate(
    period_branded_approval = case_when(
      month(first_approval_date) %in% 1:6 ~ as.Date(paste0(year(first_approval_date), "-06-30")),
      month(first_approval_date) %in% 7:12 ~ as.Date(paste0(year(first_approval_date), "-12-31")),
      TRUE ~ NA_Date_
    ),
    period_generic_approval = case_when(
      is.na(first_generic_approval_date) ~ NA_character_,
      month(first_generic_approval_date) %in% 1:6 ~ paste0(year(first_generic_approval_date), "-06-30"),
      month(first_generic_approval_date) %in% 7:12 ~ paste0(year(first_generic_approval_date), "-12-31"),
      TRUE ~ NA_character_
    )
  ) %>%
  distinct()

# --- Export Final Aggregated Data ---
# Save ingredient-level summary to a parquet file for downstream use
long_mol %>% write_parquet('parquet/long_mol.parquet')
