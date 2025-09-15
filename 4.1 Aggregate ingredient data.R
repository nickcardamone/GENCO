#Title: 4.Aggregate ingredient data
#Author: Nicholas Cardamone
#Date: "5/16/2025"

# What this code does: cleans Drugs@FDA data and aggregates it to the ingredient level 

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

# Negate function
'%!in%' <- function(x,y)!('%in%'(x,y))

setwd("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//")

long_mol <- read.csv("input/long_mol10-30-2024.csv")
excl_ing_rg <- read.csv("input/excl_ing5-30-2025_rg.csv")

reform <- long_mol %>% 
  mutate(reform = if_else(doc_type1 %in% c("12", "13", "15"), 1, 0)) %>% 
  arrange(ingredient, approval_date1) %>% 
  group_by(ingredient, approval_date1) %>% 
  mutate(reform = cumsum(reform)) %>% 
  transmute(ingredient, date = approval_date1, reform)

reform %>% write_parquet('parquet/reform.parquet')

# Create dataset with generic version information.
long_mol_generic <- long_mol %>% 
  filter(grepl("^ANDA", appl_no)) %>% 
  group_by(ingredient) %>% 
  summarize(generic = 1,
   generic_approvals = n(),
   first_generic_approval_date = as.Date(min(approval_date1, na.rm = T))) %>% anti_join(excl_ing_rg, by = "ingredient")

# Maximum combination total number ... I assume this was calculated off of the original NDA and wasn't counted for the ANDA?
long_mol <- long_mol %>% 
  filter(grepl("^NDA", appl_no)) %>%
  group_by(ingredient) %>% 
  arrange(ingredient, approval_date1) %>% 
  summarize(
    first_approval_date = as.Date(min(approval_date1, na.rm = T)),
    branded_approvals = n(),
    combination_totalnumber = max(combination_totalnumber, na.rm = T) 
  ) %>% 
  left_join(long_mol_generic, by= "ingredient")

# Add period of branded / generic approval 
long_mol <- long_mol %>% 
  mutate(
    period_branded_approval = if_else(month(first_approval_date) %in% 1:6,
                                      as.Date(paste0(year(first_approval_date), "-06-30")),
                                      if_else(month(first_approval_date) %in% 7:12, as.Date(paste0(year(first_approval_date), "-12-31")), NA)), # last 6 months of the year.

    period_generic_approval = if_else(is.na(first_generic_approval_date), NA,
                                      if_else(month(first_generic_approval_date) %in% 1:6, 
                                              paste0(year(first_generic_approval_date), "-06-30"),
                                              if_else(month(first_generic_approval_date) %in% 7:12, 
                                                      paste0(year(first_generic_approval_date), "-12-31"), NA))) # last 6 months of the year.
                                                                                   
  ) %>%  distinct()

# Save to parquet file
long_mol %>% write_parquet('parquet/long_mol.parquet')


