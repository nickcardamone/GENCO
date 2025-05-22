#title: 4.Aggregate ingredient data
#author: Nicholas Cardamone
#date: "5/16/2024"

#Goal: Use 2007-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination (‘molecule’) of orally administered drugs in each year after NME approva

# What this code does:

# Load necessary packages
library(rdrop2) # connect to dropbox
library(httr)
library(stringr)
library(lubridate)
library(haven)
library(tidyverse)
library(xfun)
library(data.table)
library(arrow)
library(jsonlite)
library(dplyr)
library(pbapply)

# Negate function
'%!in%' <- function(x,y)!('%in%'(x,y))

#folder_path <- "P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//part1//formulary_data//"

long_mol <- read.csv("long_mol10-30-2024.csv")

long_mol_generic <- long_mol %>% 
  filter(grepl("^ANDA", appl_no)) %>% 
  group_by(ingredient) %>% 
  summarize(generic = 1,
   generic_approvals = n(),
   first_generic_approval_date = as.Date(min(approval_date1, na.rm = T)))

# Maximum combination total number ... I assume this was calculated off of the original NDA and wasn't counted for the ANDA?
long_mol <- long_mol %>% 
  filter(grepl("^NDA", appl_no)) %>%
  group_by(ingredient) %>% 
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

long_mol %>% write_parquet('parquet/long_mol.parquet')


