#title: 3.Query RXCUI
#author: "Nicholas Cardamone"
#date: "5/9/2025"
#last updated: 5/30/2025

#Goal: Use 2006-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans 
# to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination
# of orally administered drugs in each year after NME approval.

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

## Method 1: Create NDC-NDA XW via RxNorm API query using RxMix.

combined_data <- open_dataset("parquet/combined_data.parquet")
long_mol <- read.csv("input/long_mol10-30-2024.csv")

### RxMix Xwalk:

ndc_nda_xw = read_delim("xw_ndc_nda.txt") %>% 
  filter(ndc != "ndc11_str")
# Lots of rich data for each NDC

# List of queried NDCs.
ndc_ndc = ndc_nda_xw %>% 
  select(F1_ndc11) %>% 
  distinct()

# Extract marketing category (incl. NDA, ANDA, BLA, NDA Authorized Generics, etc.)
ndc_mc = ndc_nda_xw %>% 
  filter(F2_propName == "MARKETING_CATEGORY") %>% 
  mutate(conceptName = F1_conceptName,
         marketing_cat = F2_propValue) %>% 
  select(F1_ndc11, conceptName, marketing_cat) %>% 
  distinct()

# Inspect marketing categories.
table(ndc_mc$marketing_cat)

# Extract list of application numbers associated with marketing categories we want to include.
ndc_appl = ndc_nda_xw %>% 
  filter(F2_propName %in% c("ANDA", "NDA", "NDA_AUTHORIZED_GENERIC")) %>% 
  mutate(
         appl_no = F2_propValue) %>% 
  select(F1_ndc11, appl_no) %>% 
  distinct()

# Left join non-NA marketing categories to queried NDC list, left join application numbers from relevant marketing categories to same list.
ndc_nda_xw_clean = ndc_ndc %>% 
  left_join(ndc_mc, by= "F1_ndc11") %>% 
  left_join(ndc_appl, by= "F1_ndc11") %>% 
# We have a few instances (n=18) where the query associated the NDC with an NDA and an ANDA in our sample. Not important because we're going to aggregate at the ingredient level, but we want to only retain matches.
  mutate(ndc11_str = F1_ndc11,
         appl_str = ifelse(grepl("^NDA", appl_no), "NDA", 
                              ifelse(grepl("^ANDA", appl_no), "ANDA", NA)),
         marketing_str = ifelse(grepl("^NDA", marketing_cat), "NDA", 
                                ifelse(grepl("^ANDA", marketing_cat), "ANDA", NA)),
         match = if_else(marketing_str == appl_str, 1, 0))  %>% 
  filter(is.na(match) | match == 1) %>% 
  select(-F1_ndc11, -match, -marketing_str, -appl_str)

# Check for duplicate NDCs
ndc_nda_xw_clean_count = ndc_nda_xw_clean %>% 
  group_by(ndc11_str) %>% 
  summarize(n=n()) # should all be 1
# Preview marketing categories for NDCs
table(ndc_nda_xw_clean$marketing_cat)


# Just take distinct combos fo NDA/ANDA and ingredient name
long_mol_distinct = long_mol %>% 
  select(appl_no, ingredient, approval_date1) %>% 
  distinct()

# Left join ingredient data onto list of ndcs, preserve ndcs
ndc_nda_ingredient_xw = ndc_nda_xw_clean %>% 
  left_join(long_mol_distinct, by = "appl_no") 

# Left join ndc-nda-ingredient dataset onto full formulary dataset:
combined_data1 <- combined_data %>% 
  select(drugname, date, formulary_id, ndc11_str, rxcui, prior_authorization_yn, tier_level_value, step_therapy_yn, quantity_limit_yn, quantity_limit_amount, quantity_limit_days) %>% 
  collect() %>% 
  left_join(ndc_nda_ingredient_xw, by = "ndc11_str") %>% 
  drop_na(ingredient)

# Check that FDA approval is before the appearance in the formulary data.
check = combined_data1 %>% 
  drop_na(ingredient) %>% 
  select(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>%
  distinct() %>% 
  group_by(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>% 
  dplyr::summarise(approval_after_appearance = if_else(approval_date1 > date, 1, 0)) %>%  #approval date must be after date
  filter(approval_after_appearance != 1)

# The VANSPAR flub is the only case where the NDC for a drug before the FDA approval date of the drug it's associated with.
combined_data1 <- combined_data1 %>% 
  filter(conceptName != "buspirone hydrochloride 7.5 MG Oral Tablet [Vanspar]") %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0"))

combined_data1 %>% write_parquet('parquet/combined_data1.parquet')

# Count ingredients
long_mol %>% select(ingredient) %>% distinct() %>% nrow()
# 474 in sample 
combined_data1 %>% select(ingredient) %>% distinct() %>% nrow()
# 370 in Medicare Part D

# Read in list of excluded ingredients (not joined in via RxMix crosswalk)
excl_ing_rg <- read.csv("input/excl_ing5-30-2025_rg.csv")

long_mol_clean = long_mol %>% anti_join(excl_ing_rg, by = "ingredient")

long_mol_clean %>% select(ingredient) %>% distinct()

# Paste in trade name and ingredient name so that these can be manually checked.
long_mol_tn = long_mol %>% 
  group_by(ingredient) %>% 
  filter(grepl("^NDA", appl_no)) %>% 
  summarise(names = paste(unique(trade_name), collapse = "; "), .groups = "drop")

# Join and create CSV
excl_ing_rg <- excl_ing_rg %>% left_join(long_mol_tn, by = "ingredient") %>% write.csv("output/excl_ing6-03-2025_rg_nc.csv")








