#title: 3.Query RXCUI
#author: "Nicholas Cardamone"
#date: "5/9/2024"

#Goal: Use 2006-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination (‘molecule’) of orally administered drugs in each year after NME approva

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

combined_data <- open_dataset("parquet/combined_data.parquet")

all_ndcs  <- combined_data %>% select(PRODUCTNDC, rxcui) %>% distinct() %>% collect()
all_rxcuis  <- combined_data %>% select(rxcui) %>% distinct() %>% collect()

# No RXCUIs prior to 2010?
na_rxcui_by_date <- combined_data %>% distinct(date, PRODUCTNDC, rxcui) %>% 
  group_by(date) %>%
  summarize(n = n(), na_rxcui_count = sum(is.na(rxcui) | rxcui == "")) %>% collect()


getNdcProps <- function(ndc) {
  tryCatch({
    cat("Processing NDC:", ndc, "\n")
    Sys.sleep(0.1)
    
    url <- paste0('https://rxnav.nlm.nih.gov/REST/ndcproperties?id=', ndc)
    ndc_properties <- fromJSON(url, flatten = TRUE)
    property_list <- ndc_properties$ndcPropertyList$ndcProperty$propertyConcept
    
    if (!is.null(property_list)) {
      property_df <- bind_rows(lapply(property_list, as.data.frame)) %>%
        mutate(id = ndc, appl_no = propValue, message = NA, source = "NDC") %>%
        select(id, appl_no, message, source)
      
      return(property_df)
    } else {
      return(data.frame(id = ndc, appl_no = NA, message = "No NDC propertyConcept found", source = "NDC"))
    }
  }, error = function(e) {
    cat("Error querying NDC:", ndc, "-", e$message, "\n")
    return(data.frame(id = ndc, appl_no = NA, message = e$message, source = "NDC"))
  })
}

getRxcuiProps <- function(rxcui) {
  tryCatch({
    cat("Processing RXCUI:", rxcui, "\n")
    Sys.sleep(0.1)
    
    url <- paste0('https://rxnav.nlm.nih.gov/REST/rxcui/', rxcui, '/allProperties.json?prop=all')
    rxcui_props <- fromJSON(url, flatten = TRUE)
    prop_concepts <- rxcui_props$propConceptGroup$propConcept
    
    if (!is.null(prop_concepts)) {
      rxcui_df <- as.data.frame(prop_concepts) %>%
        mutate(id = rxcui, appl_no = propValue, message = NA, source = "RXCUI") %>%
        select(id, appl_no, message, source)
      
      return(rxcui_df)
    } else {
      return(data.frame(id = rxcui, appl_no = NA, message = "No RXCUI propConcept found", source = "RXCUI"))
    }
  }, error = function(e) {
    cat("Error querying RXCUI:", rxcui, "-", e$message, "\n")
    return(data.frame(id = rxcui, appl_no = NA, message = e$message, source = "RXCUI"))
  })
}

chunk_size <- 500

# --- NDCs ---
ndc_chunks <- split(all_ndcs$PRODUCTNDC, ceiling(seq_along(all_ndcs$PRODUCTNDC) / chunk_size))
ndc_results_all <- list()

for (i in seq_along(ndc_chunks)) {
  cat("Processing NDC chunk", i, "of", length(ndc_chunks), "\n")
  ndc_chunk_result <- pblapply(ndc_chunks[[i]], getNdcProps)
  ndc_results_all[[i]] <- bind_rows(ndc_chunk_result)
  saveRDS(ndc_results_all[[i]], file = paste0("ndc_results_chunk_", i, ".rds"))
}

ndc_results_final <- bind_rows(ndc_results_all)
saveRDS(ndc_results_final, "ndc_results_all.rds")


# --- RXCUIs ---
rxcui_chunks <- split(all_rxcui$rxcui, ceiling(seq_along(all_rxcui$rxcui) / chunk_size))
rxcui_results_all <- list()

for (i in seq_along(rxcui_chunks)) {
  cat("Processing RXCUI chunk", i, "of", length(rxcui_chunks), "\n")
  rxcui_chunk_result <- pblapply(rxcui_chunks[[i]], getRxcuiProps)
  rxcui_results_all[[i]] <- bind_rows(rxcui_chunk_result)
  saveRDS(rxcui_results_all[[i]], file = paste0("rxcui_results_chunk_", i, ".rds"))
}

rxcui_results_final <- bind_rows(rxcui_results_all)
saveRDS(rxcui_results_final, "rxcui_results_all.rds")


## Step 2. Process queried data
# Read in data

setwd("P://ORD_Schwartz_202412057D//nick//GENCO//GENCO//")

# Load in molecule sample data (which was previously extracted from Drugs@FDA and cleaned in STATA by R. Gupta)
long_mol <- read.csv("long_mol10-30-2024.csv")


# NDC
ndc_results_final <- readRDS("ndc_results_all.rds")

ndc_results_final %>% select(id) %>% distinct() %>% nrow()
# We queried 19,918 NDCs for an application number.

ndc_results_final %>% filter(grepl("^(ANDA|NDA)[0-9]", appl_no)) %>%
  filter(appl_no != "NDA" & appl_no != "ANDA"& appl_no != "NDA AUTHORIZED GENERIC") %>% select(id) %>% 
  distinct() %>% nrow()
# Of which, 11,476 NDCs returned an ANDA or NDA.

ndc_sample = ndc_results_final %>% filter(grepl("^(ANDA|NDA)[0-9]", appl_no)) %>%  
  filter(appl_no != "NDA" & appl_no != "ANDA"& appl_no != "NDA AUTHORIZED GENERIC") %>% 
  left_join(long_mol, by = "appl_no") %>% 
  filter(!is.na(ingredient)) %>% 
  select(id, appl_no) %>% 
  distinct()
ndc_sample %>% select(id) %>% nrow()
ndc_sample_list = ndc_sample %>% select(id) %>% distinct() %>% pull(id)

# Of which, 990 NDCs returned an ANDA or NDA that was in our sample.

ndc_results_final %>% 
  filter(grepl("^(BLA)[0-9]", appl_no)) %>% 
  select(id) %>% distinct() %>% nrow()
# Of which, 580 NDCs returned BLAs

ndc_results_final %>% filter(!is.na(message)) %>% 
  select(id, appl_no) %>% saveRDS("ndc_results_final.rds")

ndc_not_found_list = ndc_results_final %>% 
  filter(!is.na(message)) %>% 
  select(id) %>% 
  distinct() %>% 
  pull(id) 
# Of which, 7,766 NDCs returned no application number



# RXCUI
rxcui_results_final <- readRDS("rxcui_results_all.rds")

rxcui_results_final %>% select(id) %>% distinct() %>% nrow()
# We queried 11,370 NDCs for an application number.

rxcui_results_final %>% filter(grepl("^(ANDA|NDA)[0-9]", appl_no)) %>%
  filter(appl_no != "NDA" & appl_no != "ANDA"& appl_no != "NDA AUTHORIZED GENERIC") %>% select(id) %>% 
  distinct() %>% nrow()
# Of which, 6,628 NDCs returned an ANDA or NDA.

rxcui_sample = rxcui_results_final %>% filter(grepl("^(ANDA|NDA)[0-9]", appl_no)) %>%  
  filter(appl_no != "NDA" & appl_no != "ANDA"& appl_no != "NDA AUTHORIZED GENERIC") %>% 
  left_join(long_mol, by = "appl_no") %>% 
  filter(!is.na(ingredient)) %>% 
  select(id, appl_no) %>% 
  distinct()
rxcui_sample %>% select(id) %>% nrow()
# Of which, 888 NDCs returned an ANDA or NDA that was in our sample.

rxcui_results_final %>% 
  filter(grepl("^(BLA)[0-9]", appl_no)) %>% 
  select(id) %>% distinct() %>% nrow()
# Of which, 552 RXCUIs returned BLAs

rxcui_results_final %>% filter(message == "No RXCUI propConcept found") %>% select(id, appl_no)  %>% saveRDS("rxcui_results_final.rds")
rxcui_not_found_list = rxcui_results_final %>% filter(message == "No RXCUI propConcept found") %>% select(id) %>% distinct() %>% pull(id)
# Of which, 3,410 NDCs returned no application number

ndc_sample <- ndc_sample %>% 
  mutate(appl_no = as.character(appl_no),
         PRODUCTNDC = as.character(id)) %>% 
  select(appl_no, PRODUCTNDC)

rxcui_sample <- rxcui_sample %>% 
  mutate(rxcui = as.character(id)) %>% 
  select(appl_no, rxcui)

# Set Data.Table 
setDT(long_mol)
setDT(ndc_sample)
setDT(rxcui_sample)


appl_no_xw <- all_ndcs %>% left_join(ndc_sample, "PRODUCTNDC")
appl_no_xw <- appl_no_xw %>% left_join(rxcui_sample, "rxcui")
appl_no_xw <- appl_no_xw %>% select(rxcui, PRODUCTNDC, appl_no.x, appl_no.y)
setDT(appl_no_xw)
appl_no_xw[, checkAppl_no := fcase(
  !is.na(appl_no.x) & !is.na(appl_no.y) & appl_no.x == appl_no.y, "Appl_Nos Match",
  is.na(appl_no.x) & !is.na(appl_no.y),                        "Appl_No found via RXCUI",
  !is.na(appl_no.x) & is.na(appl_no.y),                        "Appl_No found via NDC",
  !is.na(appl_no.x) & !is.na(appl_no.y) & appl_no.x != appl_no.y, "Appl_no do not Match",
  is.na(appl_no.x) & is.na(appl_no.y),                         "No appl_no found via RXCUI or NDC search"
)]

table(appl_no_xw$checkAppl_no)
no_match = appl_no_xw %>% filter(checkAppl_no == "Appl_no do not Match")

# Join ingredient data on appl_no returned by NDC list
long_mol_test <- long_mol %>% select(ingredient, appl_no) 
long_mol_test[, appl_no.x := as.character(appl_no)]
long_mol_test <- long_mol_test[, .(ingredient, appl_no.x)]

setDT(long_mol_test)
appl_no_xw <- appl_no_xw %>% left_join(long_mol_test, "appl_no.x")

# Join ingredient data on appl_no returned by RXCUI list
long_mol_test[, appl_no.y := as.character(appl_no.x)]
long_mol_test <- long_mol_test[, .(ingredient, appl_no.y)]
ing_xw <- appl_no_xw %>% left_join(long_mol_test, "appl_no.y")
setDT(ing_xw)
ing_xw[, checkIngredient := fcase(
  !is.na(ingredient.x) & !is.na(ingredient.y) & ingredient.x == ingredient.y, "Included: Ingredients match",
  is.na(ingredient.x) & !is.na(ingredient.y),                               "Included: Ingredient found via RXCUI",
  !is.na(ingredient.x) & is.na(ingredient.y),                               "Included: Ingredient found via NDC",
  !is.na(ingredient.x) & !is.na(ingredient.y) & ingredient.x != ingredient.y, "Included: Ingredients do not Match",
  is.na(ingredient.x) & is.na(ingredient.y),                                "Not included: ingredient not in list"
)]

table(ing_xw$checkIngredient) # Confirms that "Included: Ingredients do not Match" = 0 

# Sample of ingredients list
ingredients_list <- ing_xw %>% 
  mutate(ingredient = coalesce(ingredient.x, ingredient.y)) %>% # take first non-NA ingredient name (most rows have both because both the NDC and rxcui source had it)
  select(ingredient) %>% 
  mutate(in_sample = 1) %>% 
  distinct()

ingredients_list %>% nrow()
# 332 out of #474 in our sample.

### RxMix Xwalk:

ndc_nda_xw = read_delim("2ndc_nda_xw.txt") %>% filter(ndc != "ndc11_str")
# Lots of rich data for each NDC

ndc_ndc = ndc_nda_xw %>% select(F1_ndc11) %>% distinct()

ndc_mc = ndc_nda_xw %>% 
  filter(F2_propName == "MARKETING_CATEGORY") %>% 
  mutate(conceptName = F1_conceptName,
         marketing_cat = F2_propValue) %>% 
  select(F1_ndc11, conceptName, marketing_cat) %>% distinct()

table(ndc_mc$marketing_cat)

ndc_appl = ndc_nda_xw %>% 
  filter(F2_propName %in% c("ANDA", "NDA", "NDA_AUTHORIZED_GENERIC")) %>% 
  mutate(
         appl_no = F2_propValue) %>% 
  select(F1_ndc11, appl_no) %>% distinct()

ndc_nda_xw_clean = ndc_ndc %>% 
  left_join(ndc_mc, by= "F1_ndc11") %>% 
  left_join(ndc_appl, by= "F1_ndc11") %>% 
  mutate(ndc11_str = F1_ndc11,
         appl_str = ifelse(grepl("^NDA", appl_no), "NDA", 
                              ifelse(grepl("^ANDA", appl_no), "ANDA", NA)),
         marketing_str = ifelse(grepl("^NDA", marketing_cat), "NDA", 
                                ifelse(grepl("^ANDA", marketing_cat), "ANDA", NA)),
         match = if_else(marketing_str == appl_str, 1, 0))  %>% 
  filter(is.na(match) | match == 1) %>% select(-F1_ndc11, -match, -marketing_str, -appl_str)

# Check for duplicate NDCs
ndc_nda_xw_clean_count = ndc_nda_xw_clean %>% group_by(ndc11_str) %>% summarize(n=n())
# Preview marketing categories for NDCs
table(ndc_nda_xw_clean$marketing_cat)

# Just take distinct combos fo NDA/ANDA and ingredient name
long_mol_distinct = long_mol %>% select(appl_no, ingredient, approval_date1) %>% distinct()

# Left join ingredient data onto list of ndcs, preserve ndcs
ndc_nda_ingredient_xw = ndc_nda_xw_clean %>% left_join(long_mol_distinct, by = "appl_no") 

# Left join ndc-nda-ingredient dataset onto full formulary dataset:
combined_data1 <- combined_data %>% 
  select(drugname, date, formulary_id, ndc11_str, rxcui, prior_authorization_yn) %>% 
  collect() %>% 
  left_join(ndc_nda_ingredient_xw, by = "ndc11_str") %>% 
  drop_na(ingredient) %>% 
  filter(conceptName != "buspirone hydrochloride 7.5 MG Oral Tablet [Vanspar]")

# The VANSPAR flub is the only case where the NDC for a drug appears after the FDA approval date of the drug it's associated with.
check = combined_data1 %>% 
  drop_na(ingredient) %>% 
  select(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>%
  distinct() %>% 
  group_by(drugname, conceptName, date, ndc11_str, appl_no, approval_date1) %>% 
  dplyr::summarise(approval_after_appearance = if_else(approval_date1 > date, 1, 0)) %>%  #approval date must be after date
  filter(approval_after_appearance != 1)

combined_data1 %>% write_parquet('parquet/combined_data1.parquet')

long_mol %>% select(ingredient) %>% distinct() %>% nrow()
combined_data1 %>% select(ingredient) %>% distinct() %>% nrow()






###

xw_full <- ing_xw %>% 
  filter(checkIngredient %in% c("Included: Ingredients match", "Included: Ingredient found via RXCUI", "Included: Ingredient found via NDC")) %>% 
  mutate(ingredient = coalesce(ingredient.x, ingredient.y)) %>% 
  select(rxcui, PRODUCTNDC, ingredient)

xw_ndc <- xw_full %>% select(ingredient, PRODUCTNDC) %>% distinct() %>% drop_na()

xw_rxcui = xw_full %>% select(ingredient, rxcui) %>% distinct() %>% drop_na()

# Pivot out but keep rxcui - ndc connection.
test <- combined_data %>% 
  select(drugname, date, formulary_id, ndc11, rxcui, prior_authorization_yn) %>% 
  collect() %>% 
  left_join(xw_ndc, by = "PRODUCTNDC")

combined_data1 <- test %>% 
  mutate(ingredient = coalesce(ingredient.x, ingredient.y)) %>% 
  select(ingredient, date, formulary_id, PRODUCTNDC, rxcui, prior_authorization_yn) %>% 
  drop_na(ingredient) %>% 
  distinct()

combined_data1 %>% write_parquet('parquet/combined_data1.parquet')

long_mol %>% select(ingredient) %>% distinct() %>% nrow()
combined_data1 %>% select(ingredient) %>% distinct() %>% nrow()





