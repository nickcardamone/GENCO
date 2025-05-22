#title: 3.1.Missingness Test
#author: "Nicholas Cardamone"
#date: "5/16/2024"

library(dplyr)
library(writexl)
library(arrow)
combined_data <- open_dataset("parquet/combined_data.parquet")
combined_data1 <- open_dataset("parquet/combined_data1.parquet")

long_mol <- read.csv("long_mol10-30-2024.csv")

ndc_not_found_list <- readRDS("ndc_results_final.rds") %>% select(id) %>% distinct() #%>% pull(id)
rxcui_not_found_list <- readRDS("rxcui_results_final.rds") %>% select(id) %>% distinct() #%>% pull(id)

## Missingness Test ##
## For which drugs in the formulary data are we missing rows?
## Rows with NDCs & RXCUIs that both returned no appl_no or rows with RXCUI that returned no appl_no.
misschk <- combined_data %>% 
  filter((PRODUCTNDC %in% ndc_not_found_list & rxcui %in% rxcui_not_found_list) |
           (is.na(PRODUCTNDC) & rxcui %in% rxcui_not_found_list)) %>% 
  select(drugname, date, PRODUCTNDC, rxcui, formulary_id, prior_authorization_yn) %>% 
  distinct() %>% collect() 

molecules = long_mol %>% select(ingredient) %>% mutate(ingredient = as.character(ingredient)) %>% distinct() %>% pull(ingredient)
trade_names = long_mol %>% select(trade_name)%>% mutate(trade_name = as.character(trade_name)) %>% distinct() %>% pull(trade_name)
search_terms = c(molecules, trade_names)
words = data.frame(word = search_terms)

# Do fuzzy string matching to try and detect any ingredients / trade names among the drug name column in the fomrulary data.
misschk_str <- misschk %>% filter(!is.na(drugname)) %>% 
  select(drugname, date, PRODUCTNDC, rxcui, prior_authorization_yn) %>% distinct() %>% 
  #  tidytext::unnest_tokens(output = "word", input = "drugname", drop = FALSE) %>% 
  fuzzyjoin::stringdist_inner_join(words, by = c("drugname" = "word"), max_dist = 1) 

# There aren't any near mis-spellings so we can just do this:
missed = misschk %>% 
  filter(drugname %in% trade_names) %>% 
  left_join(long_mol %>% select(ingredient, trade_name) %>% distinct(), by = c("drugname" = 'trade_name'))
# Rows missed for each drug.

missedndc = missed %>% select(drugname, ingredient, PRODUCTNDC, rxcui) %>% distinct()


missedndc = missed %>% select(drugname, ingredient, PRODUCTNDC, rxcui) %>% distinct()

alik_hemi = combined_data %>% filter(grepl("aliskiren|tekturna", drugname, ignore.case = T)) %>%
  select(drugname, PRODUCTNDC, rxcui) %>% 
  distinct() %>% 
  collect()



alik_hemi_found = combined_data1 %>% filter(ingredient == "ALISKIREN HEMIFUMARATE") %>%
  select(ingredient, appl_no, PRODUCTNDC, rxcui) %>% 
  distinct() %>% 
  collect()

found_alik <- combined_data1 %>% filter(PRODUCTNDC == "66993-0142") %>% collect()


cnt0 = missed %>% group_by(ingredient, drugname) %>% summarise(rows_missing = n(),
                                                               min_date = min(date, na.rm = T),
                                                               max_date = max(date, na.rm = T))





# Missingness by period
cnt1 = missed %>% group_by(ingredient, date) %>% summarise(rows_missing = n())

# Distinct NDCs missing.
cnt2 = missed %>% distinct(ingredient, date, PRODUCTNDC) %>% group_by(ingredient, date) %>% 
  summarise(NDC = n())

# Distinct formularies with missing data:
cnt3 = missed %>% distinct(ingredient, date, formulary_id) %>% group_by(ingredient, date) %>% 
  summarise(formularies = n())

# NDCs with number of formularies they're missing in:
cnt4 = missed %>% 
  group_by(ingredient) %>% 
  mutate(ndc_no = dense_rank(PRODUCTNDC)) %>%
  group_by(ingredient, date, ndc_no) %>% 
  summarise(formularies = n()) %>% pivot_wider(names_from = "ndc_no", values_from = "formularies", names_prefix = "missed_ndc_")

# NDCs with number of formularies they're missing in vs. formularies they're found in (in full dataset).
missed_ing = missed %>% select(ingredient) %>% distinct() %>% mutate(exists = 1)

tst = combined_data1 %>% 
  inner_join(missed_ing, by= "ingredient") %>% 
  collect() %>% 
  group_by(ingredient) %>%
  mutate(ndc_no = dense_rank(PRODUCTNDC)) %>%
  group_by(ingredient, date, ndc_no) %>% 
  summarise(formularies = n()) %>% 
  pivot_wider(names_from = "ndc_no", values_from = "formularies", names_prefix = "found_ndc_")


cnt5 = tst %>% full_join(cnt4, by = c("ingredient", "date"))

# Create tables.
write_xlsx(list(Sheet1 = cnt0, 
                Sheet2 = cnt1, 
                Sheet3 = cnt2, 
                Sheet4 = cnt3,
                Sheet5 = cnt4, 
                Sheet6 = cnt5), 
           "missingness_check-5-20-2025.xlsx")

combined_data1 %>% select(formulary_id) %>% distinct() %>% collect() %>% nrow()

# Test if RXCUI and NDC crosswalks both return the same result.
# Determine how each NDC was joined (via RXCUI or NDC).



long_mol_excl <- long_mol %>% filter(ingredient %!in% ingredients_list$ingredient)







