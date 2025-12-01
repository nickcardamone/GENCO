# =====================================================================
# Title:    6. Join datasets and analyze
# Version:  6. Final version
# Author:   Nicholas Cardamone
# Created:  2025-05-16
# Updated:  2025-09-04
# =====================================================================

# --- SETUP --------------------------------------------------------------------

# Remove scientific notation for readability
options(scipen = 999)

# Load all required packages
library(stringr)      # String operations
library(lubridate)    # Date handling
library(haven)        # Read SPSS, Stata, SAS files
library(tidyverse)    # Data manipulation and visualization (includes dplyr, ggplot2, tidyr, etc.)
library(xfun)         # Miscellaneous utility functions
library(data.table)   # Efficient data manipulation
library(arrow)        # Read/write parquet and feather files
library(jsonlite)     # Read JSON files
library(dplyr)        # Data manipulation
library(readr)        # Read tabular data
library(tools)        # File utilities
library(devtools)     # Development tools
library(did)          # Difference-in-differences estimation (Calloway & Sant'Anna)

# --- LOAD DATA ----------------------------------------------------------------

# Load prior auth/formulary data
# Pad zeroes on formulary_id for formatting consistency with enrollment data
combined_data1 <- open_dataset('parquet/combined_data1.parquet') %>%
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>%
  filter(date < '2024-12-31' & date > '2008-06-30') # Restrict to periods with enrollment data

# Load enrollment data (plan_id, date, enrollment, total_enrollment)
enrollment_period <- open_dataset('parquet/enrollment_period.parquet') %>% collect()

# Load plan data (plan_id, date, formulary_id)
combined_plan_data <- open_dataset('parquet/combined_plan_data.parquet')

# --- SUMMARY STATISTICS -------------------------------------------------------

# Count number of plans per period (date) — this is our "ground truth"
plans <- combined_plan_data %>%
  select(plan_id, date) %>%
  distinct() %>%
  group_by(date) %>%
  summarize(plans = n()) %>%
  collect()

# For each ingredient, get first and last appearance dates in prior auth data
appearance <- combined_data1 %>%
  group_by(ingredient) %>%
  summarise(
    first_appearance_date = min(date),
    last_appearance_date  = max(date)
  ) %>%
  collect()

# --- NDCs BY INGREDIENT AND PERIOD --------------------------------------------

# Calculate unique NDCs, grouped by ingredient and period (date)
ndc_date <- combined_data1 %>%
  select(ingredient, date, formulary_id, ndc11_str, marketing_cat) %>%
  left_join(combined_plan_data, by = c("formulary_id", "date")) %>%
  select(ingredient, date, plan_id, ndc11_str, marketing_cat) %>%
  distinct() %>%
  group_by(ingredient, date) %>%
  summarise(
    total_ndcs = n(),
    anda_ndcs  = sum(marketing_cat == "ANDA"),
    nda_ndcs   = sum(marketing_cat == "NDA"),
    nda_authorized_generic_ndcs = sum(marketing_cat == "NDA AUTHORIZED GENERIC")
  ) %>%
  collect()

# --- PLAN OUTCOMES: JOIN, DERIVE, AGGREGATE -----------------------------------

# Join enrollment data with NDC/plan data for each ingredient
plan_outcomes <- combined_data1 %>%
  left_join(combined_plan_data, by = c("formulary_id", "date")) %>%
  group_by(ingredient, plan_id, ndc11_str, date) %>%
  summarise(
    min_pa = min(prior_authorization_yn, na.rm = TRUE) # If any PA==0, set min_pa=0
  ) %>%
  group_by(ingredient, plan_id, date) %>%
  summarise(
    ndcs             = n(),
    ndcs_pa_free     = sum(min_pa == 0, na.rm = TRUE),   # NDCs with prior-auth free access
    ndcs_pa          = sum(min_pa == 1, na.rm = TRUE),   # NDCs with only prior-auth
    prop_ndc_pa_free = ndcs_pa_free / ndcs,              # Proportion prior-auth free
    prop_ndc_pa      = ndcs_pa / ndcs                    # Proportion prior-auth only
  ) %>%
  collect() %>%
  ungroup()

# Extract unique list of ingredients
plan_outcomes_ing <- plan_outcomes %>%
  select(ingredient) %>%
  distinct() %>%
  pull(ingredient)

# Build full grid of plan_id, date, and ingredient combinations
plans_date_ing <- combined_plan_data %>%
  select(plan_id, date) %>%
  distinct() %>%
  collect() %>%
  crossing(ingredient = plan_outcomes_ing) %>%
  left_join(plans, by = "date")

# Join plan grid to outcomes
plan_outcomes <- plans_date_ing %>%
  left_join(plan_outcomes, by = c("date", "plan_id", "ingredient"))

# --- ENRICH WITH ENROLLMENT & DERIVED VARIABLES -------------------------------

# Add enrollment and flag variables for PA access
plan_outcomes_enrollment <- plan_outcomes %>%
  left_join(enrollment_period, by = c("date", "plan_id")) %>%
  mutate(
    any_pa_free          = if_else(ndcs_pa_free > 0, 1, 0),               # Any PA free
    pa_only              = if_else(ndcs_pa_free == 0, 1, 0),              # All PA only
    any_pa_free_enrollees = if_else(any_pa_free == 1, enrollment, 0),     # Enrollees with PA free
    pa_only_enrollees     = if_else(pa_only == 1, enrollment, 0)          # Enrollees with PA only
  )

# Fill missing outcome values with 999 to indicate not covered
plan_outcomes_enrollment <- plan_outcomes_enrollment %>%
  mutate(across(ndcs:pa_only_enrollees, ~replace_na(., 999)))

# Write a sample of the outcomes for inspection
writexl::write_xlsx(plan_outcomes_enrollment %>% head(100000), "output/plan_outcomes_enrollment.xlsx")

# --- GENERIC VERSION APPEARANCE METRICS ---------------------------------------

# Load long molecule (ingredient) data
long_mol <- open_dataset('parquet/long_mol.parquet')

# For each ingredient, compute when 10% and 50% of possible formularies cover the generic (ANDA)
generic_appearance <- combined_data1 %>%
  filter(marketing_cat == "ANDA") %>%
  collect() %>%
  group_by(ingredient, marketing_cat) %>%
  arrange(ingredient, marketing_cat, date) %>%
  summarise(
    unique_formularies_g   = n_distinct(formulary_id),
    percentile_10_index_g  = ceiling(0.10 * unique_formularies_g),
    percentile_50_index_g  = ceiling(0.50 * unique_formularies_g),
    first_appearance_g     = min(date),
    date_10_percent_g      = nth(date, percentile_10_index_g),
    date_50_percent_g      = nth(date, percentile_50_index_g)
  )

# Find date when a second unique ANDA reaches 10% coverage
generic_applno <- combined_data1 %>%
  filter(marketing_cat == "ANDA") %>%
  collect() %>%
  group_by(ingredient, appl_no) %>%
  arrange(ingredient, appl_no, date) %>%
  summarise(
    unique_formularies_g    = n_distinct(formulary_id),
    percentile_10_index_g_2 = ceiling(0.10 * unique_formularies_g),
    date_10_percent_g_2     = nth(date, percentile_10_index_g_2)
  ) %>%
  arrange(ingredient, date_10_percent_g_2) %>%
  group_by(ingredient) %>%
  slice(2) %>%
  select(ingredient, date_10_percent_g_2) %>%
  ungroup()

generic_appearance <- generic_appearance %>%
  left_join(generic_applno, by = "ingredient")

# Save generic appearance metrics
writexl::write_xlsx(generic_appearance, "output/generic_appearance.xlsx")

# --- INGREDIENT-LEVEL MASTER DATA ---------------------------------------------

ingredient_info <- long_mol %>%
  collect() %>%
  left_join(generic_appearance, by = "ingredient") %>%
  left_join(appearance, by = "ingredient") %>%
  mutate(generic = if_else(is.na(generic), 0, generic)) %>%
  select(
    ingredient, generic, combination_totalnumber, first_approval_date,
    first_appearance_date, first_generic_approval_date, branded_approvals,
    generic_approvals, date_10_percent_g, date_50_percent_g
  )

ingredient_info_generic <- ingredient_info %>%
  filter(generic == 1)

writexl::write_xlsx(ingredient_info_generic, "output/ingredient_info_generic.xlsx")

# --- INGREDIENT-LEVEL OUTCOMES: AGGREGATION -----------------------------------

ingredient_outcomes <- plan_outcomes_enrollment %>%
  group_by(ingredient, date) %>%
  summarise(
    plans                  = max(plans, na.rm = TRUE),
    plans_pa_free          = sum(any_pa_free == 1),
    plans_pa_only          = sum(pa_only == 1),
    covering_plans         = sum(pa_only != 999),
    uncovered_plans        = sum(pa_only == 999),
    prop_covering_plans_pa_free = plans_pa_free / covering_plans,
    prop_all_plans_pa_free      = plans_pa_free / plans,
    any_pa_free_enrollment      = sum(any_pa_free_enrollees[any_pa_free_enrollees != 999]),
    pa_only_enrollment          = sum(pa_only_enrollees[pa_only_enrollees != 999]),
    total_enrollment            = max(total_enrollment)
  ) %>%
  ungroup() %>%
  mutate(
    uncovered_enrollment = total_enrollment - (any_pa_free_enrollment + pa_only_enrollment),
    prop_apf = any_pa_free_enrollment / total_enrollment,
    prop_pao = pa_only_enrollment / total_enrollment,
    prop_uncov = uncovered_enrollment / total_enrollment
  ) %>%
  left_join(ingredient_info, by = "ingredient") %>%
  left_join(ndc_date, by = c("ingredient", "date")) %>%
  filter(date >= first_appearance_date)

# --- ADD REFORMULATIONS -------------------------------------------------------

reform <- open_dataset('parquet/reform.parquet') %>% collect()

ingredient_outcomes <- ingredient_outcomes %>%
  left_join(reform %>% mutate(date = as.Date(date)), by = c("ingredient", "date")) %>%
  group_by(ingredient) %>%
  arrange(ingredient, date) %>%
  tidyr::fill(reform, .direction = "down") %>%
  ungroup() %>%
  mutate(reform = replace_na(reform, 0)) %>%
  arrange(ingredient, date)

# --- VISUALIZATIONS -----------------------------------------------------------

# 1. Plans by coverage/prior-auth requirements over time
ingredient_outcomes_long <- ingredient_outcomes %>%
  select(date, plans_pa_free, plans_pa_only, uncovered_plans) %>%
  tidyr::pivot_longer(cols = c(plans_pa_free, plans_pa_only, uncovered_plans),
                      names_to = "type", values_to = "count")

ggplot(ingredient_outcomes_long, aes(x = date, y = count, fill = type)) +
  geom_col() +
  theme_minimal() +
  labs(title = "Plans by coverage and prior authorization\nrequirements over time",
       y = "Number of plans", fill = "Type", x = "Date") +
  scale_fill_manual(values = c('plans_pa_free' = "#1b9e77", "plans_pa_only" = "#d95f02", "uncovered_plans" = "grey"))

# 2. Proportion of enrollment by coverage/prior-auth status, for generic ingredients
ggplot(ingredient_outcomes %>% filter(ingredient %in% generic_appearance$ingredient),
       aes(x = date, y = reorder(ingredient, date_10_percent_g), fill = prop_apf)) +
  geom_tile(alpha = 0.85, aes(color = if_else(date == date_10_percent_g, "white", "black")), show.legend = TRUE) +
  theme_minimal() +
  labs(title = "Enrollment proportion by coverage and\nprior authorization requirements over time",
       y = "Ingredient", fill = "Proportion Prior-Auth Free", x = "Date") +
  scale_x_date() +
  scale_fill_gradient(low = "white", high = "red") +
  scale_color_identity()

# --- MODEL DATA PREPARATION ---------------------------------------------------

# Add conversion dates/periods for modeling
model_dat <- ingredient_outcomes %>%
  mutate(
    generic        = if_else(is.na(date_10_percent_g), 0, 1),
    date           = as.Date(date),
    conversion_date = as.Date(date_10_percent_g)
  ) %>%
  filter(date >= "2008-12-31")

# Define start date for period calculation
start_period <- as.Date("2008-12-31")

# Calculate period indices (six-month intervals), conversion periods, and assign unique ingredient IDs
model_dat <- model_dat %>%
  mutate(
    period = as.integer((year(date) - year(start_period)) * 2 + (month(date) > 6)),
    conversion_period = ifelse(!is.na(conversion_date),
                               as.integer((as.numeric(difftime(conversion_date, start_period, units = "days")) / 183)) + 1,
                               NA_integer_
    ),
    ingredient_id = as.integer(factor(ingredient))
  ) %>%
  group_by(ingredient) %>%
  mutate(conversion_period = as.numeric(if_else(is.na(conversion_period), 0, conversion_period))) %>%
  ungroup() %>%
  arrange(ingredient, period)

# Save model data
write.csv(model_dat, "output/model_dat_ingredient-9-04-2025.csv")

# --- MODEL VISUALIZATION ------------------------------------------------------

# Visualize trends for generic ingredients, highlighting conversion dates
model_dat %>%
  filter(generic == 1) %>%
  ggplot(aes(x = as.Date(date), y = reorder(ingredient, date_10_percent_g), alpha = prop_apf)) +
  geom_point(aes(x = as.Date(conversion_date), y = reorder(ingredient, date_10_percent_g)), color = "red3") +
  geom_tile(fill = "blue1") +
  labs(y = "Ingredient", x = "Date") +
  scale_x_date() +
  theme_minimal()

# --- COHORT COMPARISON FUNCTIONS ----------------------------------------------

# Visualize trends for converted, not yet converted, and never converted cohorts

plot_cohorts <- function(c_date) {
  viz_dat <- model_dat %>%
    mutate(Cohort = if_else(is.na(date_10_percent_g), "Never converted",
                     if_else(date_10_percent_g > c_date, "Not yet converted",
                             paste0("Converted in ", c_date)))) %>%
    filter(Cohort == "Not converted" |
           date_10_percent_g > c_date |
           date_10_percent_g == c_date)

  viz_dat <- viz_dat %>%
    group_by(Cohort, date) %>%
    summarize(
      mean_prop_apf = mean(prop_apf),
      se_prop_apf   = sd(prop_apf) / sqrt(n())
    ) %>%
    ungroup()

  ggplot(viz_dat, aes(x = as.Date(date), y = mean_prop_apf, color = Cohort)) +
    geom_point(size = 2) +
    geom_line() +
    geom_vline(aes(xintercept = as.Date(c_date)), linetype = "dashed") +
    geom_errorbar(aes(ymin = mean_prop_apf - se_prop_apf, ymax = mean_prop_apf + se_prop_apf), width = 0.2, alpha = 0.8) +
    labs(y = "Prop monthly enrollees prior auth free", x = "Date") +
    scale_x_date() +
    theme_minimal()
}


# --- COHORTS: COMBINED COMPARISON ---------------------------------------------

plot_cohorts_combined <- function(c_date) {
  viz_dat <- model_dat %>%
    mutate(Cohort = if_else(is.na(date_10_percent_g) | date_10_percent_g > c_date,
                            "Never converted/Not yet converted",
                            paste0("Converted in ", c_date))) %>%
    filter(Cohort == "Not converted" |
           date_10_percent_g > c_date |
           date_10_percent_g == c_date)

  viz_dat <- viz_dat %>%
    group_by(Cohort, date) %>%
    summarize(
      mean_prop_apf = mean(prop_apf),
      se_prop_apf   = sd(prop_apf) / sqrt(n())
    ) %>%
    ungroup()

  ggplot(viz_dat, aes(x = as.Date(date), y = mean_prop_apf, color = Cohort)) +
    geom_point(size = 2) +
    geom_line() +
    geom_vline(aes(xintercept = as.Date(c_date)), linetype = "dashed") +
    geom_errorbar(aes(ymin = mean_prop_apf - se_prop_apf, ymax = mean_prop_apf + se_prop_apf), width = 0.2, alpha = 0.8) +
    labs(y = "Prop monthly enrollees prior auth free", x = "Date") +
    scale_x_date() +
    theme_minimal()
}

# --- COLLAPSE FOR EVENT STUDY -------------------------------------------------

all_comparison_dat <- function(c_date) {
  viz_dat <- model_dat %>%
    mutate(Cohort = if_else(is.na(date_10_percent_g) | date_10_percent_g > c_date,
                            paste0("Not converted as of ", c_date),
                            paste0("Converted in ", c_date))) %>%
    filter(is.na(date_10_percent_g) |
           date_10_percent_g > c_date |
           date_10_percent_g == c_date)

  viz_dat <- viz_dat %>%
    group_by(Cohort, date) %>%
    summarize(
      n = n(),
      mean_prop_apf = mean(prop_apf),
      se_prop_apf = sd(prop_apf) / sqrt(n())
    ) %>%
    ungroup() %>%
    mutate(conversion_date = c_date)
}

# Aggregate all for event study
d1 <- all_comparison_dat("2017-12-31")

# Combine all event study datasets
d_full <- rbind.data.frame(d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11) %>%
  mutate(
    Type = if_else(str_detect(Cohort, "^Converted in"), "Converted", "Not converted"),
    period = as.integer((year(date) - year(start_period)) * 2 + (month(date) > 6)),
    conversion_period = ifelse(!is.na(conversion_date),
                               as.integer((as.numeric(difftime(conversion_date, start_period, units = "days")) / 183)) + 1,
                               NA_integer_),
    diffperiod = period - conversion_period
  )

# --- EVENT STUDY VISUALIZATION ------------------------------------------------

library(plotly)

ggplot(d_full, aes(x = diffperiod, y = mean_prop_apf, group = Type, color = Type)) +
  geom_line(aes(group = Cohort, color = Type), alpha = 0.5) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  geom_smooth(se = FALSE, method = "lm") +
  labs(y = "Prop monthly enrollees prior auth free", x = "Period") +
  theme_minimal()

# --- DIFFERENCE-IN-DIFFERENCES ANALYSIS ---------------------------------------

# Run DiD model on plan-level PA-free proportion
didmodel <- att_gt(
  yname = "prop_all_plans_pa_free",
  tname = "period",
  idname = "ingredient_id",
  gname = "conversion_period",
  data  = model_dat,
  #xformla = ~reform, # Uncomment if including reformulation as covariate
  control_group = "notyettreated",
  allow_unbalanced_panel = TRUE,
  base_period = "varying"
)

ggdid(didmodel, title = "Proportion of plans with prior auth free access")

# Dynamic event study DiD
es <- aggte(didmodel, type = "dynamic", na.rm = TRUE)
ggdid(es, title = "Proportion of plans with prior auth free access")

# Repeat with enrollment-weighted outcome
didmodel_enrollment <- att_gt(
  yname   = "prop_apf",
  tname   = "period",
  idname  = "ingredient_id",
  gname   = "conversion_period",
  data    = model_dat,
  #xformla = ~reform,
  control_group = "notyettreated",
  allow_unbalanced_panel = TRUE,
  base_period = "varying"
)

ggdid(didmodel_enrollment, title = "Proportion of avg. monthly enrollees covered by a plan with prior auth free access to drug")

es_enrollment <- aggte(didmodel_enrollment, type = "dynamic", na.rm = TRUE)
ggdid(es_enrollment, title = "Proportion of avg. monthly enrollees covered by a plan with prior auth free access to drug")

# --- STAGGERED ADOPTION EXAMPLE -----------------------------------------------

library(staggered)

df <- staggered::pj_officer_level_balanced

eventPlotResults <- staggered(
  df = model_dat,
  i  = "uid",
  t  = "period",
  g  = "conversion_period",
  y  = "complaints",
  estimand = "cohort"
)

eventPlotResults$ymin_ptwise

ggplot(eventPlotResults, aes(x = eventTime, y = estimate)) +
  geom_pointrange(aes(ymin = ymin_ptwise, ymax = ymax_ptwise))



