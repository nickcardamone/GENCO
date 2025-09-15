# Title: 6.Join datasets and analyze
# Version: 6. Final version
# Author: Nicholas Cardamone
# Created date: 5/16/2025
# Last updated: 09/04/2025

# 

#8. Create final model data, including a “conversion date” variable – the period at which NDCs associated with a generic application number appear in 10% of the formularies this basket of generic NDCs is ever going to appear in.


# Remove scientific notation
options(scipen =999)

# Load necessary packages
library(stringr) # work with string variables
library(lubridate) # work with dates
library(haven) # read files of various formats
library(tidyverse) # data manipulation
library(xfun) #misc functions
library(data.table) # working/manipulating large datasets
library(arrow) # storing big data
library(jsonlite) # reading json files
library(dplyr) #data manipulation
library(readr) # reading in different data types
library(tools) # utility 
library(devtools) # utility
library(did) #difference-in-difference - Calloway & Sant'Anna

# Load prior auth/formulary data
# Pad zeroes on formulary id column to ensure that the formatting is the same as the one in enrollment data.
combined_data1 <- open_dataset('parquet/combined_data1.parquet') %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>%
  filter(date < '2024-12-31' & date > '2008-06-30') # limit dataset to periods with enrollment data.

# Load enrollment data (plan_id, date, enrollment, total_enrollment)
enrollment_period <- open_dataset('parquet/enrollment_period.parquet') %>% collect()
# Load plan data (plan_id, date, formulary_id)
combined_plan_data <- open_dataset('parquet/combined_plan_data.parquet') 

# Count number of plans per period in plan-formulary xw data table. This is our "ground truth" for which plans appear in which periods.
plans <- combined_plan_data %>% 
  select(plan_id, date) %>%
  distinct() %>% 
  group_by(date) %>% 
  summarize(plans = n()) %>% 
  collect()

# Generate features for ingredient appearance in prior auth data.
appearance <- combined_data1 %>% 
  group_by(ingredient) %>% 
  summarise(
    first_appearance_date = min(date),
    last_appearance_date = max(date)) %>% # take first appearance of an NDC associated with that ANDA
  collect()

# Unique NDCs per ingredient-period:
ndc_date <- combined_data1 %>% select(ingredient, date, formulary_id, ndc11_str, marketing_cat) %>% 
  left_join(combined_plan_data, by = c("formulary_id", "date")) %>% 
  select(ingredient, date, plan_id, ndc11_str, marketing_cat) %>% 
  distinct() %>% 
  group_by(ingredient, date) %>% 
  summarise(total_ndcs = n(),
            anda_ndcs = sum(marketing_cat == "ANDA"),
            nda_ndcs = sum(marketing_cat == "NDA"),
            nda_authorized_generic_ndcs = sum(marketing_cat == "NDA AUTHORIZED GENERIC")) %>% 
  collect()

## Join Enrollment Data to Part D (NDC) data set:
## FYI - we don't have enrollment for 2007 Q2/Q4 & 2008 Q2:
## Start with plan:
plan_outcomes = combined_data1 %>%
  left_join(combined_plan_data, by = c("formulary_id", "date")) %>% 
# ensure there are no NAs in the combined plan data then join to prior auth data by formulary id and date.
  group_by(ingredient, plan_id, ndc11_str, date) %>% 
  summarise(
            min_pa = min(prior_authorization_yn, na.rm = T) # if there are identical NDCs in two formularies and one has PA == 1 and another is PA == 0, make it PA == 0.
            ) %>% 
  group_by(ingredient, plan_id, date) %>% 
  summarise(ndcs = n(), # For each ingredient, count number of NDCs
            ndcs_pa_free = sum(min_pa == 0, na.rm = TRUE),  # Count number of NDCs where there's prior auth free access PA ==0.
            ndcs_pa = sum(min_pa == 1, na.rm = TRUE), # Count number of NDCs where there's only prior autth access.
            prop_ndc_pa_free = ndcs_pa_free/ndcs,  # Proportion of NDCs, out of all unique NDCs, with prior auth free access.
            prop_ndc_pa = ndcs_pa/ndcs
            ) %>% 
  collect() %>% 
  ungroup()

# Extract list of all ingredients 
plan_outcomes_ing <- plan_outcomes %>% 
  select(ingredient) %>% 
  distinct() %>% 
  pull(ingredient)

# Cross to assign all unique combinations of plan_id and date for each ingredient. Then, we can add the plan number variable by
plans_date_ing <- combined_plan_data %>% 
  select(plan_id, date) %>%
  distinct() %>% 
  collect() %>%
  crossing(ingredient = plan_outcomes_ing) %>%   
  left_join(plans, by = "date")


plan_outcomes = plans_date_ing %>%
  left_join(plan_outcomes, by = c("date", "plan_id", "ingredient")) 



# Pare down to relevant variables and join to expanded plan enrollment data grid for a given date.
plan_outcomes_enrollment <- plan_outcomes %>% 
  left_join(enrollment_period, by = c("date", "plan_id")) %>% 
  mutate(any_pa_free = if_else(ndcs_pa_free > 0, 1, 0), # if there's any where PA == 0, then 1.
         pa_only = if_else(ndcs_pa_free == 0, 1, 0), # if there's none where PA == 0, then 1
         any_pa_free_enrollees = if_else(any_pa_free == 1, enrollment, 0),
         pa_only_enrollees = if_else(pa_only == 1, enrollment, 0)
  ) 

## Add 999 to rows of an ingredient for plans that they're not covered by.
plan_outcomes_enrollment <- plan_outcomes_enrollment %>% 
  dplyr::mutate(across(ndcs:pa_only_enrollees, ~replace_na(., 999)))

writexl::write_xlsx(plan_outcomes_enrollment %>% head(100000), "output/plan_outcomes_enrollment.xlsx")

# Load ingredient data
long_mol <- open_dataset('parquet/long_mol.parquet')  

#write.csv(generic_appearance, "generic_appearance.csv")
#write.csv(ing_list, "approved_generics.csv")

# Create variables for generic version appearance.
# 10% (or 50%) of the formularies that will ever cover the drug, cover at least one NDC from any ANDA.

generic_appearance <- combined_data1 %>% 
  filter(marketing_cat == "ANDA") %>% # Take generic drug approval records
  collect() %>% 
  dplyr::group_by(ingredient, marketing_cat) %>% 
  arrange(ingredient, marketing_cat, date) %>% 
  dplyr::summarise(
    unique_formularies_g = n_distinct(formulary_id), #count distinct formulates
    percentile_10_index_g = ceiling(0.10 * unique_formularies_g), # 10% of total number of formularies associated with an ANDA.
    percentile_50_index_g = ceiling(0.50 * unique_formularies_g), # 50% of total number of formularies associated with an ANDA.
    first_appearance_g = min(date), # take first appearance of an NDC associated with that ANDA
    date_10_percent_g = dplyr::nth(date, percentile_10_index_g), # Period in which at least 10% of the formularies that will ever be connected with the ANDA appear in the data. 
    date_50_percent_g = dplyr::nth(date, percentile_50_index_g) # Period in which at least 50% of the formularies that will ever be connected with the ANDA appear in the data. 
    ) 

# Create a variable to indicate date at which there are at least 2 unique ANDAs covered by 10% of formularies.

generic_applno <- combined_data1 %>% 
  filter(marketing_cat == "ANDA") %>% # Take generic drug approval records
  collect() %>% 
  dplyr::group_by(ingredient, appl_no) %>% 
  arrange(ingredient, appl_no, date) %>% 
  dplyr::summarise(
    unique_formularies_g = n_distinct(formulary_id), #count distinct formulates
    percentile_10_index_g_2 = ceiling(0.10 * unique_formularies_g), # 10% of total number of formularies associated with an ANDA.
    date_10_percent_g_2 = dplyr::nth(date, percentile_10_index_g_2), # Period in which at least 10% of the formularies that will ever be connected with the ANDA appear in the data. 
  ) %>% arrange(ingredient, date_10_percent_g_2) %>% 
  group_by(ingredient) %>%
  slice(2) %>%
  select(ingredient, date_10_percent_g_2) %>%
  ungroup()

generic_appearance <- generic_appearance %>% 
  left_join(generic_applno, by = "ingredient")
  


writexl::write_xlsx(generic_appearance, "output/generic_appearance.xlsx")



# Keep:
## Conversion date
## Date_10_percent_g
## Date_50_percent_g
## Add generic appearance data to long molecule
## Add number of NDCS in data for that drug in the time period.

ingredient_info = long_mol %>%
  collect() %>% 
  left_join(generic_appearance, by= "ingredient") %>% 
  left_join(appearance, by= "ingredient") %>% 
  mutate(generic = if_else(is.na(generic), 0, generic)) %>% 
  select(ingredient,  generic, 
         combination_totalnumber, 
         first_approval_date, 
         first_appearance_date,
         first_generic_approval_date, 
         branded_approvals, 
         generic_approvals, 
         date_10_percent_g,
         date_50_percent_g) 

ingredient_info_generic = ingredient_info %>% 
  filter(generic == 1)

writexl::write_xlsx(ingredient_info_generic, "output/ingredient_info_generic.xlsx")


# Ingredient level outcomes (ingredient-date)
ingredient_outcomes <- plan_outcomes_enrollment %>% 
  dplyr::group_by(ingredient, date) %>% 
  dplyr::summarise(
            plans = max(plans, na.rm = T),
            plans_pa_free = sum(any_pa_free == 1), # formularies where at least one NDC is pa free
            plans_pa_only = sum(pa_only == 1),
            covering_plans = sum(pa_only != 999),
            uncovered_plans = sum(pa_only == 999),
            prop_covering_plans_pa_free = plans_pa_free/covering_plans,
            prop_all_plans_pa_free = plans_pa_free/plans,
            any_pa_free_enrollment = sum(any_pa_free_enrollees[any_pa_free_enrollees != 999]),
            pa_only_enrollment = sum(pa_only_enrollees[pa_only_enrollees != 999]),
            total_enrollment = max(total_enrollment)
) %>% 
  ungroup() %>% 
  mutate(
         uncovered_enrollment = total_enrollment - (any_pa_free_enrollment + pa_only_enrollment),
         prop_apf = any_pa_free_enrollment/total_enrollment,
         prop_pao = pa_only_enrollment/total_enrollment,
         prop_uncov = uncovered_enrollment/total_enrollment) %>% 
  left_join(ingredient_info, by= "ingredient") %>% 
  left_join(ndc_date, by= c("ingredient", "date")) %>% 
  dplyr::filter(date >= first_appearance_date)


# Add reformulations dataset:
reform <- open_dataset('parquet/reform.parquet') %>% collect()

ingredient_outcomes <- ingredient_outcomes %>% 
  left_join(reform%>% mutate(date = as.Date(date)), by =c("ingredient", "date")) %>% 
  group_by(ingredient) %>%
  arrange(ingredient, date) %>% 
  tidyr::fill(reform, .direction = "down") %>% 
  ungroup()

ingredient_outcomes <- ingredient_outcomes %>% 
  mutate(reform = replace_na(reform, 0)) %>% arrange(ingredient, date) 

## Visuals:
    
ingredient_outcomes_long <- ingredient_outcomes %>% 
  select(date, plans_pa_free, plans_pa_only, uncovered_plans) %>% 
  tidyr::pivot_longer(cols = c(plans_pa_free, plans_pa_only, uncovered_plans), names_to = "type", values_to = "count")

ggplot(ingredient_outcomes_long, aes(x=date, y = count, fill=type)) +
  geom_col() + theme_minimal() + labs(title = "Plans by coverage and prior authorization\nrequirements over time", y = "Number of plans", fill = "Type", x = "Date") +
  scale_fill_manual(values = c('plans_pa_free' = "#1b9e77", "plans_pa_only" = "#d95f02", "uncovered_plans" = "grey"))

ingredient_outcomes_long <- ingredient_outcomes %>% 
  select(ingredient, date, prop_apf, prop_pao, prop_uncov) %>% 
  tidyr::pivot_longer(cols = c(prop_apf, prop_pao, prop_uncov), names_to = "type", values_to = "count")

ggplot(ingredient_outcomes %>% filter(ingredient %in% generic_appearance$ingredient), 
       aes(x=date, y = reorder(ingredient, date_10_percent_g), fill = prop_apf)) +
  geom_tile(alpha = 0.85, aes(color = if_else(date == date_10_percent_g, "white", "black")
               ),
            show.legend = T) +
  theme_minimal() + 
  labs(title = "Enrollment proportion by coverage and\nprior authorization requirements over time", 
       y = "Ingredient", fill = "Proportion Prior-Auth Free", x = "Date") + 
  scale_x_date() + 
  scale_fill_gradient(low = "white", high = "red") +
  scale_color_identity()

#ggsave("any_pa_free_enrollment.png", plot1, height = 12, width = 8)

# Create Period and Conversion Period Varaible
model_dat <- ingredient_outcomes %>%
  mutate(generic = if_else(is.na(date_10_percent_g), 0, 1),
         date = as.Date(date),                  # Ensure date is in Date format
         conversion_date = as.Date(date_10_percent_g)) %>%   # Ensure conversion_date is in Date format
  filter(date>="2008-12-31")

# Define the start period
start_period <- as.Date("2008-12-31")

# Calculate periods every six months
model_dat <- model_dat %>%
  mutate(period = as.integer(as.numeric(difftime(date, start_period, units = "days")) / 183) + 1,  # Six-month intervals
    conversion_period = ifelse(!is.na(conversion_date), as.integer((as.numeric(difftime(conversion_date, start_period, units = "days")) / 183)) + 1,
      NA_integer_
    )
  ) %>% arrange(ingredient, period)

model_dat <- model_dat %>% ungroup() %>% 
  mutate(
    period = as.integer((year(date)-year(start_period))*2 + (month(date) > 6)),
    ingredient_id = as.integer(factor(ingredient)))  # Assign numeric IDs
  
model_dat <- model_dat %>% 
  group_by(ingredient) %>% 
  mutate(conversion_period = as.numeric(if_else(is.na(conversion_period), 0, conversion_period))) %>% 
  ungroup()

#model_dat_test <- model_dat %>% 
#  mutate(generic = if_else(is.na(generic), 0, generic)) %>% 
#           filter(ingredient %in% c("DIMETHYL FUMARATE", "NILOTINIB HYDROCHLORIDE"))

write.csv(model_dat, "output/model_dat_ingredient-9-04-2025.csv")


#model_dat <- model_dat %>% 
#  mutate()
  
model_dat %>% 
  filter(generic == 1) %>% 
  ggplot(aes(x=as.Date(date), y= reorder(ingredient, date_10_percent_g), alpha = prop_apf)) + 
  geom_point(aes(x = as.Date(conversion_date), y= reorder(ingredient, date_10_percent_g)), color = "red3") +
  geom_tile(fill = "blue1") + 
  labs(y="Ingredient", x= "Date") + scale_x_date() +theme_minimal()

# Separate trends of converted, not yet converted, and never converted
plot_cohorts <- function(c_date) {

viz_dat <- model_dat %>% 
    mutate(Cohort = if_else(is.na(date_10_percent_g), "Never converted", 
                            if_else(date_10_percent_g > c_date, "Not yet converted", paste0("Converted in ", c_date)))
           ) %>%
    filter(Cohort == "Not converted" |  # never treated
             date_10_percent_g > c_date | # not yet treated
             date_10_percent_g == c_date)
  
viz_dat = viz_dat %>% 
  group_by(Cohort, date) %>%
  summarize(mean_prop_apf = mean(prop_apf),
            se_prop_apf = sd(prop_apf) / sqrt(n())
            ) %>% 
  ungroup() 

viz_dat %>% 
  ggplot(aes(x=as.Date(date), 
             y= mean_prop_apf, 
             color = Cohort)) + 
  geom_point(size = 2) +
  #geom_point(aes(color = if_else(c_date == date, "darkgreen", NA))) +
  geom_line() +
  geom_vline(aes(xintercept = as.Date(c_date)), linetype = "dashed") +
  geom_errorbar(aes(ymin = mean_prop_apf - se_prop_apf, 
                    ymax = mean_prop_apf + se_prop_apf), width = 0.2, alpha = 0.8) +
  #geom_smooth(se = F, method = "lm") + 
  labs(y="Prop monthly enrollees prior auth free", x= "Date") + 
  scale_x_date() + 
  theme_minimal()

}

# Compare non-treated to 

plot_cohorts("2017-12-31")
plot_cohorts("2018-06-30")
plot_cohorts("2019-06-30")
plot_cohorts("2019-12-31")
plot_cohorts("2020-06-30")
plot_cohorts("2020-12-31")
plot_cohorts("2021-06-30")
plot_cohorts("2021-12-31")
plot_cohorts("2022-06-30")
plot_cohorts("2022-12-31")
plot_cohorts("2023-06-30")
plot_cohorts("2024-06-30")



# Separate trends of converted, not yet converted, and never converted
plot_cohorts_combined <- function(c_date) {
  
  viz_dat <- model_dat %>% 
    mutate(Cohort = if_else(is.na(date_10_percent_g) | date_10_percent_g > c_date, 
                                                               "Never converted/Not yet converted", paste0("Converted in ", c_date))
    ) %>%
    filter(Cohort == "Not converted" |  # never treated
             date_10_percent_g > c_date | # not yet treated
             date_10_percent_g == c_date)
  
  viz_dat = viz_dat %>% 
    group_by(Cohort, date) %>%
    summarize(mean_prop_apf = mean(prop_apf),
              se_prop_apf = sd(prop_apf) / sqrt(n())
    ) %>% 
    ungroup() 
  
  viz_dat %>% 
    ggplot(aes(x=as.Date(date), 
               y= mean_prop_apf, 
               color = Cohort)) + 
    geom_point(size = 2) +
    #geom_point(aes(color = if_else(c_date == date, "darkgreen", NA))) +
    geom_line() +
    geom_vline(aes(xintercept = as.Date(c_date)), linetype = "dashed") +
    geom_errorbar(aes(ymin = mean_prop_apf - se_prop_apf, 
                      ymax = mean_prop_apf + se_prop_apf), width = 0.2, alpha = 0.8) +
    #geom_smooth(se = F, method = "lm") + 
    labs(y="Prop monthly enrollees prior auth free", x= "Date") + 
    scale_x_date() + 
    theme_minimal()
  
}

# Compare non-treated to 

plot_cohorts_combined("2017-12-31")
plot_cohorts_combined("2018-06-30")
plot_cohorts_combined("2019-06-30")
plot_cohorts_combined("2019-12-31")
plot_cohorts_combined("2020-06-30")
plot_cohorts_combined("2020-12-31")
plot_cohorts_combined("2021-06-30")
plot_cohorts_combined("2021-12-31")
plot_cohorts_combined("2022-06-30")
plot_cohorts_combined("2022-12-31")
plot_cohorts_combined("2023-06-30")
#plot_cohorts_combined("2024-12-31")


all_comparison_dat <- function(c_date) {
  
  viz_dat <- model_dat %>% 
    mutate(Cohort = if_else(is.na(date_10_percent_g) | 
                              date_10_percent_g > c_date, 
                            paste0("Not converted as of ", c_date), 
                            paste0("Converted in ", c_date))
    ) %>%
    filter(is.na(date_10_percent_g)|  # never treated
             date_10_percent_g > c_date | # not yet treated
             date_10_percent_g == c_date)
  
  viz_dat = viz_dat %>% 
    group_by(Cohort, date) %>%
    summarize(n = n(), 
              mean_prop_apf = mean(prop_apf),
              se_prop_apf = sd(prop_apf) / sqrt(n())
    ) %>% 
    ungroup() %>% mutate(conversion_date = c_date)
}

d1 <- all_comparison_dat("2017-12-31")
d2 <- all_comparison_dat("2018-06-30")
d3 <- all_comparison_dat("2019-06-30")
d4 <- all_comparison_dat("2019-12-31")
d5 <- all_comparison_dat("2020-06-30")
d6 <- all_comparison_dat("2020-12-31")
d7 <- all_comparison_dat("2021-06-30")
d8 <- all_comparison_dat("2021-12-31")
d9 <- all_comparison_dat("2022-06-30")
d10 <- all_comparison_dat("2022-12-31")
d11 <- all_comparison_dat("2023-06-30")

d_full <- rbind.data.frame(d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11) %>%
  mutate(Type = if_else(str_detect(Cohort, "^Converted in"), "Converted", "Not converted"),
         ) %>%
  mutate(period = as.integer((year(date)-year(start_period))*2 + (month(date) > 6)),
         conversion_period = ifelse(!is.na(conversion_date), as.integer((as.numeric(difftime(conversion_date, start_period, units = "days")) / 183)) + 1,
                                    NA_integer_
         ), 
         diffperiod = period - conversion_period
  )

library(plotly)

d_full %>% 
  ggplot(aes(x=diffperiod, 
                               y= mean_prop_apf, 
                               group = Type,
                               color = Type)) + 
  #geom_point(aes(x=diffperiod, 
  #               y= mean_prop_apf,
  #               group = Cohort,
  #               color = Type, size = n), alpha = 0.5) +
  geom_line(aes(x=diffperiod, 
                y= mean_prop_apf, 
                group = Cohort,
                color = Type), alpha = 0.5) +
  geom_vline(aes(xintercept = 0), linetype = "dashed") +
  #geom_errorbar(aes(ymin = mean_prop_apf - se_prop_apf, 
  #                  ymax = mean_prop_apf + se_prop_apf), width = 0.2, alpha = 0.8) +
  geom_smooth(se = F, method = "lm") + 
  #geom_smooth() +
  labs(y="Prop monthly enrollees prior auth free", x= "Period") + 
  theme_minimal()


didmodel <- att_gt(yname = "prop_all_plans_pa_free", #outcome
                        tname = "period", #time
                        idname = "ingredient_id", #molecule 
                        gname = "conversion_period", 
                        data = model_dat,
                        #xformla = ~reform,
                        control_group="notyettreated", #includes not-yet converted molecules in control group.
                        allow_unbalanced_panel = TRUE, #don't balance panel with respect to time and id
                        base_period= "varying" # varying base results in an estimate of ATT(g, t) being reported in the period immediately before treatment. 
)


ggdid(didmodel, title = "Proportion of plans with prior auth free access")


es <- aggte(didmodel, type = "dynamic", na.rm = T)
ggdid(es, title = "Proportion of plans with prior auth free access")




didmodel_enrollment <- att_gt(yname = "prop_apf", #outcome
                   tname = "period", #time
                   idname = "ingredient_id", #molecule 
                   gname = "conversion_period", 
                   data = model_dat,
                   #xformla = ~reform,
                   control_group="notyettreated", #includes not-yet converted molecules in control group.
                   allow_unbalanced_panel = TRUE, #don't balance panel with respect to time and id
                   base_period= "varying" # varying base results in an estimate of ATT(g, t) being reported in the period immediately before treatment. 
)


ggdid(didmodel, title = "Proportion of plans with prior auth free access")


es <- aggte(didmodel_enrollment, type = "dynamic", na.rm = T)
ggdid(es, title = "Proportion of avg. monthly enrollees covered by a plan with prior auth free access to drug")



## Staggered:


library(staggered)

df <- staggered::pj_officer_level_balanced

eventPlotResults <- staggered(df = model_dat,
                                i = "uid",
                                t = "period",
                                g = "conversion_period",
                                y = "complaints",
                                estimand = "cohort")




eventPlotResults$ymin_ptwise

ggplot(eventPlotResults, aes(x = eventTime, y = estimate)) _
geom_pointrange(aes(ymin = ymin_ptwise, ymax = ymax_ptwise))

















