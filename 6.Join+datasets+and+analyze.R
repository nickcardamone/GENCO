#title: 6.Join datasets and analyze
#author: "Nicholas Cardamone"
#date: "5/16/2024"


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
library(readr) # reading in different data types
library(tools) # utility 
library(devtools) # utility
library(did)

# Load formulary data
combined_data1 <- open_dataset('parquet/combined_data1.parquet')

# Load ingredient data
long_mol <- open_dataset('parquet/long_mol.parquet') %>% collect()

# Load enrollment data
enrollmentlj <- open_dataset('parquet/plan_enrollment.parquet')

## Create Enrollment Share Values
# Create formulary-level enrollment variable.
# Count number of distinct plans and number of plans missing enrollment data.
formulary_enrollmentshare <- enrollmentlj %>% collect %>% 
  group_by(formulary_id, date) %>% 
  dplyr::summarise(sum_enrollment_share = round(sum(avg_enrollment_weight, na.rm = T), 5),
                   plans = n_distinct(plan_id, contract_id, segment_id),
                   plans_missing_enrollment = sum(is.na(avg_enrollment_weight))) %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>% distinct()

## Join Enrollment Data to Part D (NDC) dataset}
formulary_outcomes <- combined_data1 %>% 
  mutate(formulary_id = str_pad(formulary_id, 8, pad = "0")) %>% 
  collect() %>% 
  group_by(ingredient, formulary_id, date) %>% 
  summarise(min_pa = min(prior_authorization_yn, na.rm = T), # if at least 
            ndc = n(),
            ndc_pa_free = sum(prior_authorization_yn == 0, na.rm = TRUE), 
            ndc_pa = sum(prior_authorization_yn == 1, na.rm = TRUE),
            prop_ndc_pa_free = ndc_pa_free/ndc, 
            prop_ndc_pa = ndc_pa/ndc
            ) %>% left_join(formulary_enrollmentshare, by = c("formulary_id", "date")) %>% 
        mutate(
              any_pa_free = if_else(ndc_pa_free > 0, 1, 0), 
              pa_only = if_else(ndc_pa_free == 0, 1, 0),
              any_pa_free_enrollment = if_else(any_pa_free == 1, sum_enrollment_share, NA),
              pa_only_enrollment = if_else(pa_only == 1, sum_enrollment_share, NA)
            ) 

# Check
zero_enrollment = formulary_enrollmentshare %>% filter(date == "2015-12-31")
check = combined_data1 %>% filter(formulary_id == "00016000") %>% collect()

generic_appearance <- combined_data1 %>% filter(marketing_cat == "ANDA") %>% collect() %>% 
  group_by(ingredient, marketing_cat) %>% 
  arrange(date) %>% 
  summarise(
    unique_formularies_g = n_distinct(formulary_id),
    first_appearance_g = min(date),
    percentile_10_index_g = ceiling(0.10 * unique_formularies_g),
    percentile_50_index_g = ceiling(0.50 * unique_formularies_g),
    
    date_10_percent_g = nth(date, percentile_10_index_g),
    
    date_50_percent_g = nth(date, percentile_50_index_g),
    
    periods_to_10_percent_g = as.integer(difftime(date_10_percent_g, first_appearance_g, units = "days") / 183),
    periods_to_50_percent_g = as.integer(difftime(date_50_percent_g, first_appearance_g, units = "days") / 183),
    diff = periods_to_50_percent_g - periods_to_10_percent_g) #%>% select(-marketing_cat)

# Add generic appearance data to long molecule 
long_mol = long_mol %>% left_join(generic_appearance, by= "ingredient")

# Ingredient level outcomes (ingredient-date)
ingredient_outcomes <- formulary_outcomes %>% 
  group_by(ingredient, date) %>% 
  summarise(formularies_pa_free = sum(any_pa_free == 1, na.rm = TRUE), # formularies where at least one NDC is pa free
            formularies_pa_only = sum(pa_only == 1, na.rm = TRUE),
            formularies = n(),
            prop_formularies_pa_free = formularies_pa_free/formularies,
            prop_formularies_pa_only = formularies_pa_only/formularies,
            sum_formulary_enrollmentshare = sum(sum_enrollment_share, na.rm = T),
            any_pa_free_enrollmentshare = sum(any_pa_free_enrollment, na.rm = T),
            pa_only_enrollmentshare = sum(pa_only_enrollment, na.rm = T)
  ) %>% left_join(long_mol, by= "ingredient")

ingredient_outcomes_chk = ingredient_outcomes %>% group_by(date) %>% 
  summarize(any_pa_free_enrollmentshare = mean(any_pa_free_enrollmentshare, na.rm = T))

ingredient_outcomes %>% 
  filter(generic == 1) %>% 
  ggplot(aes(x=as.Date(date), y= ingredient, alpha = any_pa_free_enrollmentshare)) + 
  geom_tile(fill = "blue1") + 
  labs(y="Ingredient", x= "Date") + scale_x_date() +theme_minimal()


# Create Period and Conversion Period Varaible}
model_dat <- ingredient_outcomes %>%
  mutate(date = as.Date(date),                  # Ensure date is in Date format
         conversion_date = as.Date(date_10_percent_g))  # Ensure conversion_date is in Date format

# Define the start period
start_period <- as.Date("2006-12-31")

# Calculate periods every six months
model_dat <- model_dat %>%
  mutate(period = as.integer(as.numeric(difftime(date, start_period, units = "days")) / 183) + 1,  # Six-month intervals
    conversion_period = ifelse(!is.na(conversion_date), as.integer((as.numeric(difftime(conversion_date, start_period, units = "days")) / 183)) + 1,
      NA_integer_
    )
  ) %>% arrange(ingredient, period)

model_dat <- model_dat %>% 
  mutate(
    period = as.integer((year(date)-year(start_period))*2 + (month(date) > 6)),
    ingredient_id = as.integer(factor(ingredient)))  # Assign numeric IDs
    
  ) %>% group_by(ingredient) %>% 
  mutate(conversion_period = period[date == conversion_date][1]) %>%
  ungroup()
  

didmodel <- att_gt(yname = "prop_formularies_pa_free", #outcome
                        tname = "period", #time
                        idname = "ingredient_id", #molecule 
                        gname = "conversion_period", 
                        data = model_dat,
                        xformla=~sum_formulary_enrollmentshare,
                        control_group="notyettreated", #includes not-yet converted molecules in control group.
                        allow_unbalanced_panel = TRUE, #don't balance panel with respect to time and id
                        base_period= "varying" # varying base results in an estimate of ATT(g, t) being reported in the period immediately before treatment. 
)


es <- aggte(didmodel, type = "dynamic", na.rm = T)
#group_effects <- aggte(example_attgt, type = "group", na.rm = T)

summary(didmodel) # The P-value for pre-test of parallel trends assumption is for a Wald pre-test of the parallel trends assumption. Here the parallel trends assumption would not be rejected at conventional significance levels.
summary(es)

ggdid(es)










## MOdels below

data <- model_dat %>% select(mol_id, period, conversion_period, prop_average_monthly_enrollees_paf) %>% na.omit()
Dtl <- sapply(-(max(data$period)-1):(max(data$period)-2), function(l) {
  dtl <- 1*( (data$period == data$conversion_period + l) & (data$conversion_period > 0) )
  dtl
})

Dtl <- as.data.frame(Dtl)
cnames1 <- paste0("Dtmin", (max(data$period)-1):1)
colnames(Dtl) <- c(cnames1, paste0("Dt", 0:(max(data$period)-2)))

data <- cbind.data.frame(data, Dtl)
row.names(data) <- NULL

head(data)
```
```{r}
# load plm package
library(plm)

# run event study regression
# normalize effect to be 0 in pre-treatment period
es <- plm(prop_average_monthly_enrollees_paf ~ Dtmin3 + Dtmin2 + Dt0 + Dt1 + Dt2, 
          data = data, model = "within", effect = "twoways",
          index = c("mol_id", "period"))

summary(es)

coefs1 <- coef(es)
ses1 <- sqrt(diag(summary(es)$vcov))
idx.pre <- 1:(max(data$period)-2)
idx.post <- (max(data$period)-1):length(coefs1)
coefs <- c(coefs1[idx.pre], 0, coefs1[idx.post])
ses <- c(ses1[idx.pre], 0, ses1[idx.post])
exposure <- -(max(data$period)-1):(max(data$period)-2)

cmat <- data.frame(coefs=coefs, ses=ses, exposure=exposure)

```
```{r}
library(ggplot2)

ggplot(data = cmat, mapping = aes(y = coefs, x = exposure)) +
  geom_line(linetype = "dashed") +
  geom_point() + 
  geom_errorbar(aes(ymin = (coefs-1.96*ses), ymax = (coefs+1.96*ses)), width = 0.2) +
  ylim(c(-2, 5)) +
  theme_bw()

```

```{r}
es <- aggte(example_attgt, type = "dynamic", na.rm = T)
#group_effects <- aggte(example_attgt, type = "group", na.rm = T)

summary(example_attgt) # The P-value for pre-test of parallel trends assumption is for a Wald pre-test of the parallel trends assumption. Here the parallel trends assumption would not be rejected at conventional significance levels.
summary(es)
```






writexl::write_xlsx(appearance, "C://Users//Nick//Desktop//VA//GENCO-Misc//Appearance-3-12-2025.xlsx")
#write.csv(ndc_level_fj, "C://Users//Nick//Desktop//VA//GENCO-Misc//Full Dataset-3-12-2025.csv")
#writexl::write_xlsx(ndc_level_fj_agg, "C://Users//Nick//Desktop//VA//GENCO-Misc//Formulary_Proportion-3-12-2025.xlsx")


#Join back to master dataset

ndc_level_fin <- left_join(ndc_level_f, appearance, "ingredient")


#Trim down final dataset
ndc_level_fin <- ndc_level_fin %>% select(ingredient, appl_no, appl_type, ndc11, date, date_10_percent_g, formulary_id, prior_authorization_yn, sum_enrollment_share)

```
```{r, Create two new variables - pre and post conversion}
ndc_level_fin <- ndc_level_fin %>%
  mutate(
    pre_conversion = date_10_percent_g %m-% years(1), # Date one year before
    post_conversion = date_10_percent_g %m+% years(1)  # Date one year after
  )
colnames(ndc_level_fin)[colnames(ndc_level_fin) == "date_10_percent_g"] <- "conversion_date"

```

```{r}

library(dplyr)
#library(hablar)

# Collapse dataset to ingredient level
ingredient_data <- ndc_level_fin %>%
  group_by(ingredient, ndc11) %>%
  summarise(
    conversion_date = min(s(conversion_date), na.rm = TRUE),
    pre_conversion = min(s(pre_conversion), na.rm = TRUE),
    post_conversion = min(s(post_conversion), na.rm = TRUE),
    earliest = min(date, na.rm = TRUE),
    mid = median(date, na.rm = TRUE),
    most_recent_date = max(date, na.rm = TRUE)) %>% 
  mutate(conversion_date = if_else(is.infinite(conversion_date), NA, conversion_date),
         pre_conversion = if_else(is.infinite(pre_conversion), NA, pre_conversion),
         post_conversion = if_else(is.infinite(post_conversion), NA, post_conversion),
         converted = ifelse(any(!is.na(conversion_date)), 1, 0))

```{r}
writexl::write_xlsx(ingredient_data, "C://Users//Nick//Desktop//VA//GENCO-Misc//all_ndcs-2-3-2025.xlsx" )
```

```{r}
test <- ndc_level_fin# %>% filter(!is.na(conversion_date))
#filter(ingredient == "CLOBAZAM" | ingredient == "ASENAPINE MALEATE" | ingredient == "APIXABAN" | ingredient == "MARAVIROC")

# Assign NDC based on first appearance per ingredient, taking the lowest NDC in case of ties
new_appearance <- ndc_level_fin %>% 
  group_by(ingredient, formulary_id) %>% 
  arrange(date, ndc11) %>%  # Arrange by date first, then NDC to break ties
  slice(1) %>%  # Select the first occurrence
  distinct(ingredient, date, ndc11, formulary_id) %>% group_by(ingredient, date, ndc11) %>% summarize(unique_formularies = n_distinct(formulary_id))


