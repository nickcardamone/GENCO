# GENCO

## Objective

Use 2007–2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of **prior authorization, quantity limits, and step therapy** for each unique brand-generic-dose-formulary combination of orally administered drugs in each year after NME approval.

---

## Prior Reading

- **Drugs excluded from Medicare Part D** ([Source](#))
    - Agents used for anorexia, weight loss or gain (even if for non-cosmetic purposes, e.g., morbid obesity)
    - Agents used to promote fertility
    - Agents for cosmetic purposes or hair growth
    - Agents for symptomatic relief of cough and colds
    - Prescription vitamins and mineral products (except prenatal vitamins and fluoride preparations)
    - Nonprescription drugs
    - Covered outpatient drugs where the manufacturer requires associated test/monitoring services to be purchased exclusively from them/designee
    - Drugs for sexual or erectile dysfunction (unless used for another FDA-approved condition)
- [National Drug Code (NDC) directory](https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory)
- [NDC Product File Definitions](https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions)
- [NDC Package File Definitions](https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-package-file-definitions)

---

## Data Sources

### A. FDA Approved Drugs
- **Accessed from:** [FDA Drug Approval Database](https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm)
- **Downloaded date:** ?
- **Accessed date:** ?
- **Years:** 2007–2024
- **Description:**
    - Unique application numbers: 1,021*
    - Unique ingredients: 370*
    - Inclusion: Novel therapeutic agents approved 2007–2024 (excludes biologics and diagnostic agents)

### B. Medicare Part D Formulary Data
- **Accessed from:** Ravi Gupta’s Dropbox (exact acquisition details unclear)
- **Accessed date:** September 2024 (May 2025 for 2024 data)
- **Years:** January 1, 2007 – end of 2024
- **Data types:** `.txt`, `.dta` (STATA)
- **Description:** Restrictions (prior authorization, step therapy, quantity limits, etc.) per formulary, per drug, per half-year (Jan–Jun, Jul–Dec)
- **Variables:**
    - `DRUGNAME`: Marketed name (available for some years only)
    - `FORMULARY_ID`: 8-digit ID (3 leading zeroes), 6,729 unique formularies (2006–2023)
    - `FORMULARY_VERSION`: Single digit (7, 8, or 9; not used)
    - `CONTRACT_YEAR`: Not used
    - `RXCUI`: RxNorm identifier
    - `NDC`: 10- or 11-digit National Drug Code (padded to 11 digits as `ndc11`)
    - `TIER_LEVEL_VALUE`: Drug plan tiers (not used)
    - `QUANTITY_LIMIT_YN`: Quantity limit (not used)
    - `QUANTITY_LIMIT_AMOUNT`: Quantity limit value
    - `QUANTITY_LIMIT_DAYS`: Quantity limit in days
    - `PRIOR_AUTHORIZATION_YN`: Prior authorization required?
    - `STEP_THERAPY_YN`: Step therapy required?
    - _Note: All NDCs have an expiration date_

### C. Medicare Part D Plan Data
- **Accessed from:** Ravi Gupta’s Dropbox
- **Accessed date:** September 2024 (May 2025 for 2024 data)
- **Years:** January 1, 2007 – end of 2024

### D. Medicare Part D Enrollment Data
- **Accessed from:** [CMS Monthly Enrollment Plan](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan)
- **Accessed date:** September 2024 (May 2025 for 2024 data)
- **Years:** January 1, 2007 – end of 2024
- **Description:** 1,113,716 unique rows
    1. Only kept plans offering Medicare Part D in a given month.
    2. Removed enrollment dates with missing plan IDs.
    3. Rows with enrollment ≤10: added random number 1–10.
    4. Aggregated to half-year intervals for joining with formulary data.

---

## Code-base Overview

### 1. Connect to Dropbox and Download Formulary Files (`Connect to Dropbox and Download Formulary Files.R`)
- Connects to Dropbox via an account token.
- Finds and downloads files starting with "basic drugs formulary".

### 2. Process Formulary Data (`Process Formulary Data.R`)
- Lists all `.dta`, `.txt`, and `.zip` files ending with "with drug names".
- Extracts dates, processes each file by date, and combines into a single data frame with:
    - `drugname`, `formulary_id`, `rxcui`, `ndc11_str`, `ndc_raw`, `PRODUCTNDC`, `prior_authorization_yn`, `tier_level_value`, `step_therapy_yn`, `quantity_limit_yn`, `quantity_limit_amount`, `quantity_limit_days`, `date`
    - _Note: File format-specific cleaning, then datasets are merged._

### 3. NDC–NDA Crosswalk and Missingness Test

#### Crosswalk from NDC to NDA/ANDA
- **Tool:** RxNav and RxMix
- **Process:**
    1. Extract all unique NDCs (in NDC11 format) from formulary data.
    2. Download NDC list as `.csv`.
    3. Use RxMix to get NDC status and properties (active/alien/obsolete; marketing category, NDA/ANDA).
    4. Load results into R.
    5. Join all NDCs (`ndc_ndc`, n=22,884) with marketing category (`ndc_mc`, n=19,635) and NDA/ANDA (`ndc_appl`, n=18,352).
    6. Result: Dataset with unique NDC, conceptName, marketing category, and application number.

### 4. Aggregate Ingredient Data
1. Create feature cumulative reformulations for ingredient over time 
2. Create dataset with generic version information
3. Add period of branded/generic approval
4. Save to parquet file

### 5. Plan Crosswalk (XW) and Enrollment

1. Download and combine all plan-by-period formulary files.
2. Webscrape CMS plan enrollment data by month.
3. Combine monthly files, summarize by half-year.
4. Aggregate plan-level enrollment by period (weight = plan’s average monthly enrollment ÷ sum of all plans’ enrollment).
5. Join enrollment data to plan-formulary crosswalk at half-year.
6. Join to formulary-level prior authorization data:
    - Organized as ingredient > application number (branded/generic) > RxCUI > NDC > formulary > period.

### 6. Join Datasets & Analyze

1. Load prior auth/formulary data; pad leading zeroes in formulary IDs to match enrollment data.
2. Load enrollment data (`plan_id`, `date`, `enrollment`, `total_enrollment`)
3. Load plan data (`plan_id`, `date`, `formulary_id`)
4. Count plans per period for ground truth.
5. Generate ingredient appearance features in prior auth data.
6. Generate table of unique NDCs per ingredient-period.
7. Join enrollment data to Part D (NDC) dataset:
    - Aggregate at plan level
    - Cross all unique plan_id/date per ingredient and add plan number variable
8. Add 999 to rows for plans not covering a given ingredient.
9. Roll up to ingredient-level outcomes (ingredient-date).
10. Create final model data, including a “conversion date” variable—period where generic NDCs reach 10% of the formularies they will ever appear in.

---

<sub>*Numbers are approximate and marked for review</sub>
