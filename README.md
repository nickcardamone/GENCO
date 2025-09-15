# GENCO
Objective: 
Use 2007-2023 Part D formulary files for stand-alone Medicare Part D and Medicare Advantage prescription drug plans to identify plans’ use of prior authorization, quantity limits, and step therapy for each unique brand-generic-dose-formulary combination of orally administered drugs in each year after NME approval

Prior reading:
•	Drugs that are excluded from Medicare Part D (Source)
    o	Agents when used for anorexia, weight loss or gain (even if used for non-cosmetic purposes, for example: morbid obesity). 
    o	Agents when used to promote fertility. 
    o	Agents when used for cosmetic purposes or hair growth. 
    o	Agents when used for symptomatic relief of cough and colds . 
    o	Prescription vitamins and mineral products, except prenatal vitamins and fluoride preparations. 
    o	Nonprescription drugs. 
    o	Covered outpatient drugs where the manufacturer seeks to require as a condition of sale that associated test or monitoring services be purchased exclusively from the manufacturer or its designee. 
    o	Drugs when used for the treatment of sexual or erectile dysfunction, unless such drugs were used to treat a condition other than sexual or erectile dysfunction for which the drugs have been approved by the Food and Drug Administration
•	National Drug Code (NDC) directory: https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory
•	NDC Product File Definitions: https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions
•	NDC Package File Definitions: https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-package-file-definitions

Data Sources:
  A.	FDA approved drugs
    a.	Accessed from: https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm
    b.	Downloaded date: ?
    c.	Accessed date: ?
    d.	Years: 2007-2024
    e.	Description:
        1.	Unique application numbers: 1,021*
        2.	Unique ingredients: 370*
        3.	Our inclusion criteria were all novel therapeutic agents approved from 2007 to 2024. We excluded biologics and diagnostic agents. 
  B.	Medicare Part D formulary data:
    a.	Accessed from: Ravi Gupta’s Dropbox. Unclear how/when the data was acquired previously. 
    b.	Accessed date: September 2024 (May 2025 for 2024 data).
    c.	Years: January 1st, 2007 through end of 2024.
    d.	Data types: .txt and .dta (STATA) files.
    e.	Description: Restrictions (prior authorization, step therapy, quantity limits, etc.) per formulary, per drug in a given half-year (data aggregated January 1st – June 30th & July 1st – December 31st). 
    f.	Documentation
    g.	Variables:
          o	DRUGNAME: The marketed name of the drug. It’s only available for certain half-years (2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014). 
          o	FORMULARY_ID: 8-digit id number (3 leading zeroes), 6729 unique formularies in our data July 1st, 2006 to December 31st, 2023. About formulary plans.
          o	FORMULARY_VERSION: Single digit number, 7, 8, or 9. Unclear what it means. Probably not important (not used).
          o	CONTRACT_YEAR: (not used).
          o	RXCUI: 
          o	NDC: National Drug Code, a unique 10- or 11-digit identifier used in the U.S. to identify drugs for human use. It is divided into three segments: labeler code, product code, and package code. We padded the front with zeroes to construct ndc11 and query that on RxMix (RxNav) to acquire NDA/ANDAs associated with it. About NDCs. CMS requires the NDC identifier to be formatted as 11 digits with no spaces, hyphens or other characters.
  All NDC have an expiration date
    o	TIER_LEVEL_VALUE: Drug plan tiers. Tier 1 is lowest copayment, tier 3 is highest copayment (not used). About tiers.
    o	QUANTITY_LIMIT_YN: Is there a quantity limit for this drug? (not used)
    o	QUANTITY_LIMIT_AMOUNT: If there’s a quantity limit on the amount, what is it. 
    o	QUANTITY_LIMIT_DAYS: If there’s a quantity limit on the days filled, what is it.
    o	PRIOR_AUTHORIZATION_YN: Is there a prior authorization requirement for this drug?  
    o	STEP_THERAPY_YN: Is there a step therapy requirement for this drug?
C.	Medicare Part D plan data 
    a.	Accessed from: Ravi Gupta’s Dropbox. Unclear how/when the data was acquired previously.
    b.	Accessed date: September 2024 (May 2025 for 2024 data).
    c.	Years: January 1st, 2007 through end of 2024.
D.	Medicare Part D enrollment data
    a.	Accessed from: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan
    b.	Accessed date: September 2024 (May 2025 for 2024 data).
    c.	Years: January 1st, 2007 through end of 2024.
    d.	Description: Unique rows: 1,113,716.
          1.	Kept only plans that offered Medicare Part D in a given month. 
          2.	Removed all enrollment dates with missing plan ids.
          3.	Data rows with enrollment values of 10 or less have been removed from the file. We added a random number between one and 10 to these rows.
          4.	Aggregated 12 months of data into first-half/second-half of year to join with formulary data.

  E.	Code-base:
      a.	Connect to Dropbox and Download Formulary Files.R
        1.	Connects to online Dropbox database via unique account token.
        2.	Finds all formulary files **only** downloads those files (they all start with the phrase "basic drugs formulary").
  b.	Process Formulary Data.R
        1.	List all .dta, .txt, and .zip files in the folder that end with "with drug names"
        2.	Process the list of drug files and extract dates
        3.	Create function to process each file based on the extracted date
  4.	Apply the function to all files and row bind the results to one data frame with the following:
        1.	drugname, (string of drug name text from formulary file, only present in some formulary files)
        2.	formulary_id, 
        3.	rxcui, (drug identifier)
        4.	ndc11_str,
        5.	ndc_raw,
        6.	PRODUCTNDC, (processed NDC identifier, used for crosswalk to NDA/ANDA).
        7.	prior_authorization_yn, (does this NDC have a prior authorization requirement for this formulary?)
        8.	tier_level_value,
        9.	step_therapy_yn,
        10.	quantity_limit_yn,
        11.	quantity_limit_amount,
        12.	quantity_limit_days,
        13.	date (6 month period)
            Note: The files have different formats (.dta vs. .txt vs. .zip) so the code will clean them in different ways depending on what format they're in. Then it will smash the datasets together.
c.	NDC-NDA XW and Missingness Test:
      1.	Crosswalk from NDC to NDA/ANDA:
      i.	Tool: RxNav and RxMix
      ii.	Note: “By default, only active NDCs' properties are returned. This restriction can be adjusted with the ndcstatus parameter (see getNDCStatus for a description of NDC status values). However, when an RxCUI is given as the id, only active NDCs are available.” (Source)
2.	Steps:
      1.	Combine all prior authorization datasets: Extract all the unique NDCs from the formulary data in NDC11 format (i.e. string pad zeroes in front of the ID to get it to 11 digits, that’s why CMS uses). Download this list as a .csv.
      2.	Go to RxMix. Apply the function getNDCStatus. Then on the F1_ndc11, get NDCProperties, (and check off “active”, “alien”, or “obsolete” and click propertyConceptList). This will return the properties (including the marketing category) for the NDC, even if it is obsolete. Download the .txt file.
    3.	Upload the .txt file to VINCI, load it into the R environment.
    4.	Joining in R:
    a.	Create a list of all unique NDCs queries (named it ndc_ndc, n=22,884).
    b.	Create a list of all ndcs where the query returned a non-NA value for marketing category (named it ndc_mc, n=19,635)
    c.	Create a list of all ndcs and the associated application number where the query returned an NDA or ANDA (named it ndc_appl, n=18,352).
    d.	With ndc_ndc as the base, left joined ndc_mc, then left joined ndc_appl. This returns a dataset where one row is one unique ndc with a conceptName, marketing category, and application number.
    d.	Aggregate ingredient data
      1.	Create feature cumulative reformulations for ingredient over time 
      2.	Create dataset with generic version information
    3.	Add period of branded/generic approval
    4.	Save to parquet file.
      e.	Plan XW and Enrollment
      1.	Download formulary by plan by period files from Dropbox - save to a directory folder called "plan".
      2.	#2. Combine all formulary by plan by period files into one dataframe.
      3.	Webscrape plan enrollment by month data from CMS website.
      4.	Combine all monthly plan enrollment files into one dataframe, summarize by January - June / July - December.
      5.	Extract and monthly enrollment data 2007-2003 and aggregate to 6-month periods (plan level) by taking the average monthly enrollment proportion (among all plans) for a plan by period (i.e. in period X, Plan A has 100 people but there are 400 enrollees across all plans, then Plan A receives a weight of 0.25). 
      6.	Join enrollment data to plan-formulary crosswalk dataset at 6-month period. Now that enrollment data is now organized at the formulary level with the sum enrollment weight.
      7.	Join formulary-level enrollment data to formulary-level prior authorization data. The data is now organized as ingredient > application number (branded/generic) > RxCUI > NDC > formulary > period.
  f.	Join datasets and analyze
    1.	Load prior auth/formulary data and pad zeroes on formulary id column to ensure that the formatting is the same as the one in enrollment data.
    2.	Load enrollment data (plan_id, date, enrollment, total_enrollment)
    3.	Load plan data (plan_id, date, formulary_id)
    4.	Count number of plans per period in plan-formulary xw data table. This is our "ground truth" for which plans appear in which periods.
    5.	Generate features for ingredient appearance in prior auth data.
    6.	Generate table  unique NDCs per ingredient-period:
    7.	Join Enrollment Data to Part D (NDC) data set:
    1.	Aggregate at plan level
    2.	Cross to assign all unique combinations of plan_id and date for each ingredient. Then, we can add the plan number variable by
    8.	Add 999 to rows of an ingredient for plans that they're not covered by.
    9.	Roll up to ingredient level outcomes (ingredient-date)
    10.	Create final model data, including a “conversion date” variable – the period at which NDCs associated with a generic application number appear in 10% of the formularies this basket of generic NDCs is ever going to appear in.

