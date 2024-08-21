[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare BCDA Connector

## 🔗  Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the project and how you can use it.
<br/><br/>

## 🧰  What does this repo do?
The Medicare BCDA Connector is a dbt package that maps Medicare BCDA claims data to the Tuva [claims data model](https://thetuvaproject.com/claims-data/data-model/about) which then makes it simple to run the entire [Tuva Project](https://github.com/tuva-health/the_tuva_project).
<br/><br/>  

## 🔌 Database Support
- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅  Quickstart Guide

### Step 1: Import package
Import the package by adding it to your `packages.yml`.  Click [this link](https://github.com/tuva-health/bcda_demo/blob/main/packages.yml) to see an example.
<br/><br/> 

### Step 2: Set vars
Add two vars to your [project.yml](https://github.com/tuva-health/bcda_demo/blob/main/dbt_project.yml):
- bcda_coverage_file_prefix:  your-string-here
  - This var contains a string that matches the text before the date in your coverage file.
- claims_enabled: true
  - This allows you to run [The Tuva Project](https://github.com/tuva-health/tuva). 
<br/><br/> 

### Step 3: Configure Data Source
[Add models to your project](https://github.com/tuva-health/bcda_demo/tree/main/models) to select all the data from these tables:
- coverage
- coverage_extension
- explanationofbenefit
- explanationofbenefit_benefitbalance_0_financial
- explanationofbenefit_careteam
- explanationofbenefit_diagnosis
- explanationofbenefit_extension
- explanationofbenefit_item_0_adjudication
- explanationofbenefit_item_0_extension
- explanationofbenefit_procedure
- explanationofbenefit_supportinginfo
- patient
- patient_identifier
<br/><br/> 


## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
