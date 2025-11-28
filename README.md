# Spend Categorization
This is an example of how to use machine learning and generative AI to do spend categorization and then serve the models on Databricks.

## Dataset
This dataset (`us_tm_mfg_transactions.csv`) contains US federal Time and Materials (T&M) contract actions up to 1,000,000 USD across selected industries from FY 2015-2024, sourced from USAspending.gov. It is intended for spend analysis, vendor profiling, and categorization experiments in capital- and asset-intensive sectors.

Each row represents a contract action filtered to:
- Time Period: FY 2015-2024
- Award Amount: ≤ 1,000,000 USD total obligations
- Type of Contract Pricing: Time and Materials

NAICS sectors:
- 21 - Mining, Quarrying, and Oil and Gas Extraction
- 22 - Utilities
- 23 - Construction
- 31, 32, 33 - Manufacturing (all three 2-digit manufacturing groupings).

Core fields typically include award/transaction identifiers, action date, recipient (vendor) name, award description, obligated amount, contract pricing type, NAICS code, and agency/department metadata.

To reproduce: go to `USAspending.gov` → “Advanced Search” → “Awards”. Set filters:
- Time Period: FY 2015-2024 (select FY 2015 through FY 2024).
- Award Amounts: “$1,000,000.00 and below”.
- Award Type: “Contracts”.
- Type of Contract Pricing: “Time and Materials”.
- NAICS: select codes with first two digits 21, 22, 23, 31, 32, 33.

Run the search, then use “Download” → “Transactions”. 

Save the CSV to data/raw/, then run this repo's preprocessing script (for example: python scripts/build_dataset.py) to clean vendor names, standardize column names, and output the final dataset to data/processed/tm_under_1m_naics_21_33.csv.