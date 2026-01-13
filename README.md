# Spend Categorization - Complete Solution Summary

## Table Structure

All tables use consistent naming with the `cat_` prefix for categorization:

### Core Tables
- **`invoices`** - Main invoices with LLM-enhanced descriptions
- **`categories`** - Category hierarchy (3 levels)
- **`prompts`** - Categorization prompt versions

### Categorization Tables (with `source` field)
- **`cat_bootstrap`** - LLM categorizations (source: `bootstrap`)
- **`cat_catboost`** - CatBoost predictions (source: `catboost`)  
- **`cat_vectorsearch`** - Vector search predictions (source: `vectorsearch`)

### Review Table
- **`cat_reviews`** - Append-only human reviews (training data)

## Data Flow

```
1. Generate Invoices
   └─> invoices (0_generate.ipynb)
       
2. Bootstrap with LLM
   └─> cat_bootstrap (2_bootstrap.ipynb)
       source: "bootstrap"
       
3. Human Review (App)
   └─> cat_reviews (append-only)
       Captures corrections and approvals
       
4. Train Models on Reviews
   ├─> cat_catboost (4_catboost.ipynb)
   │   source: "catboost"
   │   Uses cat_reviews as training data
   │
   └─> cat_vectorsearch (5_vectorsearch.ipynb)
       source: "vectorsearch"
       Uses cat_reviews as training data
```

## Key Features

### Source Tracking
Every categorization includes a `source` field:
- `bootstrap` - LLM-based (Claude/GPT)
- `catboost` - Gradient boosting model
- `vectorsearch` - RAG with embeddings

### Append-Only Reviews
The `cat_reviews` table is **append-only**:
- Never delete rows
- Never update rows
- Only insert new reviews
- Provides clean training data for models
- Enables full audit trail

### Batch Processing
All LLM operations use batch processing:
- `batch_size`: 1000 rows per batch (configurable)
- `MERGE INTO` for idempotent updates
- `processed_at` timestamp for tracking

### App Features
- Switch between categorization sources (bootstrap/catboost/vectorsearch)
- Review and correct categorizations
- Submit to append-only reviews table
- Test mode uses mock CSV data from `assets/`

## File Structure

```
spend_categorization/
├── config.yaml                    # Unified configuration
├── DATA_FLOW.md                   # Detailed data flow documentation
├── assets/
│   ├── categories.csv             # Category hierarchy
│   ├── invoices.csv               # Sample/test invoice data
│   └── cat_*.csv                  # Mock categorization data for test mode
├── 0_generate.ipynb               # Generate invoices
├── 1_eda.ipynb                    # EDA
├── 2_bootstrap.ipynb              # LLM categorization
├── 3_optimize.ipynb               # Prompt optimization
├── 4_catboost.ipynb               # Train CatBoost model
├── 5_vectorsearch.ipynb           # Train vector search model
├── app.py                         # Streamlit review app
├── src/
│   ├── config.py                  # Unified config class
│   ├── utils.py                   # Shared utilities
│   └── app/
│       ├── database.py            # Backend abstraction
│       ├── queries.py             # Query functions
│       └── ui_components.py       # Streamlit components
├── assets/
│   ├── cat_bootstrap.csv          # Mock bootstrap data
│   ├── cat_catboost.csv           # Mock catboost data
│   ├── cat_vectorsearch.csv       # Mock vectorsearch data
│   └── cat_reviews.csv # Mock review data
└── tests/
    ├── test_config.py             # Config tests
    ├── test_database.py           # Database tests
    └── test_generate.py           # Generation tests
```

## Configuration

All tables and settings are in `config.yaml`:

```yaml
categorize:
  tables:
    cat_bootstrap: cat_bootstrap
    cat_catboost: cat_catboost
    cat_vectorsearch: cat_vectorsearch
    cat_reviews: cat_reviews
  
  categorization_source: bootstrap  # Which source to use in app
```

## Testing

- **Test mode**: Uses mock CSV files from `assets/`
- **Prod mode**: Uses Databricks Lakebase PostgreSQL
- Switch modes in `config.yaml`: `app.mode: test` or `prod`

## Next Steps

1. Run `0_generate.ipynb` to create invoices
2. Run `2_bootstrap.ipynb` for initial LLM categorization
3. Use `app.py` to review and correct
4. Reviews accumulate in `cat_reviews`
5. Train models with `4_catboost.ipynb` and `5_vectorsearch.ipynb`
6. Switch app to use trained models
7. Iterate: more reviews → better models
