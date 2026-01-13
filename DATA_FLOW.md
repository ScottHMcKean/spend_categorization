# Spend Categorization Data Flow

## Overview

This solution provides a complete pipeline for automated spend categorization using multiple ML approaches with a continuous feedback loop.

## Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌──────────┐
│  invoices   │     │  categories  │     │ prompts  │
│             │     │  (hierarchy) │     │(versioned)│
└──────┬──────┘     └──────┬───────┘     └────┬─────┘
       │                   │                   │
       │                   └────────┬──────────┘
       │                            │
       └────────────────┬───────────┘
                        │
                        ▼
                ┌─────────────────┐
                │  cat_bootstrap  │  ← LLM-based categorization (Claude/GPT)
                └──────┬──────────┘  source: bootstrap
                       │
                       ▼
                ┌──────────────┐
                │ cat_reviews  │  ← Human reviews (append-only)
                └──────┬───────┘  Training data for models
                       │
                       ├────────────────────┐
                       │                    │
                       ▼                    ▼
              ┌──────────────┐      ┌──────────────────┐
              │ cat_catboost │      │ cat_vectorsearch │
              └──────┬───────┘      └──────┬───────────┘
              source: catboost      source: vectorsearch
              Trained on reviews    Trained on reviews
                     │                     │
                     └──────────┬──────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ cat_reviews  │  ← Feedback loop
                         └──────────────┘  Continuous improvement
```

## Tables

### Core Tables

1. **`invoices`** - Main invoices table
   - Generated in `0_generate.ipynb`
   - Contains: order_id, date, description, supplier, amount, actual categories

2. **`categories`** - Category hierarchy
   - Loaded from `assets/categories.csv`
   - 3-level hierarchy: Level 1 (Direct/Indirect/Non-Procureable) → Level 2 → Level 3
   - Versioned in `categories_str` table (change detection via hash)

3. **`prompts`** - Categorization prompt versions
   - Stores LLM prompts used for bootstrap categorization
   - Enables prompt versioning and tracking
   - Append-only, only saves when prompt changes
   - Used by cat_bootstrap for LLM categorization

### Categorization Tables

All cat_ tables include a `source` field identifying the categorization method.

4. **`cat_bootstrap`** - LLM bootstrap categorizations
   - Source: `bootstrap`
   - Generated in `2_bootstrap.ipynb`
   - **Requires**: `invoices`, `categories`, `prompts` (latest version)
   - Uses LLM (Claude/GPT) for initial categorization
   - Fields: order_id, source, pred_level_1, pred_level_2, pred_level_3, confidence, categorized_at

5. **`cat_catboost`** - CatBoost model predictions
   - Source: `catboost`
   - Generated in `4_catboost.ipynb`
   - **Requires**: `cat_reviews` (training data)
   - Gradient boosting model

6. **`cat_vectorsearch`** - Vector search predictions
   - Source: `vectorsearch`
   - Generated in `5_vectorsearch.ipynb`
   - **Requires**: `cat_reviews` (training data)
   - Uses similarity search with embeddings

### Review Table

7. **`cat_reviews`** - Append-only human reviews
   - Training data for catboost and vectorsearch models
   - Never deleted or updated - only appended
   - Creates feedback loop for continuous improvement
   - Fields:
     - review_id: Unique review identifier
     - order_id: Invoice being reviewed
     - source: Which categorization method (bootstrap/catboost/vectorsearch)
     - reviewer: User who performed review
     - review_date: When review occurred
     - original_level_X: Original predicted categories
     - reviewed_level_X: Corrected categories
     - review_status: approved | corrected | rejected
     - comments: Review notes
     - created_at: Timestamp

## Notebooks

1. **`0_generate.ipynb`** - Generate synthetic invoices with LLM descriptions
2. **`1_eda.ipynb`** - Exploratory data analysis
3. **`2_bootstrap.ipynb`** - LLM-based bootstrap categorization
4. **`3_optimize.ipynb`** - Prompt optimization using subset of cat_reviews
5. **`4_catboost.ipynb`** - Train CatBoost model on cat_reviews
6. **`5_vectorsearch.ipynb`** - Train vector search model on cat_reviews

## App

The Streamlit app (`app.py`) allows users to:
- Switch between categorization sources (bootstrap/catboost/vectorsearch)
- Review and correct categorizations
- Submit reviews to the append-only `cat_reviews` table

Reviews are used to continuously improve the catboost and vectorsearch models.

## Model Training Flow

1. **Bootstrap**: LLM generates initial categorizations → `cat_bootstrap`
2. **Review**: Humans review and correct → `cat_reviews` (append-only)
3. **Optimize** (3_optimize.ipynb): Use subset of cat_reviews for prompt optimization
4. **Train CatBoost**: Use cat_reviews as training data → `cat_catboost`
5. **Train Vector Search**: Use cat_reviews as training data → `cat_vectorsearch`
6. **Feedback Loop**: Models make new predictions → humans review → back to cat_reviews
7. **Iterate**: More reviews → better models → higher accuracy

## Feedback Loop

The feedback loop ensures continuous improvement:

1. Models (CatBoost/Vector Search) make predictions on new invoices
2. Humans review predictions through the app
3. Corrections are saved to `cat_reviews` (append-only)
4. Models are periodically retrained on the growing review dataset
5. Model accuracy improves over time with more human feedback
