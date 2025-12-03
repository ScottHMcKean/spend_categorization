You are generating synthetic but realistic procurement transactions for a wind-turbine manufacturer called Borealis Wind Systems.

The following fields are ALREADY selected by the program and MUST NOT be changed:

- date: {{date}}
- category_level_1: {{category_level_1}}
- category_level_2: {{category_level_2}}
- category_level_3: {{category_level_3}}
- cost_centre: {{cost_centre}}
- plant: {{plant}}
- plant_id: {{plant_id}}
- region: {{region}}
- country: {{country}}

You must generate ONLY the following fields:
- supplier
- supplier_id
- order
- order_id
- description
- reviewer_comments
- amount
- unit_price
- unit
- total

Here are some primary suppliers to use where they make sense:
  - Grainger
  - McMaster-Carr
  - Siemens
  - SKF Bearings
  - Brazilian Foundry
  - German Electronics
  - US Steel Co
  - Composite Materials Ltd
  - Hydraulic Systems Inc
  - Amazon Industrial
  - Engineering Consultants
  - Logistics Partner
  - IT Vendor

Category-specific supplier guidance:
- Direct / Raw Materials:
  - Prefer: US Steel Co, Composite Materials Ltd, Brazilian Foundry.
- Direct / Components, Bearings & Seals, Electrical Assemblies, Hydraulic Systems:
  - Prefer: Siemens, SKF Bearings, German Electronics, Hydraulic Systems Inc.
- Direct / Sub-Assemblies, Blades & Hub Parts, Castings & Forgings:
  - Mix: Composite Materials Ltd, Brazilian Foundry, Siemens, SKF Bearings.
- Indirect / MRO, Office Supplies, Facilities & Utilities, Safety & PPE:
  - Prefer: Grainger, McMaster-Carr, Local MRO Supplier, Amazon Industrial.
- Indirect / Logistics & Freight:
  - Prefer: Logistics Partner.
- Indirect / IT & Software, Telecom & Connectivity, Subscriptions & Data Services:
  - Prefer: IT Vendor.
- Indirect / Professional Services, Training & HR, Recruitment Services, Events & Conferences:
  - Prefer: Engineering Consultants, Logistics Partner, Utility Company, IT Vendor (for IT training).
- Non-Procureable:
  - Use supplier = "Borealis Internal" (do NOT use seed suppliers).

Other constraints:

1. supplier_id:
   - Generate a short code like SUP-XXXX (4 digits).

2. Orders:
   - order and order_id: PO-like codes, e.g. ORD-2024-00123.
   - You may set order = order_id.

3. Description:
   - Must include the category_level_3 phrase plus usage context like:
     “for turbine assy”, “for nacelle maint”, “for blade prod”, “line support”, etc.
   - Make it messy and realistic:
     - Include abbreviations (assy, maint, qty, rplcmnt), mixed casing, and typos.
     - Add extra notes like “spot buy – see old PO”, “LOCAL VEND”, “see email”.
   - About 10–15% of descriptions can be very generic and vague such as:
     “misc supplies – emerg buy, local vend”.

4. Reviewer_comments:
   - Often empty, or one of:
     “OK”, “Check price vs last PO”, “Urgent – line-down risk”,
     “Need tech review”, “Local buy – no contract”, “Approved as exception”.

5. Amount, unit_price, unit, total:
   - amount: integer > 0.
   - For Non-Procureable categories (Payroll, Government Services & Taxes, Finance Charges, Miscellaneous Expenses), use amount = 1 (lump sum).
   - unit_price: positive float with 2 decimals.
     - 5–5,000 typical for goods and services.
     - 1,000–500,000 for Non-Procureable lump sums.
   - unit: choose from: EA, KG, M, L, BOX, PALLET, HOUR.
     - Non-Procureable can use EA.
   - total MUST equal amount * unit_price, rounded to 2 decimals.
   - Use plain numbers only (no thousands separators, no currency symbols).

Output format:

Return a SINGLE JSON object with EXACTLY these keys:
{
  "supplier": ...,
  "supplier_id": ...,
  "order": ...,
  "order_id": ...,
  "description": ...,
  "reviewer_comments": ...,
  "amount": ...,
  "unit_price": ...,
  "unit": ...,
  "total": ...
}

Do not include any other text or explanation.