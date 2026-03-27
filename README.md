# Audit Data Pipeline: Purchase vs. GSTR-2A Reconciliation

This repository contains a Python-based data pipeline designed to automate the reconciliation of **Chemical/FOCUS** ledger reports against Purchase Orders (PO), Goods Received Notes (GRN), and GSTR-2A GST filings.

---

## Core Capabilities
* **High-Speed Processing:** Converts bulky `.xlsx` ledgers into `.parquet` format for optimized RAM usage and speed.
* **Regex Extraction:** Automatically extracts PO and GRN identifiers from unstructured narration strings.
* **Fuzzy Logic Reconciliation:** Utilizes `rapidfuzz` to match inconsistent Invoice/Bill numbers between internal ledgers and government GSTR-2A data.
* **Memory Management:** Implements explicit garbage collection (`gc.collect`) to handle large-scale audit datasets without system crashes.

---

## System Requirements
The pipeline requires the following Python libraries:
* `pandas`
* `pyarrow`
* `os`
* `gc`
* `rapidfuzz`

---

## Function Reference

### 1. Data Conversion & Pre-processing
| Function | Purpose |
| :--- | :--- |
| `convert_to_partquet` | Scans a source folder, identifies the "Date" header, cleans numeric `Debit/Credit` columns, and saves as Parquet. |
| `formatting` | Merges "Search" and "Statement" ledger formats into a single unified audit base. |
| `po_process` | Consolidates multiple Purchase Order reports into a single master DataFrame. |

### 2. Reconciliation Modules
* **`purchase_vs_po`**: Performs a left-join between the ledger and PO reports using extracted PO numbers as the key.
* **`purchase_vs_grn`**: Reconciles the ledger against physical GRN reports using a composite key of `[PO, GRN]`.
* **`purchase_vs_gstr2a`**: 
    1.  Filters GSTR-2A data by **PAN** (extracted via Regex).
    2.  Performs a `token_sort_ratio` fuzzy match on **Bill/Invoice Numbers**.
    3.  Outputs three separate reports: Matched, Unmatched, and a Complete Master file.

---

## Regex Logic
The pipeline utilizes specific patterns to parse unstructured "Narration" fields:
* **PO Pattern:** `(PO\/\d+-\d+\/\d+)`
* **GRN Pattern:** Advanced lookahead pattern to capture GRN numbers while excluding date strings (`dt`, `dated`).

---

## Usage Workflow
1.  **Stage 1:** Run `convert_to_partquet()` to prepare raw ledger files.
2.  **Stage 2:** Run `formatting()` to create the cleaned `df3` base.
3.  **Stage 3:** Execute specific audit functions (`purchase_vs_po`, `purchase_vs_grn`, or `purchase_vs_gstr2a`) depending on the audit objective.

---

## Note on Memory Management
Due to the size of typical ledger exports, this script utilizes `del` and `gc.collect()` after major transformations. It is recommended to monitor RAM usage when processing annual datasets.