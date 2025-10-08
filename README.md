# Firestore to GCS JSON Export Script

## Overview
This repository contains a Python script that automates the export of Firestore data to Google Cloud Storage (GCS) in a readable **JSON format**.

Firestore’s built-in export command produces metadata-encoded files that are not human-readable and are designed for database imports or BigQuery ingestion.  
This script addresses that limitation by:
- Querying documents directly from Firestore (e.g., from a `messages` collection)  
- Converting them to JSON  
- Uploading the resulting file to a GCS bucket  

The goal is to maintain a daily backup of Firestore message history in GCS for analysis, auditing, or recovery.

---

## Repository structure

---

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/your-org/firestore-gcs-export.git
cd firestore-gcs-export
```
### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up Google Cloud credentials
Ensure you have a service account with access to Firestore and GCS. Download the JSON key and replace 
```bash 
"serviceAccountKey.json" 
```
with the path to your key. 

## Usage
Run the export script:
```bash
python main.py
```
This will:
- Query Firestore documents
- Convert them to JSON
- Upload the file to your specified GCS bucket
