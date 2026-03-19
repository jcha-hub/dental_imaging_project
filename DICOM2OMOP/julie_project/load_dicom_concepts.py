import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = r"C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project\omop_dental_cbct_v1.db"


def load_dicom_concepts(staging_csv_path, db_path=DB_PATH):
    """Load one DICOM2OMOP staging CSV into the OMOP concept table."""

    staging_csv_path = Path(staging_csv_path)

    if not staging_csv_path.exists():
        print(f"Error: file not found: {staging_csv_path}")
        return

    df = pd.read_csv(staging_csv_path)
    print(f"Loaded {len(df)} rows from staging CSV")
    print(f"Columns: {list(df.columns)}")
    print("\nSample rows:")
    print(df.head(3).to_string())

    print("\n--- Staging File Summary ---")
    if "vocabulary_id" in df.columns:
        print(f"Vocabularies: {df['vocabulary_id'].dropna().unique()}")
    if "domain_id" in df.columns:
        print(f"Domains: {df['domain_id'].dropna().unique()}")
    if "concept_class_id" in df.columns:
        print(f"Concept classes: {df['concept_class_id'].value_counts().to_dict()}")

    with sqlite3.connect(db_path) as conn:
        print("\n--- SQLite concept table schema ---")
        schema = pd.read_sql("PRAGMA table_info(concept);", conn)
        print(schema.to_string(index=False))

        # --- SIMPLE TYPE CLEANUP ---
        if "concept_id" in df.columns:
            df["concept_id"] = pd.to_numeric(df["concept_id"], errors="coerce")

        text_cols = [
            "concept_name",
            "domain_id",
            "vocabulary_id",
            "concept_class_id",
            "standard_concept",
            "concept_code",
            "invalid_reason",
        ]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype("string")

        for col in ["valid_start_date", "valid_end_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col].astype(str),
                    format="%Y%m%d",
                    errors="coerce"
                ).dt.strftime("%Y-%m-%d")

        print("\n--- DataFrame dtypes after cleanup ---")
        print(df.dtypes)

        # Drop bad concept_id rows
        bad_ids = df["concept_id"].isna()
        if bad_ids.any():
            print("\n--- Rows with invalid concept_id ---")
            print(df.loc[bad_ids, ["concept_id", "concept_name"]].head(20).to_string(index=False))
            print(f"\nDropping {bad_ids.sum()} rows with invalid concept_id")
            df = df.loc[~bad_ids].copy()

        # Drop rows missing required OMOP fields
        required_cols = [
            "concept_id",
            "concept_name",
            "domain_id",
            "vocabulary_id",
            "concept_class_id",
            "concept_code",
            "valid_start_date",
            "valid_end_date",
        ]

        for col in required_cols:
            if col in df.columns:
                df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)

        missing_required = df[required_cols].isna().any(axis=1)
        if missing_required.any():
            print("\n--- Rows missing required fields ---")
            print(df.loc[missing_required, required_cols].head(20).to_string(index=False))
            print(f"\nDropping {missing_required.sum()} rows missing required concept fields")
            df = df.loc[~missing_required].copy()

        if df.empty:
            print("\nNo valid rows remain after cleanup. Skipping insert.")
            return

        df["concept_id"] = df["concept_id"].astype(int)

        print(f"\nRows remaining for insert: {len(df)}")

        # Insert cleaned rows
        df.to_sql("concept", conn, if_exists="append", index=False)

        count = pd.read_sql("SELECT COUNT(*) AS n FROM concept", conn).iloc[0]["n"]
        print(f"✓ {count} total concepts now in OMOP concept table")


if __name__ == "__main__":
    staging_dir = Path(
        r"C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\files\OMOP CDM Staging"
    )

    if not staging_dir.exists():
        print(f"Directory not found: {staging_dir}")
        raise SystemExit(1)

    csv_file = staging_dir / "omop_table_staging_v5.csv"

    if not csv_file.exists():
        print(f"File not found: {csv_file}")
        raise SystemExit(1)

    with sqlite3.connect(DB_PATH) as conn:
        deleted = conn.execute("DELETE FROM concept").rowcount
        print(f"Cleared concept table: deleted {deleted} existing rows\n")

    print(f"Processing: {csv_file}")
    load_dicom_concepts(str(csv_file))
    print(f"Finished: {csv_file.name}")

    with sqlite3.connect(DB_PATH) as conn:
        print(pd.read_sql("SELECT COUNT(*) FROM concept", conn))
        print(pd.read_sql("SELECT * FROM concept LIMIT 10", conn))