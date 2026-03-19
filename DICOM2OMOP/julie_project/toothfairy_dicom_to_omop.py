#code to extract and transform ToothFairy DICOM to OMOP
# mod of demonstration/extract_ADNI_images.ipynb and transform_imaging_metadata.ipynb
#in terminal go to folder with this py file and then run : 
#python toothfairy_dicom_to_omop.py "C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\data\ToothFairy"

# toothfairy_dicom_to_omop.py

import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import pydicom

DB_PATH =  r"C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project\omop_dental_cbct_v1.db"


# ============================================================
# HELPERS
# ============================================================

def stable_int_id(text: str, digits: int = 10) -> int:
    """
    Create a stable positive integer ID from a string.
    Safer than Python's built-in hash(), which changes across runs.
    """
    h = hashlib.md5(text.encode("utf-8")).hexdigest()
    return int(h[:digits], 16)


def normalize_dicom_date(date_str: str) -> str:
    """
    Convert DICOM date YYYYMMDD -> YYYY-MM-DD.
    Return empty string if missing or invalid.
    """
    if not date_str:
        return ""
    try:
        return datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return ""


# ============================================================
# STEP 1: Extract DICOM metadata from ToothFairy files
# ============================================================

def extract_toothfairy_metadata(dicom_dir):
    """
    Extract metadata from ToothFairy DICOM folders.

    Assumptions from EDA:
    - each folder = one unique patient
    - no subfolders
    - each folder contains .dcm files + one metadata file
    - BodyPartExamined is missing -> hardcode JAW
    - SliceThickness is missing -> store as None
    - PixelSpacing is available and should be extracted
    """

    dicom_dir = Path(dicom_dir)

    if not dicom_dir.exists():
        raise FileNotFoundError(f"DICOM directory not found: {dicom_dir}")

    records = []

    patient_dirs = sorted([d for d in dicom_dir.iterdir() if d.is_dir()])

    if not patient_dirs:
        print("⚠ No patient folders found.")
        return pd.DataFrame()

    for patient_dir in patient_dirs:
        dcm_files = sorted(patient_dir.glob("*.dcm"))

        if not dcm_files:
            print(f"  ⚠ No .dcm files found in {patient_dir.name}")
            continue

        try:
            # Read only the first DICOM header in the folder
            ds = pydicom.dcmread(str(dcm_files[0]), stop_before_pixels=True)

            record = {
                "folder_name": patient_dir.name,
                "patient_id": str(getattr(ds, "PatientID", patient_dir.name)),
                "patient_sex": str(getattr(ds, "PatientSex", "U")),
                "study_instance_uid": str(getattr(ds, "StudyInstanceUID", "")),
                "series_instance_uid": str(getattr(ds, "SeriesInstanceUID", "")),
                "study_date": normalize_dicom_date(str(getattr(ds, "StudyDate", ""))),
                "modality": str(getattr(ds, "Modality", "CT")),

                # ToothFairy EDA finding: missing, so use placeholder
                "body_part_examined": "JAW",

                "manufacturer": str(getattr(ds, "Manufacturer", "")),
                "manufacturer_model": str(getattr(ds, "ManufacturerModelName", "")),
                "kvp": float(getattr(ds, "KVP", 0) or 0),
                "rows": int(getattr(ds, "Rows", 0) or 0),
                "columns": int(getattr(ds, "Columns", 0) or 0),
                "number_of_slices": len(dcm_files),

                # ToothFairy EDA finding: missing
                "slice_thickness": None,

                "local_path": str(patient_dir),
            }

            # Pixel spacing
            ps = getattr(ds, "PixelSpacing", None)
            if ps is not None and len(ps) >= 2:
                record["pixel_spacing_row"] = float(ps[0])
                record["pixel_spacing_col"] = float(ps[1])
            else:
                record["pixel_spacing_row"] = None
                record["pixel_spacing_col"] = None

            records.append(record)

        except Exception as e:
            print(f"  ✗ Error reading {patient_dir.name}: {e}")

    df = pd.DataFrame(records)

    print(f"\n✓ Extracted metadata from {len(df)} patient folders")

    if not df.empty:
        print(f"  Manufacturers: {df['manufacturer'].dropna().unique()}")
        print(f"  Models: {df['manufacturer_model'].dropna().unique()}")
        print(f"  Date range: {df['study_date'].min()} to {df['study_date'].max()}")
        print(f"  Modality values: {df['modality'].dropna().unique()}")
        print(f"  Pixel spacing present in {df['pixel_spacing_row'].notna().sum()} / {len(df)} records")

    return df


# ============================================================
# STEP 2: Transform to OMOP format
# ============================================================

def transform_to_omop(metadata_df):
    """
    Transform extracted ToothFairy metadata into DataFrames
    matching the actual SQLite person and image_occurrence tables.
    """

    if metadata_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    patient_to_person_id = {
        pid: stable_int_id(f"person::{pid}", digits=10)
        for pid in metadata_df["patient_id"].dropna().unique()
    }

    unique_patients = metadata_df.drop_duplicates("patient_id")

    person_records = []
    for _, row in unique_patients.iterrows():
        sex = row["patient_sex"]

        if sex == "M":
            gender_concept_id = 8507
        elif sex == "F":
            gender_concept_id = 8532
        else:
            gender_concept_id = 0

        person_records.append({
            "person_id": patient_to_person_id[row["patient_id"]],
            "gender_concept_id": gender_concept_id,
            "year_of_birth": 1900,           # placeholder
            "month_of_birth": 1,             # placeholder
            "day_of_birth": 1,               # placeholder
            "birth_datetime": None,          # deidentified
            "race_concept_id": 0,            # placeholder
            "ethnicity_concept_id": 0,       # placeholder
            "location_id": None,
            "provider_id": None,
            "care_site_id": None,
            "person_source_value": row["patient_id"],
            "gender_source_value": sex,
            "gender_source_concept_id": 0,
            "race_source_value": "Unknown",
            "race_source_concept_id": 0,
            "ethnicity_source_value": "Unknown",
            "ethnicity_source_concept_id": 0,
        })

    person_df = pd.DataFrame(person_records)

    image_records = []
    for idx, row in metadata_df.reset_index(drop=True).iterrows():
        image_occurrence_id = idx + 1
        person_id = patient_to_person_id[row["patient_id"]]

        image_records.append({
            "image_occurrence_id": image_occurrence_id,
            "person_id": person_id,
            "anatomic_site_concept_id": 4103445,   # placeholder jaw-region concept
            "local_path": row["local_path"],
            "image_occurrence_date": row["study_date"],
            "image_study_uid": row["study_instance_uid"],
            "image_series_uid": row["series_instance_uid"],
            "modality_concept_id": 4013200,        # CT
            "image_type": "CBCT",
            "image_resolution_rows": row["rows"],
            "image_resolution_columns": row["columns"],
            "image_slice_thickness": row["slice_thickness"],  # None per EDA
            "number_of_frames": row["number_of_slices"],
            "image_occurrence_source_value": f"ToothFairy_{row['patient_id']}",
        })

    image_df = pd.DataFrame(image_records)

    print("\n✓ Transformed to OMOP format:")
    print(f"  person records:            {len(person_df)}")
    print(f"  image_occurrence records:  {len(image_df)}")

    return person_df, image_df


# ============================================================
# STEP 3: Load into SQLite
# ============================================================

def load_to_omop(person_df, image_df, db_path=DB_PATH):
    """
    Load transformed data into SQLite.
    Clears person and image_occurrence first for clean reruns.
    """

    with sqlite3.connect(db_path) as conn:
        for table in ["person", "image_occurrence"]:
            try:
                deleted = conn.execute(f"DELETE FROM {table}").rowcount
                print(f"  Cleared {table}: deleted {deleted} rows")
            except Exception as e:
                print(f"  Could not clear {table}: {e}")

        if not person_df.empty:
            person_df.to_sql("person", conn, if_exists="append", index=False)

        if not image_df.empty:
            image_df.to_sql("image_occurrence", conn, if_exists="append", index=False)

        for table in ["person", "image_occurrence", "concept"]:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", conn).iloc[0]["n"]
                print(f"  {table}: {count} rows")
            except Exception as e:
                print(f"  {table}: could not count rows ({e})")

    print(f"\n✓ All data loaded to {db_path}")


# ============================================================
# BASIC CHECKS
# ============================================================

def run_basic_checks(db_path=DB_PATH):
    """
    Run simple checks after loading.
    """
    print("\nRunning basic checks...")

    with sqlite3.connect(db_path) as conn:
        try:
            print("\n--- person count ---")
            print(pd.read_sql("SELECT COUNT(*) AS n FROM person", conn))

            print("\n--- image_occurrence count ---")
            print(pd.read_sql("SELECT COUNT(*) AS n FROM image_occurrence", conn))

            print("\n--- sample image_occurrence rows ---")
            df_img = pd.read_sql("SELECT * FROM image_occurrence LIMIT 5", conn)
            print(df_img.T)

            print("\n--- image_type values ---")
            print(pd.read_sql("""
                SELECT image_type, COUNT(*) AS n
                FROM image_occurrence
                GROUP BY image_type
            """, conn))

        except Exception as e:
            print(f"Basic checks failed: {e}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python toothfairy_dicom_to_omop.py /path/to/toothfairy/dicom/")
        print("\nExample:")
        print("  python toothfairy_dicom_to_omop.py ./ToothFairy_Dataset/dicom/")
        raise SystemExit(1)

    dicom_dir = sys.argv[1]

    print("=" * 60)
    print("ToothFairy DICOM -> OMOP Pipeline (SQLite)")
    print("=" * 60)

    print("\n[1/4] Extracting DICOM metadata...")
    metadata_df = extract_toothfairy_metadata(dicom_dir)

    if metadata_df.empty:
        print("\nNo metadata extracted. Exiting.")
        raise SystemExit(1)

    print("\n[2/4] Transforming to OMOP format...")
    person_df, image_df = transform_to_omop(metadata_df)

    print("\n[3/4] Loading to SQLite...")
    load_to_omop(person_df, image_df)

    print("\n[4/4] Running basic checks...")
    run_basic_checks()

    print("\n" + "=" * 60)
    print("Done.")
    print(f"Database file: {DB_PATH}")
    print("=" * 60)

# (base) (dicom2omop_env) PS C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project> python toothfairy_dicom_to_omop.py "C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\data\ToothFairy"
# ============================================================
# ToothFairy DICOM -> OMOP Pipeline (SQLite)
# ============================================================

# [1/4] Extracting DICOM metadata...

# ✓ Extracted metadata from 4 patient folders
#   Manufacturers: ['NewTom']
#   Models: ['NTVGiMK4']
#   Date range: 2019-03-20 to 2020-09-18
#   Modality values: ['CT']
#   Pixel spacing present in 4 / 4 records

# [2/4] Transforming to OMOP format...

# ✓ Transformed to OMOP format:
#   person records:            4
#   image_occurrence records:  4

# [3/4] Loading to SQLite...
#   Cleared person: deleted 4 rows
#   Cleared image_occurrence: deleted 4 rows
#   person: 4 rows
#   image_occurrence: 4 rows
#   concept: 8811 rows

# ✓ All data loaded to C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project\omop_dental_cbct_v1.db

# [4/4] Running basic checks...

# Running basic checks...

# --- person count ---
#    n
# 0  4

# --- image_occurrence count ---
#    n
# 0  4

# --- sample image_occurrence rows ---
#                                                                                0  ...                                                  3
# image_occurrence_id                                                            1  ...                                                  4
# person_id                                                           881643926336  ...                                       597099833652
# anatomic_site_concept_id                                                 4103445  ...                                            4103445
# local_path                     C:\Users\julie\AI agent courses\dental_imaging...  ...  C:\Users\julie\AI agent courses\dental_imaging...
# image_occurrence_date                                                 2020-09-18  ...                                         2020-09-18
# image_study_uid                    1.3.76.13.10010.39.32.19.3132.1600414505.4501  ...      1.3.76.13.10010.39.32.19.3132.1600426197.4612
# image_series_uid                      1.76.380.18.10.1131014132236753.30299.91.1  ...         1.76.380.18.10.1131014132236753.30332.91.1
# modality_concept_id                                                      4013200  ...                                            4013200
# image_type                                                                  CBCT  ...                                               CBCT
# image_resolution_rows                                                        274  ...                                                274
# image_resolution_columns                                                     410  ...                                                410
# image_slice_thickness                                                       None  ...                                               None
# number_of_frames                                                             332  ...                                                278
# image_occurrence_source_value                                ToothFairy_00554076  ...                                ToothFairy_00706916

# [14 rows x 4 columns]

# --- image_type values ---
#   image_type  n
# 0       CBCT  4

# ============================================================
# Done.
# Database file: C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project\omop_dental_cbct_v1.db
# ============================================================
# (base) (dicom2omop_env) PS C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project> 