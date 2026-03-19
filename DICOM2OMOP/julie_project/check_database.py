#check that omop database is populated correctly, run in terminal with 
# get into folder with py files : cd "C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project""
# python check_database.py
import sqlite3
import pandas as pd

DB_PATH = r"C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project\omop_dental_cbct_v1.db"

with sqlite3.connect(DB_PATH) as conn:

    
    print("\n=== TABLE LIST ===")
    tables = pd.read_sql("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table'
    """, conn)
    print(tables)

    print("\n=== TABLE SCHEMAS ===")
    for table in tables['name']:
        print(f"\n--- {table} ---")
        schema = pd.read_sql(f"PRAGMA table_info({table});", conn)
        print(schema)

    print("\n--- Total concepts ---")
    print(pd.read_sql("SELECT COUNT(*) FROM concept", conn))

    print("\n--- Sample concepts ---")
    print(pd.read_sql("SELECT * FROM concept LIMIT 10", conn))

    print("\n--- CT / modality check ---")
    print(pd.read_sql("""
        SELECT concept_name, concept_code
        FROM concept
        WHERE LOWER(concept_name) LIKE '%computed tomography%'
           OR concept_code = 'CT'
        LIMIT 10
    """, conn))

    print("\n--- Dental-related terms ---")
    print(pd.read_sql("""
        SELECT concept_name
        FROM concept
        WHERE LOWER(concept_name) LIKE '%mandib%'
           OR LOWER(concept_name) LIKE '%maxill%'
           OR LOWER(concept_name) LIKE '%tooth%'
        LIMIT 10
    """, conn))


# (base) (dicom2omop_env) PS C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project> python check_database.py

# === TABLE LIST ===
#                    name
# 0               concept
# 1  concept_relationship
# 2            vocabulary
# 3                person
# 4      image_occurrence
# 5       sqlite_sequence

# === TABLE SCHEMAS ===

# --- concept ---
#    cid              name     type  notnull dflt_value  pk
# 0    0        concept_id  INTEGER        0       None   1
# 1    1      concept_name     TEXT        1       None   0
# 2    2         domain_id     TEXT        1       None   0
# 3    3     vocabulary_id     TEXT        1       None   0
# 4    4  concept_class_id     TEXT        1       None   0
# 5    5  standard_concept     TEXT        0       None   0
# 6    6      concept_code     TEXT        1       None   0
# 7    7  valid_start_date     TEXT        1       None   0
# 8    8    valid_end_date     TEXT        1       None   0
# 9    9    invalid_reason     TEXT        0       None   0

# --- concept_relationship ---
#    cid              name     type  notnull dflt_value  pk
# 0    0      concept_id_1  INTEGER        1       None   0
# 1    1      concept_id_2  INTEGER        1       None   0
# 2    2   relationship_id     TEXT        1       None   0
# 3    3  valid_start_date     TEXT        1       None   0
# 4    4    valid_end_date     TEXT        1       None   0
# 5    5    invalid_reason     TEXT        0       None   0

# --- vocabulary ---
#    cid                   name     type  notnull dflt_value  pk
# 0    0          vocabulary_id     TEXT        0       None   1
# 1    1        vocabulary_name     TEXT        1       None   0
# 2    2   vocabulary_reference     TEXT        0       None   0
# 3    3     vocabulary_version     TEXT        0       None   0
# 4    4  vocabulary_concept_id  INTEGER        0       None   0

# --- person ---
#     cid                         name     type  notnull dflt_value  pk
# 0     0                    person_id  INTEGER        0       None   1
# 1     1            gender_concept_id  INTEGER        1       None   0
# 2     2                year_of_birth  INTEGER        1       None   0
# 3     3               month_of_birth  INTEGER        0       None   0
# 4     4                 day_of_birth  INTEGER        0       None   0
# 5     5               birth_datetime     TEXT        0       None   0
# 6     6              race_concept_id  INTEGER        1       None   0
# 7     7         ethnicity_concept_id  INTEGER        1       None   0
# 8     8                  location_id  INTEGER        0       None   0
# 9     9                  provider_id  INTEGER        0       None   0
# 10   10                 care_site_id  INTEGER        0       None   0
# 11   11          person_source_value     TEXT        0       None   0
# 12   12          gender_source_value     TEXT        0       None   0
# 13   13     gender_source_concept_id  INTEGER        0       None   0
# 14   14            race_source_value     TEXT        0       None   0
# 15   15       race_source_concept_id  INTEGER        0       None   0
# 16   16       ethnicity_source_value     TEXT        0       None   0
# 17   17  ethnicity_source_concept_id  INTEGER        0       None   0

# --- image_occurrence ---
#     cid                           name     type  notnull dflt_value  pk
# 0     0            image_occurrence_id  INTEGER        0       None   1
# 1     1                      person_id  INTEGER        1       None   0
# 2     2       anatomic_site_concept_id  INTEGER        0       None   0
# 3     3                     local_path     TEXT        0       None   0
# 4     4          image_occurrence_date     TEXT        0       None   0
# 5     5                image_study_uid     TEXT        0       None   0
# 6     6               image_series_uid     TEXT        0       None   0
# 7     7            modality_concept_id  INTEGER        0       None   0
# 8     8                     image_type     TEXT        0       None   0
# 9     9          image_resolution_rows  INTEGER        0       None   0
# 10   10       image_resolution_columns  INTEGER        0       None   0
# 11   11          image_slice_thickness     REAL        0       None   0
# 12   12               number_of_frames  INTEGER        0       None   0
# 13   13  image_occurrence_source_value     TEXT        0       None   0

# --- sqlite_sequence ---
#    cid  name type  notnull dflt_value  pk
# 0    0  name             0       None   0
# 1    1   seq             0       None   0

# --- Total concepts ---
#    COUNT(*)
# 0      8811

# --- Sample concepts ---
#    concept_id                concept_name    domain_id vocabulary_id  concept_class_id standard_concept concept_code valid_start_date valid_end_date invalid_reason
# 0  2128000010               Length to End  Measurement         DICOM  DICOM Attributes             None     00080001       1993-01-01     2099-12-31           None
# 1  2128000011      Specific Character Set  Measurement         DICOM  DICOM Attributes             None     00080005       1993-01-01     2099-12-31           None
# 2  2128000012      Language Code Sequence  Measurement         DICOM  DICOM Attributes             None     00080006       1993-01-01     2099-12-31           None
# 3  2128000013                  Image Type  Measurement         DICOM  DICOM Attributes             None     00080008       1993-01-01     2099-12-31           None
# 4  2128000014            Recognition Code  Measurement         DICOM  DICOM Attributes             None     00080010       1993-01-01     2099-12-31           None
# 5  2128000015      Instance Creation Date  Measurement         DICOM  DICOM Attributes             None     00080012       1993-01-01     2099-12-31           None
# 6  2128000016      Instance Creation Time  Measurement         DICOM  DICOM Attributes             None     00080013       1993-01-01     2099-12-31           None
# 7  2128000017        Instance Creator UID  Measurement         DICOM  DICOM Attributes             None     00080014       1993-01-01     2099-12-31           None
# 8  2128000018  Instance Coercion DateTime  Measurement         DICOM  DICOM Attributes             None     00080015       1993-01-01     2099-12-31           None
# 9  2128000019               SOP Class UID  Measurement         DICOM  DICOM Attributes             None     00080016       1993-01-01     2099-12-31           None

# --- CT / modality check ---
#           concept_name concept_code
# 0  Computed tomography           CT

# --- Dental-related terms ---
#               concept_name
# 0                 Mandible
# 1                  Maxilla
# 2      Submandibular gland
# 3  Temporomandibular joint
# (base) (dicom2omop_env) PS C:\Users\julie\AI agent courses\dental_imaging_project_local\DICOM2OMOP\julie_project> 