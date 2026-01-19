from http.client import CREATED
import pandas as pd
from scipy.stats import chi2_contingency
import requests
import io
import psycopg2
from psycopg2.extras import execute_values
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# ------------------------------
# Load environment variables
# ------------------------------
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = os.getenv("KOBO_CSV_URL")

PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

schema_name = "gender_inclusion_project"
table_name = "blossom_academy"
table2_name = "gender_lookup"
table3_name = "age_group_lookup"
table4_name = "education_lookup"
table5_name = "country_lookup"
table6_name = "responsibility_responses"
table7_name = "prioritized_actions"

# ------------------------------
# Fetch Kobo CSV
# ------------------------------
print("Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))
if response.status_code != 200:
    raise Exception(f"Failed to fetch Kobo data: {response.status_code}")

# Ensure proper UTF-8 encoding when fetching
response.encoding = 'utf-8'
csv_data = io.StringIO(response.text)
df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')

# Clean column names first (replace spaces, special characters)
df.columns = [col.strip().replace(" ", "_").replace("&", "and").replace("-", "_") for col in df.columns]

# Now we can apply mappings with cleaned column names

# ------------------------------
# Column name mappings (KoboToolbox to database)
# ------------------------------
column_mappings = {
    'How_do_you_describe_your_gender?': 'gender',
    'What_is_your_age_group?': 'age_group',
    'What_is_your_highest_level_of_education?': 'education',
    'Which_country_do_you_currently_live_in?': 'country',
    "Have_you_heard_the_term_'gender_and_inclusion'_before?": 'heard_gender_inclusion',
    'How_confident_are_you_in_your_understanding_of_gender_and_inclusion?': 'confidence_understanding',
    'Gender_and_inclusion_refers_to_equal_rights,_opportunities,_and_respect_for_all_genders.': 'definition_equal_rights',
    'Gender_and_inclusion_is_only_about_women.': 'definition_only_women',
    'Do_you_think_gender_and_inclusion_is_practiced_in_your_country?': 'practiced_in_country',
    'How_important_is_gender_and_inclusion_in_society?': 'importance_in_society',
    'Have_you_personally_felt_excluded_or_treated_unfairly_because_of_your_gender?': 'personal_exclusion',
    'Have_you_witnessed_someone_excluded_because_of_gender?': 'witnessed_exclusion',
    'Are_there_barriers_to_gender_and_inclusion_in_your_country?': 'barriers_exist',
    'Who_should_be_responsible_for_promoting_gender_and_inclusion?': 'responsibility_responses',
    'Which_actions_should_be_prioritized?': 'prioritized_actions',
    'The_government_should_create_and_enforce_policies_promoting_gender_and_inclusion.': 'govt_create_policies',
    'The_government_should_provide_education_and_awareness_programs.': 'govt_provide_education',
    'The_government_should_support_marginalized_groups_economically.': 'govt_support_groups',
    'The_government_should_ensure_equal_gender_representation_in_leadership.': 'govt_equal_representation',
    '_submission_time': 'submission_time',  # Add date column mapping
}

# Value mappings for categorical columns
gender_mapping = {
    'Female': 1,
    'Male': 2,
    'Prefer not to say': 3,
    'Other': 3,
}

age_group_mapping = {
    'Under 18': 1,
    '18-24': 2,
    '18–24': 2,  # en-dash variant
    '25-34': 3,
    '25–34': 3,  # en-dash variant
    '35-44': 4,
    '35–44': 4,  # en-dash variant
    '45-54': 5,
    '45–54': 5,  # en-dash variant
    '55+': 6,
    '55 +': 6,
}

education_mapping = {
    'No formal education': 1,
    'Primary': 2,
    'Secondary': 3,
    'Vocational/Technical': 4,
    'Vocational': 4,
    'University/College': 5,
    'Tertiary education': 5,
    'University': 5,
    'College': 5,
    'Postgraduate': 6,
    'Graduate': 6,
}

country_mapping = {
    'Nigeria': 1,
    'Rwanda': 2,
    'Other': 3,
}

# Function to find actual column name from mapping
def find_column(df, search_key):
    """Find a column in df that matches the mapping key"""
    if search_key in df.columns:
        return search_key
    
    # Try exact match first (case-insensitive)
    search_lower = search_key.lower()
    for col in df.columns:
        if col.lower() == search_lower:
            return col
    
    # Try partial match - check if key words are in the column
    # Remove special chars and extra spaces for comparison
    search_normalized = search_key.lower().replace("'", "").replace('"', '')
    for col in df.columns:
        col_normalized = col.lower().replace("'", "").replace('"', '')
        if search_normalized in col_normalized or col_normalized in search_normalized:
            return col
    
    # Try simple substring match
    for col in df.columns:
        if search_key.lower() in col.lower():
            return col
    
    return None

# Rename columns based on mappings
print("Mapping KoboToolbox columns to database schema...")
rename_dict = {}
for kobo_col, db_col in column_mappings.items():
    actual_col = find_column(df, kobo_col)
    if actual_col:
        rename_dict[actual_col] = db_col
        print(f"  Mapped: {kobo_col} -> {db_col}")

print(f"\nRename dictionary keys: {list(rename_dict.keys())[:5]}...")

df.rename(columns=rename_dict, inplace=True)

print(f"Renamed {len(rename_dict)} columns")
print(f"\nVerifying data in DataFrame:")
if len(df) > 0:
    first_row = df.iloc[0]
    print(f"  gender: {first_row['gender'] if 'gender' in df.columns else 'NOT FOUND'}")
    print(f"  heard_gender_inclusion: {first_row['heard_gender_inclusion'] if 'heard_gender_inclusion' in df.columns else 'NOT FOUND'}")
    print(f"  confidence_understanding: {first_row['confidence_understanding'] if 'confidence_understanding' in df.columns else 'NOT FOUND'}")

# Convert date column
if "Date" in df.columns:
    df.rename(columns={"Date": "date"}, inplace=True)

# ------------------------------
# Dynamic multi-select splitter
# ------------------------------
def split_options(cell_value):
    """
    Dynamically splits multi-select values from Kobo exports.
    Handles commas, semicolons, spaces, or mixed separators.
    """
    if pd.isna(cell_value):
        return []
    
    value_str = str(cell_value).strip()
    
    if ',' in value_str:
        sep = ','
    elif ';' in value_str:
        sep = ';'
    else:
        sep = None
    
    if sep:
        parts = [part.strip() for part in value_str.split(sep)]
    else:
        parts = value_str.split()  # whitespace split
    
    return [part for part in parts if part]

# ------------------------------
# Upload to PostgreSQL
# ------------------------------
print("Uploading data to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    # Create schema
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # ------------------------------
    # Lookup tables
    # ------------------------------
    lookup_tables = {
        "gender_lookup": [(1, 'Female'), (2, 'Male'), (3, 'Prefer not to say')],
        "age_group_lookup": [(1, 'Under 18'), (2, '18-24'), (3, '25-34'), (4, '35-44'), (5, '45-54'), (6, '55+')],
        "education_lookup": [(1, 'No formal education'), (2, 'Primary'), (3, 'Secondary'), (4, 'Vocational/Technical'),
                             (5, 'University/College'), (6, 'Postgraduate')],
        "country_lookup": [(1, 'Nigeria'), (2, 'Rwanda'), (3, 'Other')]
    }

    for table, rows in lookup_tables.items():
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table} (
            id INT PRIMARY KEY,
            label TEXT NOT NULL
        );
        """)
        execute_values(
            cur,
            f"INSERT INTO {schema_name}.{table} (id, label) VALUES %s ON CONFLICT (id) DO NOTHING",
            rows
        )

    # ------------------------------
    # Main survey table
    # ------------------------------
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        start TIMESTAMP,
        "end" TIMESTAMP,
        date DATE,
        gender_id INT,
        age_group_id INT,
        education_id INT,
        country_id INT,
        heard_gender_inclusion TEXT,
        confidence_understanding TEXT,
        definition_equal_rights TEXT,
        definition_only_women TEXT,
        practiced_in_country TEXT,
        importance_in_society TEXT,
        personal_exclusion TEXT,
        witnessed_exclusion TEXT,
        barriers_exist TEXT,
        govt_create_policies TEXT,
        govt_provide_education TEXT,
        govt_support_groups TEXT,
        govt_equal_representation TEXT,
        CONSTRAINT fk_gender FOREIGN KEY (gender_id) REFERENCES {schema_name}.gender_lookup(id),
        CONSTRAINT fk_age_group FOREIGN KEY (age_group_id) REFERENCES {schema_name}.age_group_lookup(id),
        CONSTRAINT fk_education FOREIGN KEY (education_id) REFERENCES {schema_name}.education_lookup(id),
        CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES {schema_name}.country_lookup(id)
    );
    """)

    # ------------------------------
    # Responsibility and Prioritized Actions tables
    # ------------------------------
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table6_name} (
        id SERIAL PRIMARY KEY,
        respondent_id INT NOT NULL,
        responsibility_option TEXT,
        CONSTRAINT fk_responsibility_respondent FOREIGN KEY (respondent_id)
            REFERENCES {schema_name}.{table_name}(id) ON DELETE CASCADE,
        CONSTRAINT unique_responsibility UNIQUE (respondent_id, responsibility_option)
    );
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table7_name} (
        id SERIAL PRIMARY KEY,
        respondent_id INT NOT NULL,
        action_option TEXT,
        CONSTRAINT fk_action_respondent FOREIGN KEY (respondent_id)
            REFERENCES {schema_name}.{table_name}(id) ON DELETE CASCADE,
        CONSTRAINT unique_action UNIQUE (respondent_id, action_option)
    );
    """)

    # ------------------------------
    # Insert blossom_academy
    # ------------------------------
    insert_sql = f"""
    INSERT INTO {schema_name}.{table_name} (
        start, "end", date, gender_id, age_group_id, education_id, country_id,
        heard_gender_inclusion, confidence_understanding, definition_equal_rights, definition_only_women,
        practiced_in_country, importance_in_society, personal_exclusion, witnessed_exclusion, barriers_exist,
        govt_create_policies, govt_provide_education, govt_support_groups, govt_equal_representation
    ) VALUES %s
    ON CONFLICT DO NOTHING;
    """

    # Function to map survey responses to lookup IDs
    def map_gender_id(value):
        if pd.isna(value):
            return None
        return gender_mapping.get(str(value).strip(), None)
    
    def map_age_group_id(value):
        if pd.isna(value):
            return None
        val_str = str(value).strip()
        
        # First try direct match
        if val_str in age_group_mapping:
            return age_group_mapping[val_str]
        
        # Normalize dashes and try again (handle en-dash vs hyphen)
        normalized = val_str.replace('–', '-').replace('—', '-')
        if normalized in age_group_mapping:
            return age_group_mapping[normalized]
        
        # Try removing extra spaces
        normalized_spaces = ' '.join(normalized.split())
        if normalized_spaces in age_group_mapping:
            return age_group_mapping[normalized_spaces]
        
        # Fallback - try to find any matching key
        for key, val in age_group_mapping.items():
            if key.replace('–', '-').replace('—', '-') == normalized:
                return val
        
        return None
    
    def map_education_id(value):
        if pd.isna(value):
            return None
        val_str = str(value).strip()
        
        # Direct match
        if val_str in education_mapping:
            return education_mapping[val_str]
        
        # Case-insensitive match
        val_lower = val_str.lower()
        for key, id_val in education_mapping.items():
            if key.lower() == val_lower:
                return id_val
        
        # Partial match for education levels
        if 'tertiary' in val_lower or 'university' in val_lower or 'college' in val_lower:
            return education_mapping.get('Tertiary education', education_mapping.get('University/College'))
        if 'vocational' in val_lower or 'technical' in val_lower:
            return education_mapping.get('Vocational/Technical')
        if 'secondary' in val_lower:
            return education_mapping.get('Secondary')
        if 'primary' in val_lower:
            return education_mapping.get('Primary')
        if 'postgraduate' in val_lower or 'graduate' in val_lower:
            return education_mapping.get('Postgraduate')
        if 'no formal' in val_lower or 'none' in val_lower:
            return education_mapping.get('No formal education')
        
        return None
    
    def map_country_id(value):
        if pd.isna(value):
            return None
        return country_mapping.get(str(value).strip(), None)

    # Apply mappings to create ID columns
    print("Mapping survey responses to lookup IDs...")
    print(f"Columns before ID mapping: {df.columns.tolist()[:10]}...")
    if 'gender' in df.columns:
        print("  Mapping gender...")
        df['gender_id'] = df['gender'].apply(map_gender_id)
    else:
        print("  WARNING: gender column not found")
        
    if 'age_group' in df.columns:
        print("  Mapping age_group...")
        df['age_group_id'] = df['age_group'].apply(map_age_group_id)
        print(f"    Sample age groups: {df['age_group'].unique()[:3]}")
    else:
        print("  WARNING: age_group column not found")
        
    if 'education' in df.columns:
        print("  Mapping education...")
        df['education_id'] = df['education'].apply(map_education_id)
    else:
        print("  WARNING: education column not found")
        
    if 'country' in df.columns:
        print("  Mapping country...")
        df['country_id'] = df['country'].apply(map_country_id)
    else:
        print("  WARNING: country column not found")

    # Prepare records for insertion
    print(f"Preparing {len(df)} records for insertion...")
    records = []
    skipped = 0
    for idx, (_, row) in enumerate(df.iterrows()):
        try:
            # Handle timestamp columns safely - convert to None if invalid
            # For pandas Series, use bracket notation and try/except
            start = None
            end = None
            date = None
            
            try:
                if "start" in df.columns and pd.notna(row["start"]):
                    start = pd.to_datetime(row["start"])
            except:
                pass
                    
            try:
                if "end" in df.columns and pd.notna(row["end"]):
                    end = pd.to_datetime(row["end"])
            except:
                pass
            
            # Extract date from submission_time
            try:
                if "submission_time" in df.columns and pd.notna(row["submission_time"]):
                    date = pd.to_datetime(row["submission_time"]).date()
            except:
                pass
            
            # Helper function to safely get value from Series
            def safe_get_col(col_name):
                try:
                    if col_name in df.columns:
                        val = row[col_name]
                        return val if pd.notna(val) else None
                except:
                    pass
                return None
            
            record = (
                start, end, date,
                safe_get_col("gender_id"), 
                safe_get_col("age_group_id"), 
                safe_get_col("education_id"), 
                safe_get_col("country_id"),
                safe_get_col("heard_gender_inclusion"), safe_get_col("confidence_understanding"),
                safe_get_col("definition_equal_rights"), safe_get_col("definition_only_women"),
                safe_get_col("practiced_in_country"), safe_get_col("importance_in_society"),
                safe_get_col("personal_exclusion"), safe_get_col("witnessed_exclusion"), safe_get_col("barriers_exist"),
                safe_get_col("govt_create_policies"), safe_get_col("govt_provide_education"),
                safe_get_col("govt_support_groups"), safe_get_col("govt_equal_representation")
            )
            records.append(record)
        except Exception as e:
            skipped += 1
            if skipped <= 3:  # Only print first 3 warnings
                print(f"  Warning: Row {idx} skipped - {e}")
    
    if skipped > 3:
        print(f"  ... and {skipped - 3} more rows skipped")
    print(f"Total valid records: {len(records)}")

    try:
        execute_values(cur, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"[OK] Inserted {len(records)} rows into {schema_name}.{table_name}")
    except Exception as insert_err:
        print(f"[ERROR] Failed to insert records: {insert_err}")
        # Print first record for debugging
        if records:
            print(f"Sample record: {records[0]}")
        conn.rollback()
        raise

    # ------------------------------
    # Build respondent lookup for multi-select tables
    # ------------------------------
    cur.execute(f"""
    SELECT id, start, "end", gender_id, age_group_id, education_id, country_id
    FROM {schema_name}.{table_name};
    """)
    respondent_lookup = {
        (r[1], r[2], r[3], r[4], r[5], r[6]): r[0] for r in cur.fetchall()
    }

    # ------------------------------
    # Insert responsibility_responses
    # ------------------------------
    responsibility_sql = f"""
    INSERT INTO {schema_name}.{table6_name} (respondent_id, responsibility_option)
    VALUES %s ON CONFLICT DO NOTHING;
    """
    responsibility_records = []
    for _, row in df.iterrows():
        key = (row.get("start"), row.get("end"), row.get("gender_id"),
               row.get("age_group_id"), row.get("education_id"), row.get("country_id"))
        respondent_id = respondent_lookup.get(key)
        if respondent_id:
            for option in split_options(row.get("responsibility_responses")):
                responsibility_records.append((respondent_id, option))

    if responsibility_records:
        execute_values(cur, responsibility_sql, responsibility_records, page_size=1000)
        conn.commit()
        print(f"[OK] Inserted {len(responsibility_records)} responsibility responses")

    # ------------------------------
    # Insert prioritized_actions
    # ------------------------------
    action_sql = f"""
    INSERT INTO {schema_name}.{table7_name} (respondent_id, action_option)
    VALUES %s ON CONFLICT DO NOTHING;
    """
    action_records = []
    for _, row in df.iterrows():
        key = (row.get("start"), row.get("end"), row.get("gender_id"),
               row.get("age_group_id"), row.get("education_id"), row.get("country_id"))
        respondent_id = respondent_lookup.get(key)
        if respondent_id:
            for action in split_options(row.get("prioritized_actions")):
                action_records.append((respondent_id, action))

    if action_records:
        execute_values(cur, action_sql, action_records, page_size=1000)
        conn.commit()
        print(f"[OK] Inserted {len(action_records)} prioritized actions")

except Exception as e:
    print("[ERROR]", e)
    if conn:
        conn.rollback()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("[OK] Data successfully loaded into PostgreSQL!")
