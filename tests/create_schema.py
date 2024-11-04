#anusha gpt response

import csv
import json
import xmltodict
import sqlite3
import difflib
from collections import defaultdict



def convert_xml_to_dict(data):
    """Recursively converts an XML structure (parsed by xmltodict) into a standard dictionary."""
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                data[key] = [convert_xml_to_dict(item) for item in value]
            elif isinstance(value, dict):
                data[key] = convert_xml_to_dict(value)
    return data

def read_file(file_path):
    """Detects file type and reads data accordingly."""
    if file_path.endswith('.csv') or file_path.endswith('.tsv'):
        delimiter = ',' if file_path.endswith('.csv') else '\t'
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            data = [row for row in reader]
    elif file_path.endswith('.json'):
        with open(file_path, 'r') as file:
            data = json.load(file)
    elif file_path.endswith('.xml'):
        with open(file_path, 'r') as file:
            data = xmltodict.parse(file.read())
            data = convert_xml_to_dict(data)
    else:
        raise ValueError("Unsupported file format.")
    return data

def clean_data(data):
    """Correct typos, remove duplicates, and standardize data."""
    cleaned_data = []
    seen_rows = set()
    
    for row in data:
        # Standardize data: lower case and strip whitespace, handle None values
        cleaned_row = {k.lower().strip(): (v.lower().strip() if isinstance(v, str) else v) for k, v in row.items()}
        row_tuple = tuple(cleaned_row.items())
        
        # Remove duplicates and correct typos
        if row_tuple not in seen_rows:
            seen_rows.add(row_tuple)
            for k, v in cleaned_row.items():
                if isinstance(v, str):  # Only attempt typo correction on strings
                    corrected_value = difflib.get_close_matches(v, seen_rows, n=1)
                    cleaned_row[k] = corrected_value[0] if corrected_value else v
            cleaned_data.append(cleaned_row)
    
    return cleaned_data


def infer_relationships(data):
    """Infer relationships, primary keys, and foreign keys."""
    columns = data[0].keys()
    potential_keys = {}
    relationships = defaultdict(list)

    for col in columns:
        unique_vals = {row[col] for row in data if row[col]}
        if len(unique_vals) == len(data):
            potential_keys[col] = 'primary key'
        else:
            relationships[col] = unique_vals
    
    # Infer foreign keys by looking for columns with similar values
    for col, values in relationships.items():
        for other_col, other_values in relationships.items():
            if col != other_col and values & other_values:
                relationships[col].append(other_col)
    
    return potential_keys, relationships

def create_sql_schema(data, potential_keys, relationships):
    """Generate SQL schema based on inferred keys and relationships."""
    conn = sqlite3.connect('normalized_data.db')
    cursor = conn.cursor()
    
    table_name = 'main_table'
    columns = ', '.join([f"{col} TEXT" for col in data[0].keys()])
    primary_keys = ', '.join([k for k, v in potential_keys.items() if v == 'primary key'])
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}, PRIMARY KEY ({primary_keys}))")
    conn.commit()
    
    # Insert data into SQL tables
    placeholders = ', '.join('?' * len(data[0].keys()))
    for row in data:
        cursor.execute(f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", tuple(row.values()))
    
    conn.commit()
    conn.close()

def main(file_path):
    data = read_file(file_path)
    cleaned_data = clean_data(data)
    potential_keys, relationships = infer_relationships(cleaned_data)
    create_sql_schema(cleaned_data, potential_keys, relationships)
    print("Data normalized and stored in 'normalized_data.db'")

# Example Usage
file_path = 'ex.csv'  # Replace with your file path
main(file_path)

conn = sqlite3.connect('normalized_data.db')
cursor = conn.cursor()

# List all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# View data in a specific table (replace 'table_name' with the actual name)
cursor.execute("SELECT * FROM usa_table;")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()