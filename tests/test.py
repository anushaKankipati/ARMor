#lucas gpt response

import pandas as pd
import sqlite3
import json
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from sklearn.preprocessing import LabelEncoder

class DataModeler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
    
    def load_data(self):
        if self.file_path.endswith('.csv') or self.file_path.endswith('.tsv'):
            sep = '\t' if self.file_path.endswith('.tsv') else ','
            self.data = pd.read_csv(self.file_path, sep=sep)
        elif self.file_path.endswith('.json'):
            self.data = pd.read_json(self.file_path)
        elif self.file_path.endswith('.xml'):
            tree = ET.parse(self.file_path)
            root = tree.getroot()
            data = [child.attrib for child in root]
            self.data = pd.DataFrame(data)
        else:
            raise ValueError("Unsupported file format.")
    
    def clean_data(self):
        # Drop duplicates
        self.data.drop_duplicates(inplace=True)
        
        # Fill missing values
        self.data.fillna(method='ffill', inplace=True)
        
        # Standardize text fields (example)
        for col in self.data.select_dtypes(include=['object']).columns:
            self.data[col] = self.data[col].apply(lambda x: x.lower().strip())
    
    def infer_keys_and_relationships(self):
        # Detect primary keys based on unique columns
        primary_keys = []
        for col in self.data.columns:
            if self.data[col].is_unique:
                primary_keys.append(col)
        
        # Check for potential foreign keys
        foreign_keys = []
        for i, col1 in enumerate(self.data.columns):
            for j, col2 in enumerate(self.data.columns):
                if i != j and self.data[col1].dtype == self.data[col2].dtype:
                    similarity = SequenceMatcher(None, col1, col2).ratio()
                    if similarity > 0.8:
                        foreign_keys.append((col1, col2))
        
        return primary_keys, foreign_keys

    def normalize_data(self):
        # Placeholder normalization (splitting based on foreign keys)
        normalized_tables = {}
        for fk1, fk2 in self.foreign_keys:
            table_name = f"{fk1}_{fk2}_table"
            normalized_tables[table_name] = self.data[[fk1, fk2]].drop_duplicates()
        
        return normalized_tables
    
    def save_to_sql(self, db_name):
        conn = sqlite3.connect(db_name)
        
        # Save primary data table
        self.data.to_sql('main_table', conn, if_exists='replace', index=False)
        
        # Save normalized tables
        for table_name, table_data in self.normalized_tables.items():
            table_data.to_sql(table_name, conn, if_exists='replace', index=False)
        
        conn.close()
    
    def process_file(self, db_name="output.db"):
        self.load_data()
        self.clean_data()
        self.primary_keys, self.foreign_keys = self.infer_keys_and_relationships()
        self.normalized_tables = self.normalize_data()
        self.save_to_sql(db_name)

# Usage
file_path = 'input.csv'  # replace with your file path
modeler = DataModeler(file_path)
modeler.process_file()
