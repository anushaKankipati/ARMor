#anusha claude response
import pandas as pd
import numpy as np
from typing import Dict, List, Set, Tuple
import sqlite3
import io
import sys

class DataModeler:
    def __init__(self):
        self.tables = {}
        self.relationships = []
        self.db_connection = None
        
    def load_multi_table_csv(self, file_path: str) -> None:
        """Load a CSV file containing multiple tables separated by blank lines."""
        try:
            with open(file_path, 'r') as file:
                content = file.read()
            
            # Split the content into separate tables
            table_blocks = [block.strip() for block in content.split('\n\n') if block.strip()]
            
            for block in table_blocks:
                # Convert block to StringIO to use with pandas
                block_io = io.StringIO(block)
                # Read the data, handling spaces after commas
                df = pd.read_csv(block_io, skipinitialspace=True)
                
                # Determine table name from the ID column
                id_columns = [col for col in df.columns if col.endswith('ID')]
                if not id_columns:
                    raise ValueError(f"No ID column found in table with columns: {df.columns}")
                
                # Use the first ID column to name the table
                table_name = id_columns[0].replace('ID', 's')
                self.tables[table_name] = df
                
            if not self.tables:
                raise ValueError("No valid tables found in the CSV file")
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find CSV file: {file_path}")
        except pd.errors.EmptyDataError:
            raise ValueError("The CSV file is empty")
        except Exception as e:
            raise Exception(f"Error loading CSV file: {str(e)}")
    
    def _identify_primary_key(self, df: pd.DataFrame) -> str:
        """Identify primary key based on ID column and uniqueness."""
        # First try to find an ID column that's unique
        id_columns = [col for col in df.columns if col.endswith('ID')]
        for col in id_columns:
            if df[col].nunique() == len(df):
                return col
        
        # If no suitable ID column found, return None
        return None
    
    def _identify_foreign_keys(self, source_df: pd.DataFrame, target_df: pd.DataFrame, 
                             source_table: str, target_table: str) -> List[Tuple[str, str]]:
        """Identify potential foreign key relationships between tables."""
        relationships = []
        target_id_columns = [col for col in target_df.columns if col.endswith('ID')]
        
        for source_col in source_df.columns:
            if source_col.endswith('ID'):
                source_values = set(source_df[source_col].dropna().unique())
                
                for target_col in target_id_columns:
                    target_values = set(target_df[target_col].dropna().unique())
                    
                    # Check if source values are subset of target values
                    if (source_values and target_values and 
                        source_values.issubset(target_values) and
                        source_col != target_col):  # Avoid self-referential keys
                        relationships.append((
                            f"{source_table}.{source_col}",
                            f"{target_table}.{target_col}"
                        ))
        
        return relationships
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer SQL data type from pandas series."""
        dtype = series.dtype
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'REAL'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'INTEGER'  # SQLite doesn't have boolean
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TEXT'  # Store datetime as TEXT in SQLite
        else:
            return 'TEXT'  # Default to TEXT for strings and other types
    
    def analyze(self) -> Dict:
        """Analyze loaded tables and infer relationships."""
        schema = {}
        self.relationships = []
        
        # First pass: identify primary keys and data types
        for table_name, df in self.tables.items():
            primary_key = self._identify_primary_key(df)
            columns = []
            
            if primary_key is None:
                primary_key = 'id'
                columns.append({
                    'name': primary_key,
                    'type': 'INTEGER',
                    'constraints': ['PRIMARY KEY AUTOINCREMENT']
                })
            else:
                columns.append({
                    'name': primary_key,
                    'type': 'INTEGER',
                    'constraints': ['PRIMARY KEY']
                })
            
            # Add remaining columns
            for column in df.columns:
                if column != primary_key:
                    data_type = self._infer_data_type(df[column])
                    constraints = []
                    if df[column].notna().all():
                        constraints.append('NOT NULL')
                    
                    columns.append({
                        'name': column,
                        'type': data_type,
                        'constraints': constraints
                    })
            
            schema[table_name] = {
                'columns': columns,
                'primary_key': primary_key
            }
        
        # Second pass: identify foreign key relationships
        for source_table, source_df in self.tables.items():
            for target_table, target_df in self.tables.items():
                if source_table != target_table:
                    relationships = self._identify_foreign_keys(
                        source_df, target_df, source_table, target_table
                    )
                    self.relationships.extend(relationships)
        
        return {
            'schema': schema,
            'relationships': self.relationships
        }
    
    def create_database(self, db_path: str, analysis: Dict) -> None:
        """Create SQLite database file and populate it with the data."""
        try:
            # Create or connect to SQLite database
            self.db_connection = sqlite3.connect(db_path)
            cursor = self.db_connection.cursor()
            
            # Enable foreign key support
            cursor.execute("PRAGMA foreign_keys = ON;")
            
            # Create tables
            for table_name, table_info in analysis['schema'].items():
                columns = []
                for col in table_info['columns']:
                    column_def = f"{col['name']} {col['type']}"
                    if col['constraints']:
                        column_def += f" {' '.join(col['constraints'])}"
                    columns.append(column_def)
                
                create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
                cursor.execute(create_table_sql)
                
                # Insert data
                df = self.tables[table_name]
                df.to_sql(table_name, self.db_connection, 
                         if_exists='replace', index=False)
            
            # Add foreign key constraints
            for source, target in analysis['relationships']:
                source_table, source_col = source.split('.')
                target_table, target_col = target.split('.')
                constraint_name = f"fk_{source_table}_{source_col}"
                
                try:
                    alter_table_sql = (
                        f"ALTER TABLE {source_table} "
                        f"ADD CONSTRAINT {constraint_name} "
                        f"FOREIGN KEY ({source_col}) REFERENCES {target_table}({target_col})"
                    )
                    cursor.execute(alter_table_sql)
                except sqlite3.OperationalError as e:
                    print(f"Warning: Could not add foreign key constraint {constraint_name}: {str(e)}")
            
            self.db_connection.commit()
            
        except Exception as e:
            if self.db_connection:
                self.db_connection.rollback()
            raise Exception(f"Error creating database: {str(e)}")
        
        finally:
            if self.db_connection:
                self.db_connection.close()

if __name__ == "__main__":
    # Create modeler instance
    modeler = DataModeler()
    
    # Load and process the CSV file
    print("Loading CSV file...")
    modeler.load_multi_table_csv('ex.csv')  # Replace with your CSV filename
    
    # Analyze the data
    print("Analyzing data and inferring relationships...")
    analysis = modeler.analyze()
    
    # Create the database
    print("Creating database file...")
    modeler.create_database('output.db', analysis)  # This will create output.db
    
    print("Database created successfully!")
    
    # Print summary of what was created
    print("\nCreated tables:")
    for table_name in analysis['schema'].keys():
        print(f"- {table_name}")
    
    print("\nIdentified relationships:")
    for source, target in analysis['relationships']:
        print(f"- {source} -> {target}")