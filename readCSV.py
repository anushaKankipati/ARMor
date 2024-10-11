import csv
from collections import defaultdict

def find_duplicates(file_path):
    rows = defaultdict(int)
    duplicates = []

    # Reading the CSV file
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header

        for row in reader:
            row_tuple = tuple(row)  # Convert row to tuple so it can be hashed
            rows[row_tuple] += 1

    # Finding duplicates
    for row, count in rows.items():
        if count > 1:
            duplicates.append(row)

    return duplicates

# Test the function
csv_file = 'sample.csv'
duplicates = find_duplicates(csv_file)

if duplicates:
    print("Duplicate entries found:")
    for dup in duplicates:
        print(dup)
else:
    print("No duplicates found.")
