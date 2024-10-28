import csv
from collections import defaultdict

def find_duplicates(file_path, output_file = 'dups.csv'):
    rows = defaultdict(int)
    #duplicates = []

    # Reading the CSV file
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header

        # NEW: Open output file to write dups
        with open(output_file, mode='w', newline='') as output:
            writer = csv.writer(output)
            writer.writerow(header) # Write header to output csv file

            for row in reader:
                row_tuple = tuple(row)  # Convert row to tuple so it can be hashed
                rows[row_tuple] += 1

                if rows[row_tuple] == 2:
                    writer.writerow(row) # We only write it the first time the dup has been confirmed
                    print(row)

            # Finding duplicates
            #for row, count in rows.items():
                #if count > 1:
                    #writer.writerow(row)
                    #print(row)

    #return duplicates
    print(f"Duplicates are stored in '{output_file}'\n") #maybe keep track of how many dups

# Test the function
csv_file = 'sample.csv'
find_duplicates(csv_file)
'''
duplicates = find_duplicates(csv_file)

if duplicates:
    print("Duplicate entries found:")
    for dup in duplicates:
        print(dup)
else:
    print("No duplicates found.")
'''
