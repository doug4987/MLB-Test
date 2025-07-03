import sqlite3
import csv
from datetime import datetime

# Connect to the SQLite database
conn = sqlite3.connect('mlb_props.db')
cursor = conn.cursor()

# Define the query to select the plays for today
today = datetime.now().strftime('%Y-%m-%d')
query = """
SELECT * FROM props WHERE DATE(scrape_date) = ?
"""

# Execute the query
cursor.execute(query, (today,))

# Fetch all results
results = cursor.fetchall()

# Define CSV file path
csv_file_path = '/Users/doug/new_scraper/plays_today.csv'

# Get the column names
column_names = [description[0] for description in cursor.description]

# Write to CSV
with open(csv_file_path, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(column_names)  # Write the header
    writer.writerows(results)  # Write data rows

# Close the connection
conn.close()

print(f"Exported {len(results)} plays for {today} to {csv_file_path}")
print(f"CSV file saved at: {csv_file_path}")
