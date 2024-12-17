import os
import csv
import sqlite3
from datetime import datetime
from collections import defaultdict
import sys

# Increase CSV field size limit to avoid errors
csv.field_size_limit(sys.maxsize)

# Function to create output directory using TXT file name and timestamp
def create_output_directory(txt_file):
    base_name = os.path.splitext(os.path.basename(txt_file))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.getcwd(), f"{base_name}_{timestamp}")
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"Output directory created at: {output_path}")
    return output_path

# Function to create SQLite database with a unique name
def create_database(txt_file, output_path):
    # Generate a unique name based on dataset and timestamp
    base_name = os.path.splitext(os.path.basename(txt_file))[0]
    db_name = os.path.join(output_path, f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")

    # Create SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create the bird sightings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bird_sightings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            common_name TEXT,
            scientific_name TEXT,
            locality TEXT,
            country TEXT,
            state TEXT,
            county TEXT,
            observation_date TEXT,
            year INTEGER,
            month INTEGER,
            observation_count INTEGER,
            breeding_category TEXT
        )
    ''')
    conn.commit()
    print(f"Database created: {db_name}")
    return conn, cursor, db_name

# Function to insert data into SQLite
def insert_data(cursor, rows):
    cursor.executemany('''
        INSERT INTO bird_sightings (
            common_name, scientific_name, locality, country, state, county,
            observation_date, year, month, observation_count, breeding_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows)

# Function to parse TXT files and insert into database
def process_txt_file(txt_file, cursor):
    with open(txt_file.strip('"'), 'r', encoding='utf-8') as file:  # Remove quotation marks
        reader = csv.DictReader(file, delimiter='\t')
        rows = []

        for row in reader:
            try:
                # Check if OBSERVATION DATE exists and is not None
                if row['OBSERVATION DATE']:
                    year, month, _ = row['OBSERVATION DATE'].split('-')
                else:
                    continue  # Skip rows with missing date
                
                observation_count = int(row['OBSERVATION COUNT']) if row['OBSERVATION COUNT'].isdigit() else 0
                rows.append(
                    (
                        row['COMMON NAME'],
                        row['SCIENTIFIC NAME'],
                        row['LOCALITY'],
                        row['COUNTRY'],
                        row['STATE'],
                        row['COUNTY'],
                        row['OBSERVATION DATE'],
                        int(year),
                        int(month),
                        observation_count,
                        row.get('BREEDING CATEGORY', 'N/A')
                    )
                )
            except (ValueError, KeyError) as e:
                print(f"Skipping invalid row: {row} due to error: {e}")
                continue  # Skip problematic rows

        insert_data(cursor, rows)

# Function to extract unique bird names and save to CSV
def extract_unique_bird_names(cursor, output_path):
    cursor.execute('''
        SELECT DISTINCT common_name, scientific_name
        FROM bird_sightings
    ''')
    bird_names = cursor.fetchall()
    
    # Generate a unique file name for bird names CSV
    bird_names_file = os.path.join(output_path, f"unique_bird_names_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    with open(bird_names_file, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["COMMON NAME", "SCIENTIFIC NAME"])
        writer.writerows(bird_names)
    print(f"Unique bird names saved to: {bird_names_file}")

# Function to export database to CSV in parts
def export_database_to_csv_in_parts(conn, output_path, rows_per_file=1048576):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bird_sightings")
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    file_count = 1
    output_file = os.path.join(output_path, f"full_database_part_{file_count}.csv")
    with open(output_file, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        row_count = 0
        for row in rows:
            writer.writerow(row)
            row_count += 1
            if row_count >= rows_per_file:
                print(f"Saved {output_file}")
                file_count += 1
                output_file = os.path.join(output_path, f"full_database_part_{file_count}.csv")
                file = open(output_file, 'w', encoding='utf-8', newline='')
                writer = csv.writer(file)
                writer.writerow(headers)
                row_count = 0
    print(f"Saved {output_file}")

# Function to search for a bird and generate analysis
def generate_summary(cursor, bird_name, is_common_name=True, region_type="country", region_filter=None):
    column = 'common_name' if is_common_name else 'scientific_name'

    # Add filtering for region type
    region_query = ""
    params = [bird_name]
    if region_type == "state":
        region_query = "AND state = ?"
        params.append(region_filter)
    elif region_type == "county":
        region_query = "AND county = ?"
        params.append(region_filter)

    # Query observations grouped by year
    cursor.execute(f'''
        SELECT year, SUM(observation_count)
        FROM bird_sightings
        WHERE {column} = ? {region_query}
        GROUP BY year
        ORDER BY year
    ''', tuple(params))
    yearly_data = cursor.fetchall()

    # Calculate % change in observations
    year_changes = []
    for i in range(1, len(yearly_data)):
        year1, obs1 = yearly_data[i - 1]
        year2, obs2 = yearly_data[i]
        percent_change = ((obs2 - obs1) / obs1) * 100 if obs1 > 0 else 0
        year_changes.append((year1, year2, percent_change))

    # Best months for observations
    cursor.execute(f'''
        SELECT month, SUM(observation_count) AS total
        FROM bird_sightings
        WHERE {column} = ? {region_query}
        GROUP BY month
        ORDER BY total DESC
        LIMIT 1
    ''', tuple(params))
    best_month = cursor.fetchone()
    month_name = datetime.strptime(str(best_month[0]), "%m").strftime("%B") if best_month else "N/A"

    # Check for breeding status
    cursor.execute(f'''
        SELECT COUNT(*)
        FROM bird_sightings
        WHERE {column} = ? {region_query} AND breeding_category != ''
    ''', tuple(params))
    breeding_count = cursor.fetchone()[0]
    breeding_status = "Yes" if breeding_count > 0 else "No"

    # Get top locations based on region type
    location_limit = 100 if region_type == "country" else 25 if region_type == "state" else 10
    cursor.execute(f'''
        SELECT locality || ', ' || county || ', ' || state AS location, SUM(observation_count) as total_observations
        FROM bird_sightings
        WHERE {column} = ? {region_query}
        GROUP BY location
        ORDER BY total_observations DESC
        LIMIT {location_limit}
    ''', tuple(params))
    top_locations = cursor.fetchall()

    return yearly_data, year_changes, month_name, breeding_status, top_locations

# Main function
def main():
    # Print program blurb
    print("""
Welcome to the EBird Analysis Program!
This program processes eBird data, providing bird observation analysis, trends, and breeding status.
Warning: Larger datasets may not work due to computer contraints. 
    """)

    txt_file = input("Enter the path to the eBird TXT file: ").strip()
    output_path = create_output_directory(txt_file)
    conn, cursor, db_name = create_database(txt_file, output_path)
    print("Processing file and populating database...")
    process_txt_file(txt_file, cursor)
    conn.commit()

    print("Exporting full database to CSV in parts...")
    export_database_to_csv_in_parts(conn, output_path)
    print("Extracting unique bird names...")
    extract_unique_bird_names(cursor, output_path)

    # Ask user for region type
    region_type = input("Is this dataset for Country, State, or County? (Enter country/state/county): ").strip().lower()
    region_filter = None
    if region_type == "state":
        region_filter = input("Enter the state to analyze: ").strip()
    elif region_type == "county":
        region_filter = input("Enter the county to analyze: ").strip()

    # Loop for analysis
    while True:
        bird_name = input("Enter the common or scientific name of the bird: ").strip()
        search_type = input("Search by (1) Common Name or (2) Scientific Name? Enter 1 or 2: ").strip()
        is_common_name = search_type == '1'

        yearly_data, year_changes, best_month, breeding_status, top_locations = generate_summary(
            cursor, bird_name, is_common_name, region_type, region_filter)

        print(f"\nAnalysis for {bird_name}:")
        print("Yearly Observations:")
        for year, count in yearly_data:
            print(f"{year}: {count} observations")

        print("\nYear-over-Year % Changes:")
        for year1, year2, change in year_changes:
            print(f"{year1} to {year2}: {change:.2f}% change")

        print(f"\nBest Month to Spot: {best_month}")
        print(f"Breeding Status: {breeding_status}")

        print("\nTop Locations (including County and State):")
        for location, total_obs in top_locations:
            print(f"{location}: {total_obs} observations")

        repeat = input("Would you like analysis for a different species? (yes/no): ").strip().lower()
        if repeat != 'yes':
            break

    conn.close()
    print(f"All files have been saved in: {output_path}\nDone!")

if __name__ == "__main__":
    main()
