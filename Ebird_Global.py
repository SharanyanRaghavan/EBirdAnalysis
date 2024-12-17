import os
import csv
import sqlite3
from datetime import datetime
import sys

# Increase field size limit for large rows
csv.field_size_limit(sys.maxsize)

def create_output_directory(txt_file):
    base_name = os.path.splitext(os.path.basename(txt_file))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.getcwd(), f"{base_name}_{timestamp}")
    os.makedirs(output_path, exist_ok=True)
    print(f"Output directory created at: {output_path}")
    return output_path

def create_database(output_path):
    db_name = os.path.join(output_path, "global_bird_analysis.db")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
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
    return conn, cursor

def process_global_data(txt_file, cursor):
    with open(txt_file.strip('"'), 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        rows = []

        for row in reader:
            try:
                year, month, _ = row['OBSERVATION DATE'].split('-')
                observation_count = int(row['OBSERVATION COUNT']) if row['OBSERVATION COUNT'].isdigit() else 0
                rows.append((
                    row['COMMON NAME'], row['SCIENTIFIC NAME'], row['LOCALITY'],
                    row['COUNTRY'], row['STATE'], row['COUNTY'],
                    row['OBSERVATION DATE'], int(year), int(month), observation_count,
                    row.get('BREEDING CATEGORY', '')
                ))
            except (ValueError, KeyError):
                continue  # Skip invalid rows

            if len(rows) >= 10000:
                cursor.executemany('''
                    INSERT INTO bird_sightings (
                        common_name, scientific_name, locality, country, state, county,
                        observation_date, year, month, observation_count, breeding_category
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', rows)
                rows = []

        if rows:
            cursor.executemany('''
                INSERT INTO bird_sightings (
                    common_name, scientific_name, locality, country, state, county,
                    observation_date, year, month, observation_count, breeding_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

def analyze_global_data(cursor, bird_name, filter_type, country_filter=None, state_filter=None, county_filter=None):
    column = 'common_name' if filter_type == "common" else 'scientific_name'

    query = f"WHERE {column} = ?"
    params = [bird_name]

    if country_filter:
        query += " AND country = ?"
        params.append(country_filter)
    if state_filter:
        query += " AND state = ?"
        params.append(state_filter)
    if county_filter:
        query += " AND county = ?"
        params.append(county_filter)

    # Query for top locations
    cursor.execute(f'''
        SELECT locality, state, county, country, SUM(observation_count)
        FROM bird_sightings
        {query}
        GROUP BY locality, state, county, country
        ORDER BY SUM(observation_count) DESC
        LIMIT 100
    ''', tuple(params))
    top_locations = cursor.fetchall()

    # Query for yearly population data
    cursor.execute(f'''
        SELECT year, SUM(observation_count)
        FROM bird_sightings
        {query}
        GROUP BY year
        ORDER BY year
    ''', tuple(params))
    yearly_data = cursor.fetchall()

    # Calculate year-over-year population change
    year_changes = []
    for i in range(1, len(yearly_data)):
        year1, obs1 = yearly_data[i - 1]
        year2, obs2 = yearly_data[i]
        percent_change = ((obs2 - obs1) / obs1) * 100 if obs1 > 0 else 0
        year_changes.append((year1, year2, percent_change))

    # Query for breeding data
    cursor.execute(f'''
        SELECT COUNT(*)
        FROM bird_sightings
        {query} AND breeding_category != ''
    ''', tuple(params))
    breeding_status = "Yes" if cursor.fetchone()[0] > 0 else "No"

    # Query for best month to spot
    cursor.execute(f'''
        SELECT month, SUM(observation_count)
        FROM bird_sightings
        {query}
        GROUP BY month
        ORDER BY SUM(observation_count) DESC
        LIMIT 1
    ''', tuple(params))
    best_month = cursor.fetchone()
    month_name = datetime.strptime(str(best_month[0]), "%m").strftime("%B") if best_month else "N/A"

    return top_locations, yearly_data, year_changes, breeding_status, month_name

def main():
    print("Welcome to the Global Bird Observation Analysis Tool! This tool is used to handle larger datasets, specifically for global data.")
    txt_file = input("Enter the path to the global eBird TXT file: ").strip()
    output_path = create_output_directory(txt_file)
    conn, cursor = create_database(output_path)

    print("Processing data... This may take a while for large datasets.")
    process_global_data(txt_file, cursor)
    conn.commit()

    while True:
        bird_name = input("Enter the bird name (common or scientific): ").strip()
        search_type = input("Search by (1) Common Name or (2) Scientific Name? Enter 1 or 2: ").strip()
        filter_type = "common" if search_type == "1" else "scientific"

        # Ask for country, state, or county filters
        country_filter = input("Enter the country to filter (or press Enter to include all countries): ").strip()
        state_filter = None
        county_filter = None

        if country_filter:
            filter_by_state = input("Would you like to filter by state? (yes/no): ").strip().lower()
            if filter_by_state == "yes":
                state_filter = input("Enter the state: ").strip()
                filter_by_county = input("Would you like to filter further by county? (yes/no): ").strip().lower()
                if filter_by_county == "yes":
                    county_filter = input("Enter the county: ").strip()

        # Get analysis results
        top_locations, yearly_data, year_changes, breeding_status, best_month = analyze_global_data(
            cursor, bird_name, filter_type, country_filter, state_filter, county_filter)

        # Print results
        print(f"\nTop Locations for {bird_name}:")
        print("Locality | State | County | Country | Observations")
        print("-" * 70)
        for locality, state, county, country, count in top_locations:
            print(f"{locality} | {state or 'N/A'} | {county or 'N/A'} | {country} | {count}")

        print("\nYearly Observations:")
        for year, count in yearly_data:
            print(f"{year}: {count} observations")

        print("\nYear-Over-Year % Changes:")
        for year1, year2, change in year_changes:
            print(f"{year1} to {year2}: {change:.2f}% change")

        print(f"\nBreeding Status: {breeding_status}")
        print(f"Best Month to Spot: {best_month}")

        repeat = input("\nWould you like to analyze another bird? (yes/no): ").strip().lower()
        if repeat != "yes":
            break

    conn.close()
    print(f"Analysis complete! Results are stored in: {output_path}")

if __name__ == "__main__":
    main()
