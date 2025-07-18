import pandas as pd
from sqlalchemy import create_engine, text

# ✅ Step 4.1: DB config - CHANGE THESE!
DB_CONFIG = {
    "host": "localhost",  # or your IP / domain
    "user": "sami",
    "password": "password",
    "database": "ewanc",
    "port": 3306  # default MySQL port
}

# ✅ Step 4.2: SQLAlchemy DB connection
connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(connection_string)

# ✅ Step 4.3: Your CSV file and table name
FILE_PATH = "/Users/samijan/Downloads/neighbourhoods.csv"  # District dataset file
TABLE_NAME = "neighbourhoods"  # Change to your actual DB table name

# ✅ Step 4.4: Seed in CHUNKS to avoid memory crash
CHUNK_SIZE = 1000

# Get valid sub_area_ids from database
def get_valid_sub_area_ids():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id FROM sub_areas"))
        return set(row[0] for row in result.fetchall())

def seed_data():
    # Get valid sub_area_ids once before processing chunks
    valid_sub_area_ids = get_valid_sub_area_ids()
    print(f"Found {len(valid_sub_area_ids)} valid sub_area_ids")

    for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE):
        # Drop any unnamed columns that might exist
        chunk = chunk.loc[:, ~chunk.columns.str.contains("^Unnamed")]

        # Only keep columns that exist in the neighbourhoods table schema
        # Based on the Laravel migration schema provided
        expected_columns = [
            "sub_area_id",
            "city_id",
            "name_en",
            "name_ar",
            "LKDistrictId",
            "LocationId",
            "AbsAr",
            "AbsEn",
            "LKCityId",
            "Min_X",
            "Min_Y",
            "Max_X",
            "Max_Y",
            "Longitude",
            "Latitude",
            "DISTRICT_ID",
            "District_ID_MOMRAH",
            "LKDistrictId_MergeWith",
            "DISTRICT_ID_PREV",
            "DISTRICT_ID_New",
            "SECTOR_ID",
            "AMANA_ID",
            "GOVERNORATE_ID",
            "MUNICIPALITY_ID",
            "GLOBALID",
            "AMANABALADI",
            "MUNICIPALITYBALADI",
            "DISTRICTBALADI",
            "MOMRAH_ObjectID",
            "ManualUpdate",
            "REGION_ID_NHC",
            "GOVERNORATE_ID_NHC",
            "TimeInsert",
            "TimeUpdate",
            "TimeDelete",
            "IsDeleted",
            "RowVer",
            "LOCATION_X",
            "LOCATION_Y",
        ]

        # Map CSV column names to database column names if different
        column_mapping = {
            "nameAr": "name_ar",
            "nameEn": "name_en",
            "CityId": "city_id",  # Map CityId from CSV to city_id in database
            # sub_area_id column already exists and correctly named in CSV
        }

        # Rename columns if they exist in the chunk
        for csv_col, db_col in column_mapping.items():
            if csv_col in chunk.columns and db_col not in chunk.columns:
                chunk = chunk.rename(columns={csv_col: db_col})

        # Handle sub_area_id - ensure it exists in sub_areas table
        if "sub_area_id" in chunk.columns:
            chunk["sub_area_id"] = pd.to_numeric(chunk["sub_area_id"], errors="coerce")
            # Only keep rows where sub_area_id exists in sub_areas table
            chunk = chunk[chunk["sub_area_id"].isin(valid_sub_area_ids)]
            if len(chunk) == 0:
                print("⚠️ No valid sub_area_ids in this chunk, skipping...")
                continue

        # Handle city_id directly from the CSV (mapped from CityId)
        if "city_id" in chunk.columns:
            chunk["city_id"] = pd.to_numeric(chunk["city_id"], errors="coerce")
            chunk["city_id"] = chunk["city_id"].fillna(0).astype(int)

        # Filter chunk to only include existing columns
        available_columns = [col for col in expected_columns if col in chunk.columns]

        # Ensure sub_area_id is included if it exists in CSV
        if "sub_area_id" in chunk.columns and "sub_area_id" not in available_columns:
            available_columns.append("sub_area_id")

        # Ensure city_id is available for population
        if "city_id" not in available_columns:
            available_columns.append("city_id")

        chunk = chunk.reindex(columns=available_columns)

        # Clean and fix data types

        # Handle LocationId - convert to integer for unsignedBigInteger column
        if "LocationId" in chunk.columns:
            # Convert to numeric, handle 'nan' and decimal points
            chunk["LocationId"] = (
                chunk["LocationId"].astype(str).str.replace(".0", "", regex=False)
            )
            chunk["LocationId"] = (
                pd.to_numeric(chunk["LocationId"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

        # Handle LKRegionId - convert non-numeric values to 0
        if "LKRegionId" in chunk.columns:
            chunk["LKRegionId"] = pd.to_numeric(
                chunk["LKRegionId"], errors="coerce"
            ).fillna(0)

        # Handle latitude/longitude with valid range constraints
        if "Latitude" in chunk.columns:
            chunk["Latitude"] = pd.to_numeric(
                chunk["Latitude"], errors="coerce"
            ).fillna(0)
            # Keep latitude in valid range (-90 to 90)
            chunk["Latitude"] = chunk["Latitude"].clip(-90, 90)

        if "Longitude" in chunk.columns:
            chunk["Longitude"] = pd.to_numeric(
                chunk["Longitude"], errors="coerce"
            ).fillna(0)
            # Keep longitude in valid range (-180 to 180)
            chunk["Longitude"] = chunk["Longitude"].clip(-180, 180)

        if "LOCATION_X" in chunk.columns:
            chunk["LOCATION_X"] = pd.to_numeric(
                chunk["LOCATION_X"], errors="coerce"
            ).fillna(0)
            chunk["LOCATION_X"] = chunk["LOCATION_X"].clip(-180, 180)

        if "LOCATION_Y" in chunk.columns:
            chunk["LOCATION_Y"] = pd.to_numeric(
                chunk["LOCATION_Y"], errors="coerce"
            ).fillna(0)
            chunk["LOCATION_Y"] = chunk["LOCATION_Y"].clip(-90, 90)

        # Handle boolean columns
        if "ManualUpdate" in chunk.columns:
            chunk["ManualUpdate"] = (
                pd.to_numeric(chunk["ManualUpdate"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

        if "IsDeleted" in chunk.columns:
            chunk["IsDeleted"] = (
                pd.to_numeric(chunk["IsDeleted"], errors="coerce").fillna(0).astype(int)
            )

        # Handle foreign key references - set to NULL to avoid constraint violations
        foreign_key_columns = [
            "LKDistrictId_MergeWith",
            "REGION_ID_NHC",
            "GOVERNORATE_ID_NHC",
        ]
        for col in foreign_key_columns:
            if col in chunk.columns:
                chunk[col] = None  # Set to NULL to avoid FK constraint issues

        # Handle coordinate ranges
        coordinate_columns = ["Min_X", "Max_X", "Min_Y", "Max_Y"]
        for col in coordinate_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)
                if "X" in col:  # Longitude
                    chunk[col] = chunk[col].clip(-180, 180)
                else:  # Latitude
                    chunk[col] = chunk[col].clip(-90, 90)

        # Handle foreign key columns with proper validation
        if "sub_area_id" in chunk.columns:
            # Handle sub_area_id as foreign key to sub_areas table
            chunk["sub_area_id"] = pd.to_numeric(chunk["sub_area_id"], errors="coerce")
            chunk["sub_area_id"] = chunk["sub_area_id"].fillna(0).astype(int)  # Convert NaN to 0

        # Handle other numeric columns that might exist in neighbourhoods (excluding foreign keys)
        numeric_columns = [
            "DISTRICT_ID",
            "DISTRICT_ID_PREV",
            "DISTRICT_ID_New",
            "SECTOR_ID",
            "MUNICIPALITY_ID",
            "MOMRAH_ObjectID",
            "RowVer",
        ]

        for col in numeric_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)

        # Handle timestamp/date columns - set them to NULL for MySQL
        time_columns = ["TimeInsert", "TimeUpdate", "TimeDelete"]
        for col in time_columns:
            if col in chunk.columns:
                # Set all timestamp columns to None (NULL in database)
                chunk[col] = None

        # Handle ID columns that should be numbers but come as strings
        id_columns = ["AMANA_ID", "GOVERNORATE_ID"]
        for col in id_columns:
            if col in chunk.columns:
                # Convert to numeric, treat empty strings and non-numeric as 0
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)

        # Handle string columns - fill nulls and truncate if needed
        string_columns = [
            "name_ar",
            "name_en",
            "AbsAr",
            "AbsEn",
            "GLOBALID",
            "District_ID_MOMRAH",
            "AMANABALADI",
            "MUNICIPALITYBALADI",
            "DISTRICTBALADI",
            "LOCATION_X",
            "LOCATION_Y",
        ]

        for col in string_columns:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).fillna("")
                # Replace 'nan' string with empty string
                chunk[col] = chunk[col].replace("nan", "")
                # Truncate long strings to prevent data too long errors
                if col in ["name_ar", "name_en", "AbsAr", "AbsEn"]:
                    chunk[col] = chunk[col].str[:255]  # Limit to 255 chars
                elif col in ["GLOBALID", "District_ID_MOMRAH"]:
                    chunk[col] = chunk[col].str[:100]  # Limit to 100 chars
                elif col in [
                    "AMANABALADI",
                    "MUNICIPALITYBALADI",
                    "DISTRICTBALADI",
                    "LOCATION_X",
                    "LOCATION_Y",
                ]:
                    chunk[col] = chunk[col].str[:50]  # Limit to 50 chars

        if len(chunk) > 0:
            chunk.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
            print(f"✅ Inserted {len(chunk)} rows")

    print("🎉 District seeding completed!")

def check_data():
    with engine.connect() as conn:
        # Check neighbourhoods count
        result = conn.execute(text("SELECT COUNT(*) FROM neighbourhoods"))
        count = result.scalar()
        print(f"\n✅ neighbourhoods table has {count:,} rows")
        
        # Get sample of recent entries
        result = conn.execute(text("SELECT id, name_ar, name_en, sub_area_id, city_id FROM neighbourhoods ORDER BY id DESC LIMIT 3"))
        rows = result.fetchall()
        print("\n📊 Recent entries:")
        for row in rows:
            print(f"ID: {row[0]}, Name AR: {row[1]}, Name EN: {row[2]}, Sub Area ID: {row[3]}, City ID: {row[4]}")

if __name__ == "__main__":
    seed_data()
    check_data()
