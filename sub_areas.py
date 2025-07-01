import pandas as pd
from sqlalchemy import create_engine

# ✅ Step 4.1: DB config - CHANGE THESE!
DB_CONFIG = {
    "host": "localhost",  # or your IP / domain
    "user": "sami",
    "password": "password",
    "database": "ewanc",
    "port": 3306,
}

# ✅ Step 4.2: SQLAlchemy DB connection
connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(connection_string)

# ✅ Step 4.3: Your CSV file and table name
FILE_PATH = "/Users/samijan/Downloads/sub_areas.csv"  # Change to your file name
TABLE_NAME = "sub_areas"  # Change to your actual DB table name

# ✅ Step 4.4: Seed in CHUNKS to avoid memory crash
CHUNK_SIZE = 1000


def seed_data():
    for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE):
        # Drop any unnamed columns that might exist
        chunk = chunk.loc[:, ~chunk.columns.str.contains("^Unnamed")]

        # Only keep columns that exist in the sub_areas table schema
        # Based on the Laravel migration schema provided
        expected_columns = [
            "LocationId",
            "name_ar",
            "name_en",
            "AbsAr",
            "AbsEn",
            "city_id",
            "Latitude",
            "Longitude",
            "CityType",
            "LKCityParentId",
            "CITYCODE",
            "CENTER_ID",
            "CENTERNAME_AR",
            "CENTERNAME_EN",
            "AMANA_ID",
            "GOVERNORATE_ID",
            "MUNICIPALITY_ID",
            "LOCATION_X",
            "LOCATION_Y",
            "IsActive",
            "DistrictsCount",
            "Polygon_City",
            "Replaced_Governorate_ID",
        ]

        # Map CSV column names to database column names if different
        column_mapping = {
            "nameAr": "name_ar",
            "nameEn": "name_en",
            "LKCityAr": "name_ar",  # fallback mapping
            "LKCityEn": "name_en",  # fallback mapping
            "CityId": "city_id",  # Map CityId from CSV to city_id in database
        }

        # Rename columns if they exist in the chunk
        for csv_col, db_col in column_mapping.items():
            if csv_col in chunk.columns and db_col not in chunk.columns:
                chunk = chunk.rename(columns={csv_col: db_col})

        # Filter chunk to only include existing columns
        available_columns = [col for col in expected_columns if col in chunk.columns]
        chunk = chunk[available_columns]

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

        # city_id will be handled in the numeric_columns loop below

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

        # Handle other numeric columns (based on Laravel migration schema)
        numeric_columns = [
            "IsActive",
            "DistrictsCount",
            "city_id",
            "CENTER_ID",
            "AMANA_ID",
            "GOVERNORATE_ID",
            "MUNICIPALITY_ID",
            "LKCityParentId",
            "Replaced_Governorate_ID",
        ]

        for col in numeric_columns:
            if col in chunk.columns:
                if col == "IsActive":
                    # Handle IsActive as boolean (1/0)
                    chunk[col] = (
                        pd.to_numeric(chunk[col], errors="coerce").fillna(1).astype(int)
                    )
                elif col == "city_id":
                    # Handle city_id as foreign key - ensure it's valid or set to NULL
                    # First convert to numeric, this will turn invalid strings/coords to NaN
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

                    # Only accept valid city IDs (1-13) based on existing cities table
                    valid_city_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
                    chunk[col] = chunk[col].where(chunk[col].isin(valid_city_ids), None)

                    # Convert valid values to int, keeping None as None
                    chunk[col] = chunk[col].apply(
                        lambda x: int(x) if pd.notna(x) else None
                    )
                else:
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)

        # Handle string columns - fill nulls and truncate if needed
        string_columns = [
            "name_ar",
            "name_en",
            "AbsAr",
            "AbsEn",
            "CityType",
            "CENTERNAME_AR",
            "CENTERNAME_EN",
            "Polygon_City",
            "CITYCODE",
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
                elif col in ["CityType", "CENTERNAME_AR", "CENTERNAME_EN"]:
                    chunk[col] = chunk[col].str[:100]  # Limit to 100 chars
                elif col in ["CITYCODE", "LOCATION_X", "LOCATION_Y"]:
                    chunk[col] = chunk[col].str[:50]  # Limit to 50 chars

        chunk.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(chunk)} rows")

    print("🎉 Seeding completed!")


if __name__ == "__main__":
    seed_data()
