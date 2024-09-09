from fastapi import FastAPI, File, UploadFile, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd
import io

app = FastAPI()

# Database setup
DATABASE_URL = "mysql+pymysql://root:Pavan123@127.0.0.1:3306/parts_analytics"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# Function to handle CSV uploads
async def handle_upload(table_name: str, file: UploadFile):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    try:
        content = await file.read()
        try:
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        except UnicodeDecodeError:
            df = pd.read_csv(io.StringIO(content.decode('Windows-1252')))
        df.columns = df.columns.str.strip()

        inspector = inspect(engine)
        metadata.clear()

        if inspector.has_table(table_name):
            # If the table exists, read existing data from the table
            existing_df = pd.read_sql_table(table_name, engine)

            # Append the new data to the existing data
            combined_df = pd.concat([existing_df, df])

            # Remove duplicates
            combined_df.drop_duplicates(inplace=True)

            # Write the deduplicated data back to the table
            combined_df.to_sql(table_name, engine, if_exists='replace', index=False)
        else:
            # Dynamically create a table structure based on the DataFrame
            columns = []
            for column_name, dtype in df.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    column_type = Integer
                elif pd.api.types.is_float_dtype(dtype):
                    column_type = Float
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    column_type = DateTime
                else:
                    column_type = String(255)
                columns.append(Column(column_name, column_type))

            # Define the table with the dynamically created columns
            table = Table(
                table_name,
                metadata,
                *columns,
                extend_existing=True,
            )

            # Create the table in the database
            metadata.create_all(engine)

            # Insert the DataFrame rows into the table
            df.to_sql(table_name, engine, if_exists='append', index=False)

        return {"message": f"CSV data successfully uploaded and stored in the table '{table_name}'."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Separate endpoints for each table
@app.post("/chassis_app/")
async def upload_table1(file: UploadFile = File(...)):
    return await handle_upload("chassis_vbd_application", file)

@app.post("/pvpm/")
async def upload_table2(file: UploadFile = File(...)):
    return await handle_upload("pvpm", file)

@app.post("/running_hours/")
async def upload_table3(file: UploadFile = File(...)):
    return await handle_upload("running_hours", file)

@app.post("/site_cust/")
async def upload_table4(file: UploadFile = File(...)):
    return await handle_upload("site_id_customer_id", file)

@app.post("/retail/")
async def upload_table5(file: UploadFile = File(...)):
    return await handle_upload("retail", file)

@app.post("/utilization/")
async def upload_table6(file: UploadFile = File(...)):
    return await handle_upload("utilization", file)
