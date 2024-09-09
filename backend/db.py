from fastapi import Depends, HTTPException
from starlette.status import HTTP_401_UNAUTHORIZED
from fastapi.security import OAuth2PasswordBearer
import mysql.connector
from mysql.connector import Error
import pandas as pd
from jose import JWTError, jwt
from datetime import datetime, timedelta

# Database configuration
db_config = {
    "host": "127.0.0.1",  # Replace with your MySQL host
    "user": "root",  # Replace with your MySQL username
    "password": "Pavan123",  # Replace with your MySQL password
    "database": "parts_analytics",  # Replace with your MySQL database name
}

# JWT configuration
SECRET_KEY = "key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def filters(df, month, year):
    if month and year:
        df = df[(df["Month Name"] == month) & (df["Year"] == str(year))]
    elif month:
        df = df[df["Month Name"] == month]
    elif year:
        df = df[df["Year"] == str(year)]
    else:
        if not df.empty:
            first_month = df["Month Name"].iloc[0]
            first_year = df["Year"].iloc[0]
            df = df[(df["Month Name"] == first_month) & (df["Year"] == str(first_year))]
    return df

async def get_pvpm(token: str = Depends(oauth2_scheme)):
    pvpm = await get_db_connection("PVPM")
    pvpm.fillna(0, inplace=True)
    pvpm = pvpm.applymap(lambda x: int(x.replace(',', '')) if isinstance(x, str) else x)
    pvpm = pvpm.set_index("AGE")
    return pvpm

async def get_utilization(month, year, token: str = Depends(oauth2_scheme)):
    chp = await get_db_connection("chassis_vbd_application")
    util = await get_db_connection("utilization")
    
    util["Monthq"] = pd.to_datetime(util["Monthq"], format="%b-%y").dt.strftime('%b-%Y')
    util[["Month Name", 'Year']] = util['Monthq'].str.split('-', expand=True)

    util = filters(util, month, year)
    site_cust = await get_db_connection("site_id_customer_id")
    data = pd.merge(chp, util, on=["Chassis Number"], how="right").drop_duplicates()
    
    data = data[(data["AGE"] <= 10) & (data["Active Status"] == 'Active')]
    util = pd.merge(site_cust, data, on=['Site Code'], how='right')
    util.dropna(subset=["Utilization %"], inplace=True)
    util["Utilization %"] = util["Utilization %"].apply(lambda x: int(x.replace("%", "")))
    return util

async def get_retail(month, year, token: str = Depends(oauth2_scheme)):
    retail = await get_db_connection("retail", {"month": month, "year": year})
    retail["Invoice Date"] = pd.to_datetime(retail["Invoice Date"], format="%d-%m-%Y")
    retail["Month Year"] = retail["Invoice Date"].apply(lambda x: x.strftime('%b-%Y'))
    retail[["Month Name", 'Year']] = retail['Month Year'].str.split('-', expand=True)
    retail = filters(retail, month, year)
    return retail

async def get_running_hrs(token: str = Depends(oauth2_scheme)):
    running_hrs = await get_db_connection("running_hours")
    running_hrs.set_index("Application", inplace=True)
    return running_hrs

async def get_db_connection(table_name, params=None):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            try:
                if table_name == "retail":
                    month, year = params["month"], params["year"]
                    query = f"SELECT * FROM {table_name} WHERE month_name = %s AND year = %s"
                    cursor.execute(query, (month, year))
                else:
                    query = f"SELECT * FROM {table_name}"
                    cursor.execute(query)
                rows = cursor.fetchall()
                column_names = [i[0] for i in cursor.description]
                df = pd.DataFrame(rows, columns=column_names)
                return df
            finally:
                cursor.close()
                connection.close()
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username