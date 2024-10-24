import logging
from fastapi import FastAPI, Query, File, UploadFile
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi import Request, Depends, HTTPException, status, Form
from fastapi.templating import Jinja2Templates
from datetime import timedelta, datetime
from jose import JWTError, jwt
from backend.user_auth import UserAuthFactory
from backend.session_factory import SessionManagerFactory
from fastapi.staticfiles import StaticFiles
from typing import Optional
from pydantic import BaseModel
import pandas as pd

from backend.Parts_penetration import truck_vehicle_population, Segmentwise, utilization, Gross_sale, parts_penetration
from backend.db import get_utilization, get_pvpm, get_retail, get_running_hrs,get_filters
from backend.db_upload import handle_upload
import uvicorn

app = FastAPI()

class SimpleJWTFactory:
    def __init__(self, secret_key: str, algorithm: str, access_token_expire_minutes: int):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.access_token_expire_minutes = access_token_expire_minutes

    def create_token(self, user_id: str):
        expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        to_encode = {"sub": user_id, "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt

    def verify_access_token(self, token: str):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

# CORS Middleware
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    # Add any other front-end origins that need to access the API
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JWT Factory instance
SECRET_KEY = "hey"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5

jwt_factory = SimpleJWTFactory(secret_key=SECRET_KEY, algorithm=ALGORITHM, access_token_expire_minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

class User(BaseModel):
    email: str
    hashed_password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[str] = None

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Verify the JWT token
        payload = jwt_factory.verify_access_token(token)
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    # Create session manager
    session_manager = SessionManagerFactory().create_session_manager()
    print(email)
    # Check if the session is valid
    if not session_manager.is_session_active(email, token):
        raise credentials_exception

    # Fetch the user information
    user_auth = UserAuthFactory().create_user_auth()
    user = user_auth.get_user_by_email(email)
    print(user)
    if user is None:
        raise credentials_exception
    
    return user


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/register", response_class=HTMLResponse)
async def register(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    user_auth = UserAuthFactory().create_user_auth()
    if not user_auth.login(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = jwt_factory.create_token(form_data.username)

    session_manager = SessionManagerFactory().create_session_manager()
    session_manager.create_session(username=form_data.username, access_token=access_token)

    return {"message": "Login successful", "token": access_token}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")



@app.get("/check-auth")
async def check_auth(email: str = Query(...), access_token: str = Query(...)):
    session_manager = SessionManagerFactory().create_session_manager()
    print(email,access_token)
    if session_manager.is_session_active(email, access_token):
        return {"message": "Session is active"}
    raise HTTPException(status_code=401, detail="Session is inactive or expired")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/register")
async def register_user(email: str = Form(...), password: str = Form(...)):
    logger.info(f"Received email: {email}")
    logger.info(f"Received password: {password}")
    
    user_auth = UserAuthFactory().create_user_auth()
    success, message = user_auth.register(email, password)
    if not success:
        logger.error(f"Registration failed: {message}")
        raise HTTPException(status_code=400, detail=message)
    logger.info(f"Registration successful: {message}")
    return {"msg": message}


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user_auth = UserAuthFactory().create_user_auth()
    if not user_auth.login(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = jwt_factory.create_token(form_data.username)

    session_manager = SessionManagerFactory()
    session_manager.create_session(email=form_data.username, session_data=access_token)
    
    return {"access_token": access_token, "token_type": "bearer"}

class FilterModel(BaseModel):
    month: Optional[str] = None
    year: Optional[int] = None
    other_filter: Optional[str] = None

# Separate endpoints for each table
@app.post("/upload_chassis_app/")
async def upload_chassis(file: UploadFile = File(...)):
    return await handle_upload("chassis_vbd_application", file)

@app.post("/upload_pvpm/")
async def upload_pvpm(file: UploadFile = File(...)):
    return await handle_upload("pvpm", file)

@app.post("/upload_running_hours/")
async def upload_running_hours(file: UploadFile = File(...)):
    return await handle_upload("running_hours", file)

@app.post("/upload_site_cust/")
async def upload_site_cust(file: UploadFile = File(...)):
    return await handle_upload("site_id_customer_id", file)

@app.post("/upload_retail/")
async def upload_retail(file: UploadFile = File(...)):
    return await handle_upload("retail", file)

@app.post("/upload_utilization/")
async def upload_utilization(file: UploadFile = File(...)):
    return await handle_upload("utilization", file)

@app.get("/filters")
async def retrieve_filters():  

    retail =await get_filters()
    print(retail)
    # return {"Month": util["Month Name"].unique(),"Year" :util["Year"].unique(), "Part_Code":retail["Product Code"].unique()}
    return retail
@app.get("/truck_vehicle_population")
async def retrieve_tvp(month: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    data = await get_utilization(month, year)
    pvpm = await get_pvpm()
    dataframes = truck_vehicle_population(data, pvpm).reset_index().rename(columns={"index": "AGE"})
    dataframes_json = dataframes.to_dict(orient="records")
    return {"truck_vehicle_population": dataframes_json}

@app.get("/pvpm")
async def retrieve_pvpm():
    pvpm = await get_pvpm()
    dataframes_json = pvpm.reset_index().rename(columns={"index": "AGE"}).to_dict(orient="records")
    return {"pvpm": dataframes_json}

@app.get("/segmentwise_potential")
async def retrieve_segmentwise_potential(month: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    data = await get_utilization(month, year)
    pvpm = await get_pvpm()
    vehicle_pop = truck_vehicle_population(data, pvpm)
    dataframes = Segmentwise(vehicle_pop, pvpm)
    dataframes_json = dataframes.reset_index().rename(columns={"index": "AGE"}).to_dict(orient="records")
    return {"segmentwise_potential": dataframes_json}

@app.get("/utilization_per_month")
async def retrieve_utilization_per_month(month: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    data = await get_utilization(month, year)
    dataframes = utilization(data)
    dataframes_json = dataframes.T.to_dict(orient="records")
    return {"utilization_per_month": dataframes_json}

@app.get("/gross_sale")
async def retrieve_gross_sale(month: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    data = await get_retail(month, year)
    dataframe = Gross_sale(data)
    dataframes_json = dataframe.to_dict(orient="records")
    return {"gross_sale": dataframes_json}

@app.get("/pp")
async def retrieve_pp(month: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    retail = await get_retail(month, year)
    pvpm = await get_pvpm()
    data = await get_utilization(month, year)
    running = await get_running_hrs()
    
    dataframe,potential_sale,sum_of_gross_sale, pp = parts_penetration(retail, pvpm, data, running)
    dataframes_json = dataframe.to_dict(orient="records")
    
    return {
        "pp_table": dataframes_json, 
        "sum_of_gross_sale": sum_of_gross_sale, 
        "potential_sale": potential_sale, 
        "pp_percentage": pp
        }
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
