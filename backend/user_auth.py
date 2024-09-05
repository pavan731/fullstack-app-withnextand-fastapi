import hashlib
import mysql.connector
import os
import jwt
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

class UserAuth:
    def __init__(self, host=None, user=None, password=None, database=None):
        self.conn = mysql.connector.connect(
            host=host or os.getenv("DB_HOST", "127.0.0.1"),
            user=user or os.getenv("DB_USER", "root"),
            password=password or os.getenv("DB_PASSWORD", "Pavan123"),
            database=database or os.getenv("DB_NAME", "parts_analytics")
        )
        self.cursor = self.conn.cursor(dictionary=True)

    def hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def register(self, email: str, password: str):
        hashed_password = self.hash_password(password)
        
        try:
            self.cursor.execute("SELECT * FROM Users_Credentials WHERE email = %s", (email,))
            existing_user = self.cursor.fetchone()
            
            if existing_user:
                return False, "Email already registered. Please login."
            
            self.cursor.execute(
                "INSERT INTO Users_Credentials (email, password) VALUES (%s, %s)",
                (email, hashed_password)
            )
            self.conn.commit()
            return True, "Registration successful!"
        except mysql.connector.Error as err:
            return False, f"Error: {err}"

    def login(self, email: str, password: str) -> bool:
        hashed_password = self.hash_password(password)
        
        try:
            self.cursor.execute("SELECT * FROM Users_Credentials WHERE email = %s", (email,))
            user = self.cursor.fetchone()
            
            if user and user['password'] == hashed_password:
                return True
            else:
                return False
        except mysql.connector.Error as err:
            return False

    def get_user_by_email(self, email: str):
        try:
            self.cursor.execute("SELECT * FROM Users_Credentials WHERE email = %s", (email,))
            user = self.cursor.fetchone()
            return user
        except mysql.connector.Error as err:
            return None

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class UserAuthFactory:
    @staticmethod
    def create_user_auth() -> UserAuth:
        return UserAuth()

class SimpleJWTFactory:
    @staticmethod
    def create_token(user_id: str, secret_key: str, expires_in: int = 3600) -> str:
        payload = {
            'user_id': user_id,
            'exp': datetime.utcnow() + timedelta(seconds=expires_in)
        }
        token = jwt.encode(payload, secret_key, algorithm='HS256')
        return token

    @staticmethod
    def decode_token(token: str, secret_key: str):
        try:
            payload = jwt.decode(token, secret_key, algorithms=['HS256'])
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None

class SessionManagerFactory:
    # Placeholder for session management logic
    @staticmethod
    def create_session(email: str, session_data: dict):
        # Implement session creation logic
        return True

    @staticmethod
    def destroy_session(session_id: str):
        # Implement session destruction logic
        return True
