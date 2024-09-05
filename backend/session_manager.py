
# import mysql.connector
# import os
# from datetime import datetime

# class SessionManager:
#     def __init__(self, host="127.0.0.1", user="root", password="Pavan123", database="parts_analytics"):
#         # Initialize MySQL connection
#         self.conn = mysql.connector.connect(
#             host=host,
#             user=user,
#             password=password,
#             database=database
#         )
#         self.cursor = self.conn.cursor(dictionary=True)

#     def create_session(self, username: str, access_token: str):
#         session_data = {
#             "email": username,
#             "token": access_token,
#             "login_time": datetime.now()
#         }
        
#         query = "INSERT INTO Sessions (email, token, login_time) VALUES (%s, %s, %s)"
#         values = (session_data['email'], session_data['token'], session_data['login_time'])

#         self.cursor.execute(query, values)
#         self.conn.commit()

#     def get_latest_session(self, email: str):
#         query = "SELECT * FROM Sessions WHERE email = %s ORDER BY login_time DESC LIMIT 1"
#         self.cursor.execute(query, (email,))
#         result = self.cursor.fetchone()
        
#         if result:
#             return result, result['id']
#         return None

#     def update_session(self, session_id: str, update_data: dict):
#         set_clause = ", ".join([f"{key} = %s" for key in update_data])
#         values = list(update_data.values())
#         values.append(session_id)

#         query = f"UPDATE Sessions SET {set_clause} WHERE id = %s"
#         self.cursor.execute(query, values)
#         self.conn.commit()

#     def end_session(self, session_id: str):
#         self.update_session(session_id, {"logout_time": datetime.now()})


        

#     def __del__(self):
#         # Close the connection when the instance is destroyed
#         self.cursor.close()
#         self.conn.close()

import mysql.connector
from datetime import datetime, timedelta

class SessionManager:
    def __init__(self, host="127.0.0.1", user="root", password="Pavan123", database="parts_analytics"):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor(dictionary=True)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            raise

    def create_session(self, username: str, access_token: str):
        session_data = {
            "email": username,
            "token": access_token,
            "login_time": datetime.now()
        }
        
        query = "INSERT INTO Sessions (email, token, login_time) VALUES (%s, %s, %s)"
        values = (session_data['email'], session_data['token'], session_data['login_time'])

        try:
            self.cursor.execute(query, values)
            self.conn.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn.rollback()

    def get_latest_session(self, email: str):
        query = "SELECT * FROM Sessions WHERE email = %s ORDER BY login_time DESC LIMIT 1"
        try:
            self.cursor.execute(query, (email,))
            result = self.cursor.fetchone()
            if result:
                return result, result['id']
            return None
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def is_session_active(self, email: str, token: str) -> bool:
        session, session_id = self.get_latest_session(email)
        if session and session['token'] == token and not self.is_session_expired(session):
            return True
        return False

    def is_session_expired(self, session) -> bool:
        expiration_time = session['login_time'] + timedelta(hours=1)
        return datetime.now() > expiration_time

    def update_session(self, session_id: str, update_data: dict):
        set_clause = ", ".join([f"{key} = %s" for key in update_data])
        values = list(update_data.values())
        values.append(session_id)

        query = f"UPDATE Sessions SET {set_clause} WHERE id = %s"
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn.rollback()

    def end_session(self, session_id: str):
        self.update_session(session_id, {"logout_time": datetime.now()})

    def __del__(self):
        # Ensure resources are properly closed
        if hasattr(self, 'cursor') and self.cursor is not None:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()
