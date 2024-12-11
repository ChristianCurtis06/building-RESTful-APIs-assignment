# Managing a Fitness Center Database
# Task 1: Setting Up the Flask Environment and Database Connection
from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
ma = Marshmallow(app)
# Defining a function to connect the python script to the MySQL database 'fitness_center_db'
def get_db_connection():
    db_name = "fitness_center_db"
    user = "root"
    password = "Ago0#1!$"
    host = "127.0.0.1"

    try:
        conn = mysql.connector.connect(
            database=db_name,
            user=user,
            password=password,
            host=host
        )

        print("Connector to MySQL database successfully")
        return conn
    except Error as e:
        print(f"Error: {e}")
        return None

# Task 2: Implementing CRUD Operations for Members
class MemberSchema(ma.Schema):
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    age = fields.Integer(required=True)

member_schema = MemberSchema()
members_schema = MemberSchema(many=True)

class WorkoutSessionSchema(ma.Schema):
    session_id = fields.Integer(required=True)
    member_id = fields.Integer(required=True)
    session_date = fields.Date(required=True)
    session_time = fields.String(required=True)
    activity = fields.String(required=True)
    member_name = fields.String()

workout_session_schema = WorkoutSessionSchema()
workout_sessions_schema = WorkoutSessionSchema(many=True)

# Defining functions to retrieve, add, update, and delete members from 'fitness_center_db'
@app.route('/')
def home():
    return 'Welcome to the Fitness Center Management System!'

@app.route("/members/<int:id>", methods=["GET"])
def get_member(id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM Members WHERE id = %s"

        cursor.execute(query, (id,))

        member = cursor.fetchone()

        return member_schema.jsonify(member)
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/members", methods=["POST"])
def add_member():
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        new_member = (member_data['id'], member_data['name'], member_data['age'])

        query = "INSERT INTO Members (id, name, age) VALUES (%s, %s, %s)"

        cursor.execute(query, new_member)
        conn.commit()

        return jsonify({"message": "New member added successfully"}), 201   
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/members/<int:id>", methods=["PUT"])
def update_member(id):
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        updated_member = (member_data['name'], member_data['age'], id)

        query = "UPDATE Members SET name = %s, age = %s WHERE id = %s"

        cursor.execute(query, updated_member)
        conn.commit()

        return jsonify({"message": "Updated member successfully"}), 201   
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/members/<int:id>", methods=["DELETE"])
def delete_member(id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        member_to_remove = (id,)

        cursor.execute("SELECT * FROM Members WHERE id = %s", member_to_remove)
        member = cursor.fetchone()
        if not member:
            return jsonify({"error": "Member not found"}), 404
        
        cursor.execute("SELECT * FROM WorkoutSessions WHERE member_id = %s", member_to_remove)
        workout_sessions = cursor.fetchall()
        if workout_sessions:
            query = "DELETE FROM WorkoutSessions WHERE member_id = %s"
            cursor.execute(query, member_to_remove)
            conn.commit()
        
        query = "DELETE FROM Members WHERE id = %s"
        cursor.execute(query, member_to_remove)
        conn.commit()

        return jsonify({"message": "Member removed successfully"}), 200
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Task 3: Managing Workout Sessions
# Defining functions to retrieve, add, update, and filter (by member) workout sessions from 'fitness_center_db'
@app.route("/workout-sessions", methods=["GET"])
def get_workout_sessions():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True)

        query = "SELECT W.session_id, W.session_date, W.session_time, W.activity, M.name AS member_name FROM WorkoutSessions W, Members M WHERE W.member_id = M.id"

        cursor.execute(query)

        workout_sessions = cursor.fetchall()

        return workout_sessions_schema.jsonify(workout_sessions)
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/workout-sessions", methods=["POST"])
def add_workout_session():
    try:
        workout_session_data = workout_session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        new_workout_session = (workout_session_data['session_id'], workout_session_data['member_id'], workout_session_data['session_date'], workout_session_data['session_time'], workout_session_data['activity'])

        query = "INSERT INTO WorkoutSessions (session_id, member_id, session_date, session_time, activity) VALUES (%s, %s, %s, %s, %s)"

        cursor.execute(query, new_workout_session)
        conn.commit()

        return jsonify({"message": "New workout session added successfully"}), 201   
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/workout-sessions/<int:id>", methods=["PUT"])
def update_workout_session(id):
    try:
        workout_session_data = workout_session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        updated_workout_session = (workout_session_data['member_id'], workout_session_data['session_date'], workout_session_data['session_time'], workout_session_data['activity'], id)

        query = "UPDATE WorkoutSessions SET member_id = %s, session_date = %s, session_time = %s, activity = %s WHERE session_id = %s"

        cursor.execute(query, updated_workout_session)
        conn.commit()

        return jsonify({"message": "Updated workout session successfully"}), 201   
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/workout-sessions-by-member', methods=['POST'])
def workout_sessions_by_member():
    member = request.args.get('member')

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT W.session_id, W.session_date, W.session_time, W.activity, M.name as member_name
            FROM WorkoutSessions W, Members M
            WHERE W.member_id = M.id AND M.name = %s
            """, (member,)
        )
        workout_sessions = cursor.fetchall()
        return workout_sessions_schema.jsonify(workout_sessions)
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()