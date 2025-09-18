from flask import Blueprint, render_template, request, redirect, url_for, flash, session
import pymysql
from utils import get_db_connection
import secrets
import time
import json
from datetime import datetime, timedelta
# Blueprint
user_auth = Blueprint("user_auth", __name__)
# Hardcoded admin credentials for when database is empty
HARDCODED_ADMIN_USERNAME = "admin"
HARDCODED_ADMIN_PASSWORD = "admin123"
# Define Roles and Factories according to requirements
ROLES = [
    "Admin", 
    "Corporate Skill Development Head", 
    "Plant Head", 
    "HR Head", 
    "ER Officer",
    "Factory Head", 
    "Skill Head", 
    "PSD Officer",  # Changed from PSD
    "PSD Head",  # Now a non-factory role
    "Shop Floor Training Coordinators"
   
]
FACTORY_LOCATIONS = [
    'AXLE FACTORY', 'CCE-TS', 'CHINCHWAD FOUNDRY', 'CKD-SKD', 'CMS', 
    'ENGINE FACTORY', 'ERC', 'GEAR FACTORY', 'HR FUNCTION', 
    'J-11/12 PAINTSHOP', 'LAUNCH MANAGEMENT', 'LCV FACTORY', 
    'M&HCV FACTORY', 'MATERIAL AUDIT', 'MAVAL FOUNDRY', 'PE', 'PPC', 
    'PRESS & FRAME FACTORY', 'PTPA', 'QUALITY ASSURANCE', 'SCM', 
    'Service Technical Support', 'WINGER FACTORY', 'XENON FACTORY'
]
# Function to check if database is empty
def is_database_empty():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM user_auth")
    result = cursor.fetchone()
    count = result['count']  # Access by key name instead of index
    conn.close()
    return count == 0
# Function to initialize database with all users
def initialize_users():
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # Default password for all users
    default_password = "password123"
    
    # Create users for each role (except factory-specific roles)
    for role in ROLES:
        if role not in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"]:  # Updated to PSD Officer
            # Generate username: tmcv-{role_name}
            username = f"tmcv-{role.replace(' ', '').lower()}"
            
            # Check if user already exists
            cursor.execute("SELECT * FROM user_auth WHERE username = %s", (username,))
            if cursor.fetchone() is None:
                # Insert new user with plain text password
                cursor.execute(
                    "INSERT INTO user_auth (username, password, role, factory_location) VALUES (%s, %s, %s, %s)",
                    (username, default_password, role, None)
                )
                conn.commit()
                print(f"Created user: {username}")
    
    # Create Factory Head users for each factory
    for factory in FACTORY_LOCATIONS:
        # Generate username: tmcv-{factory_name}-head
        factory_name = factory.replace(' ', '').lower()
        username = f"tmcv-{factory_name}-head"
        
        # Check if user already exists
        cursor.execute("SELECT * FROM user_auth WHERE username = %s", (username,))
        if cursor.fetchone() is None:
            # Insert new user with plain text password
            cursor.execute(
                "INSERT INTO user_auth (username, password, role, factory_location) VALUES (%s, %s, %s, %s)",
                (username, default_password, "Factory Head", factory)
            )
            conn.commit()
            print(f"Created Factory Head user: {username}")
    
    # Create PSD Officer users for each factory
    for factory in FACTORY_LOCATIONS:
        # Generate username: tmcv-{factory_name}-psd
        factory_name = factory.replace(' ', '').lower()
        username = f"tmcv-{factory_name}-psd"
        
        # Check if user already exists
        cursor.execute("SELECT * FROM user_auth WHERE username = %s", (username,))
        if cursor.fetchone() is None:
            # Insert new user with plain text password
            cursor.execute(
                "INSERT INTO user_auth (username, password, role, factory_location) VALUES (%s, %s, %s, %s)",
                (username, default_password, "PSD Officer", factory)  # Updated to PSD Officer
            )
            conn.commit()
            print(f"Created PSD Officer user: {username}")
    
    # Create users for Shop Floor Training Coordinators for each factory
    for factory in FACTORY_LOCATIONS:
        # Generate username: tmcv-{factory_name}-coordinator
        factory_name = factory.replace(' ', '').lower()
        username = f"tmcv-{factory_name}-coordinator"
        
        # Check if user already exists
        cursor.execute("SELECT * FROM user_auth WHERE username = %s", (username,))
        if cursor.fetchone() is None:
            # Insert new user with plain text password
            cursor.execute(
                "INSERT INTO user_auth (username, password, role, factory_location) VALUES (%s, %s, %s, %s)",
                (username, default_password, "Shop Floor Training Coordinators", factory)
            )
            conn.commit()
            print(f"Created user: {username}")
    
    conn.close()
# ✅ Manage Users Page (No Login Required)
@user_auth.route('/manage', methods=['GET', 'POST'])
def manage_users():
    # Initialize users if database is empty
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM user_auth")
    users = cursor.fetchall()
    
    if len(users) == 0:
        initialize_users()
        flash("Database initialized with all required users!", "success")
        return redirect(url_for('user_auth.manage_users'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'edit':
            user_id = request.form.get('user_id')
            role = request.form.get('role')
            factory = request.form.get('factory') if role in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"] else None  # Updated to PSD Officer
            
            # Generate new username based on role and factory
            if role in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"] and factory:  # Updated to PSD Officer
                # Format: tmcv-{factory_name}-{role_suffix}
                factory_name = factory.replace(' ', '').lower()
                if role == "Factory Head":
                    username = f"tmcv-{factory_name}-head"
                elif role == "PSD Officer":  # Updated to PSD Officer
                    username = f"tmcv-{factory_name}-psd"
                elif role == "Shop Floor Training Coordinators":
                    username = f"tmcv-{factory_name}-coordinator"
            else:
                # Format: tmcv-{role_name}
                role_name = role.replace(' ', '').lower()
                username = f"tmcv-{role_name}"
            
            # Update user
            cursor.execute(
                "UPDATE user_auth SET username=%s, role=%s, factory_location=%s WHERE id=%s",
                (username, role, factory, user_id)
            )
            conn.commit()
            flash("User updated successfully!", "success")
            
        elif action == 'delete':
            user_id = request.form.get('user_id')
            
            # Delete user
            cursor.execute("DELETE FROM user_auth WHERE id=%s", (user_id,))
            conn.commit()
            flash("User deleted successfully!", "success")
            
        elif action == 'change_username':
            user_id = request.form.get('user_id')
            new_username = request.form.get('new_username')
            
            # Update username
            cursor.execute(
                "UPDATE user_auth SET username=%s WHERE id=%s",
                (new_username, user_id)
            )
            conn.commit()
            flash("Username updated successfully!", "success")
            
        return redirect(url_for('user_auth.manage_users'))
    
    conn.close()
    return render_template("admin/manage_users.html", users=users, roles=ROLES, factories=FACTORY_LOCATIONS)
# ✅ Update Password for a Specific User
@user_auth.route('/update-password/<int:user_id>', methods=['POST'])
def update_password(user_id):
    new_password = request.form.get('new_password')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    # Update with plain text password
    cursor.execute("UPDATE user_auth SET password=%s WHERE id=%s", (new_password, user_id))
    conn.commit()
    conn.close()
    
    flash("Password updated successfully!", "success")
    return redirect(url_for('user_auth.manage_users'))
# ✅ Show Password for a Specific User
@user_auth.route('/show-password/<int:user_id>', methods=['POST'])
def show_password(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT password FROM user_auth WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    conn.close()
    
    if user:
        # Generate a secure token
        token = secrets.token_urlsafe(16)
        
        # Store token in session with expiration (5 minutes)
        if 'password_tokens' not in session:
            session['password_tokens'] = {}
        
        session['password_tokens'][token] = {
            'password': user['password'],
            'expires': time.time() + 300  # 5 minutes
        }
        
        # Return the token to the client
        return {'token': token}
    
    return {'error': 'User not found'}, 404
# ✅ Get Password Using Token
@user_auth.route('/get-password/<token>', methods=['GET'])
def get_password(token):
    # Check if token exists and is valid
    if 'password_tokens' in session and token in session['password_tokens']:
        token_data = session['password_tokens'][token]
        
        # Check if token is expired
        if time.time() < token_data['expires']:
            # Return the actual password
            return {'password': token_data['password']}
        
        # Remove expired token
        del session['password_tokens'][token]
    
    return {'error': 'Invalid or expired token'}, 404
# ✅ Initialize Users Route
@user_auth.route('/initialize-users', endpoint='initialize_users')
def initialize_users_route():
    initialize_users()
    flash("Database initialized with all required users!", "success")
    return redirect(url_for('user_auth.manage_users'))
# ✅ Login Route - Updated to handle empty database and factory-specific roles
@user_auth.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        plant_name = request.form.get('plant_name')
        role = request.form.get('role')
        username = request.form.get('username')
        password = request.form.get('password')
        factory_location = request.form.get('factory_location')
        
        # Validate plant selection
        if plant_name != "PUNE PLANT":
            flash("Currently, only Pune Plant data is available.", "error")
            return render_template("homepage.html", factory_locations=FACTORY_LOCATIONS)
        
        # Check if factory location is required but not provided
        if role in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"] and not factory_location:  # Updated to PSD Officer
            flash("Factory Location is required for this role.", "error")
            return render_template("homepage.html", factory_locations=FACTORY_LOCATIONS)
        
        # Check if database is empty
        if is_database_empty():
            # Only allow hardcoded admin to login when DB is empty
            if username == HARDCODED_ADMIN_USERNAME and password == HARDCODED_ADMIN_PASSWORD:
                # Set session for hardcoded admin
                session['user_id'] = 0  # Dummy ID
                session['username'] = HARDCODED_ADMIN_USERNAME
                session['role'] = 'Admin'
                session['factory_location'] = None
                session['logged_in'] = True
                session['login_time'] = datetime.now().isoformat()
                session.permanent = True
                
                return redirect(url_for('admin_home'))
            else:
                flash("Invalid credentials. Database is empty. Please use the admin credentials.", "error")
                return render_template("homepage.html", factory_locations=FACTORY_LOCATIONS)
        
        # Normal authentication process when DB has users
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Check if user exists with the provided credentials
        if role in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"]:  # Updated to PSD Officer
            # For factory-specific roles, we need to check factory location too
            cursor.execute(
                "SELECT * FROM user_auth WHERE username = %s AND password = %s AND role = %s AND factory_location = %s",
                (username, password, role, factory_location)
            )
        else:
            # For other roles (including PSD Head and ER Officer)
            cursor.execute(
                "SELECT * FROM user_auth WHERE username = %s AND password = %s AND role = %s",
                (username, password, role)
            )
            
        user = cursor.fetchone()
        conn.close()
        
        if user:
            # Set session variables
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            session['factory_location'] = user['factory_location']
            session['logged_in'] = True
            session['login_time'] = datetime.now().isoformat()
            
            # Set session to expire after 1 hour of inactivity
            session.permanent = True
            
            # Redirect based on role
            if user['role'] == 'Admin':
                return redirect(url_for('admin_home'))
            elif user['role'] in ["Factory Head", "PSD Officer", "Shop Floor Training Coordinators"]:  # Updated to PSD Officer
                return redirect(url_for('user_tech_bp.view_master_data'))
            else:
                # For all other roles, redirect to user dashboard
                return redirect(url_for('user.user_dashboard'))
        else:
            # Invalid credentials - stay on login page with error message
            flash("Invalid credentials. Please try again.", "error")
            return render_template("homepage.html", factory_locations=FACTORY_LOCATIONS)
    
    # For GET requests, pass factory_locations to the template
    return render_template("homepage.html", factory_locations=FACTORY_LOCATIONS)
# Add these routes to your main app.py or wherever your dashboard routes are defined
@user_auth.route('/admin')
def admin_home():
    if not is_logged_in() or session.get('role') != 'Admin':
        return redirect(url_for('user_auth.login'))
    # Render admin dashboard
    return "Admin Dashboard"
@user_auth.route('/user/dashboard')
def user_dashboard():
    if not is_logged_in():
        return redirect(url_for('user_auth.login'))
    # Render user dashboard
    return "User Dashboard"
# Add this to your main app.py to protect all routes
@user_auth.before_request
def require_login():
    # List of endpoints that don't require authentication
    allowed_routes = ['user_auth.login', 'user_auth.logout', 'static']
    
    # Check if the request endpoint is in the allowed routes
    if request.endpoint in allowed_routes:
        return
    
    # Redirect to login if not logged in
    if not is_logged_in():
        return redirect(url_for('user_auth.login'))
    
# ✅ Check if user is logged in (helper function)
def is_logged_in():
    return 'logged_in' in session and session['logged_in']
# ✅ Check if user has specific role (helper function)
def has_role(role_name):
    return is_logged_in() and session.get('role') == role_name
# ✅ Get current user info (helper function)
def get_current_user():
    if is_logged_in():
        return {
            'id': session.get('user_id'),
            'username': session.get('username'),
            'role': session.get('role'),
            'factory_location': session.get('factory_location')
        }
    return None
# ✅ Logout Route
@user_auth.route('/logout')
def logout():
    # Clear all session data
    session.clear()
    flash("You have been logged out successfully", "success")
    # Redirect to login page instead of homepage
    return redirect(url_for('user_auth.login'))