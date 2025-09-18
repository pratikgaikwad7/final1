import os
import pymysql
from datetime import datetime, timedelta
import pandas as pd
from flask import flash

class Config:
    DB_HOST = 'localhost'
    DB_USER = 'root'
    DB_PASSWORD = '123456'
    DB_NAME = 'masterdata'
    PROGRAM_DATA_FILE = 'training_data.xlsx'  # Add this
    EOR_FILENAME = 'eor_data.xlsx'  # Add this
    QR_FOLDER = 'static/qrcodes'
    QR_BUFFER_MINUTES = 15
    ALLOWED_EXTENSIONS = {'xlsx'}
    QR_BASE_URL = 'http://1192.168.0.105:5003'
    QR_PROGRAM_PATH = '/attendance'
    QR_HALL_PATH = '/attendance/hall'

class Constants:
    LOCATION_HALLS = [
        '5-S Class Room', 'AR / VR Class Room', 'BIW FST Class Room',
        'C-6 Conference Hall', 'Chinchwad Foundry Conference Hall',
        'E-11 Conference Hall', 'Fire Security Basement Hall',
        'H-2 Conference Hall', 'H-7 Conference Hall',
        'Hostel Main Conference Hall', 'Hostel Ultra Conference Hall',
        'Hostel Hexa Conference Hall', 'Industry 4.0 Innovation Lab',
        'J-12 Paint Conference Hall', 'Jagruti Hall',
        'D-7 Kushal (Mech.) Class Room', 'Lake House VIP Conference Hall',
        'Learning Hall', 'LQOS Room', 'Maval Foundry MDC Chinchwad',
        'MMV Class Room', 'Power Train Class Room', 'Pragati PE Conference Hall',
        'Prayas (CNC) Hall', 'PRIMA Hall', 'SDP Hall',
        'TCF FST Class Room', 'Uddan Class Room', 'Unnati Class Room',
        'Utkarsh Class Room', 'Maral Training Hall'
    ]

    FACTORY_LOCATIONS = [
        'AXLE FACTORY', 'CCE-TS', 'CHINCHWAD FOUNDRY', 'CKD-SKD', 'CMS',
        'ENGINE FACTORY', 'ERC', 'GEAR FACTORY', 'HR FUNCTION',
        'J-11/12 PAINTSHOP', 'LAUNCH MANAGEMENT', 'LCV FACTORY',
        'M&HCV FACTORY', 'MATERIAL AUDIT', 'MAVAL FOUNDRY', 'PE', 'PPC',
        'PRESS & FRAME FACTORY', 'PTPA', 'QUALITY ASSURANCE', 'SCM',
        'Service Technical Support', 'WINGER FACTORY', 'XENON FACTORY'
    ]

    PROGRAM_TYPES = ['Calendar', 'Need-based (SDC Need base)', 'Need-based (Shop Need base)', 'Reschedule']
    TNI_OPTIONS = ['TNI', 'NON TNI']
    TIME_SLOTS = [f"{h:02d}:{m:02d}" for h in range(5, 23) for m in [0, 30]]

def get_db_connection():
    return pymysql.connect(
        host=Config.DB_HOST,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        db=Config.DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def load_training_data(tni_status='TNI'):
    """Load training data from database filtered by TNI status"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT Training_Name, PMO_Training_Category, PL_Category, 
                       BRSR_SQ_123_Category, Tni_Status, learning_hours
                FROM training_names
                WHERE UPPER(Tni_Status) = UPPER(%s)
            """, (tni_status,))
            training_data = cursor.fetchall()
        
        return training_data
    except Exception as e:
        print(f"Error loading training data from database: {str(e)}")
        flash(f'Error loading training data: {str(e)}', 'error')
        return []
    finally:
        conn.close()

def calculate_learning_hours(start_date, end_date, start_time, end_time):
    """Calculate total learning hours between two datetimes"""
    try:
        start_dt = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        return round((end_dt - start_dt).total_seconds() / 3600)
    except Exception as e:
        print(f"Error calculating learning hours: {e}")
        return None

def format_program_dates(program):
    """Helper function to format dates and times for display"""
    if 'start_date' in program:
        program['formatted_start_date'] = program['start_date'].strftime('%d/%m/%Y') if isinstance(program['start_date'], datetime) else program['start_date']
    if 'end_date' in program:
        program['formatted_end_date'] = program['end_date'].strftime('%d/%m/%Y') if isinstance(program['end_date'], datetime) else program['end_date']
    if 'start_time' in program:
        program['formatted_start_time'] = program['start_time'].strftime('%H:%M') if isinstance(program['start_time'], datetime.time) else program['start_time']
    if 'end_time' in program:
        program['formatted_end_time'] = program['end_time'].strftime('%H:%M') if isinstance(program['end_time'], datetime.time) else program['end_time']
    return program

def validate_attendance_time(program):
    """Validate if current time is within attendance window"""
    now = datetime.now()
    if now < program['qr_valid_from']:
        return 'not_started', program['qr_valid_from'].strftime('%d/%m/%Y %H:%M')
    elif now > program['qr_valid_to']:
        return 'ended', program['qr_valid_to'].strftime('%d/%m/%Y %H:%M')
    return None, None

def process_eor_excel(file_stream):
    """Process EOR Excel file and store directly in database"""
    try:
        # Read Excel file
        df = pd.read_excel(file_stream, dtype={'per_no': str})
        
        # Strip whitespace from column headers
        df.columns = [col.strip() for col in df.columns]

        # Extended and comprehensive column mapping
        column_mapping = {
            # PER NO variations
            'PER NO': 'per_no',
            'PER_NO': 'per_no',
            'Per No': 'per_no',
            'Pers.no.': 'per_no',
            'Pers No': 'per_no',
            'pers no': 'per_no',

            # Name variations
            'Employee Name': 'participants_name',
            'EMPLOYEE NAME': 'participants_name',
            'employee name': 'participants_name',
            'Name': 'participants_name',
            'name': 'participants_name',
            'Emp Name': 'participants_name',

            # Factory variations
            'FACTORY': 'factory',
            'Factory': 'factory',
            'factory': 'factory',

            # Department / Cost Center
            'DEPARTMENT': 'department',
            'Department': 'department',
            'department': 'department',
            'Cost Center': 'department',  # Map descriptive cost center to department
            'COST CENTER': 'department',
            'Cost center': 'department',

            # Gender
            'GENDER': 'gender',
            'Gender': 'gender',
            'gender': 'gender',
            'Gender Key': 'gender',

            # Employee Group
            'EMPLOYEE GROUP': 'employee_group',
            'Employee Group': 'employee_group',
            'employee group': 'employee_group',

            # Employee Subgroup
            'Employee Subgroup': 'employee_subgroup',
            'EMPLOYEE SUBGROUP': 'employee_subgroup',
            'employee subgroup': 'employee_subgroup',

            # Cost ctr (numeric values) - map to bc_no
            'Cost ctr': 'bc_no',
            'COST CTR': 'bc_no',
            'cost ctr': 'bc_no',
            'BCC NO': 'bc_no',
            'bcc no': 'bc_no',
        }

        # Apply column mapping if key exists in dataframe
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['per_no', 'participants_name', 'factory']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")
        
        # Clean data
        df = df.fillna('')
        
        # Connect to database
        conn = get_db_connection()
        
        try:
            with conn.cursor() as cursor:
                # Delete all existing records from eor_data table
                cursor.execute("DELETE FROM eor_data")
                
                # Insert new records
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO eor_data 
                        (per_no, participants_name, factory, department, gender, 
                         employee_group, employee_subgroup, bc_no)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get('per_no', ''),
                        row.get('participants_name', ''),
                        row.get('factory', ''),
                        row.get('department', ''),
                        row.get('gender', ''),
                        row.get('employee_group', ''),
                        row.get('employee_subgroup', ''),
                        row.get('bc_no', '')
                    ))
                
                conn.commit()
                return True, f"Successfully processed {len(df)} EOR records"
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        return False, f"Error processing EOR Excel: {str(e)}"
            
    except Exception as e:
        return False, f"Error processing EOR Excel: {str(e)}"

def process_training_excel(file_stream):
    """Process Training Excel file and store directly in database"""
    try:
        # Read Excel file
        df = pd.read_excel(file_stream)
        
        # Strip whitespace from column headers
        df.columns = [col.strip() for col in df.columns]
        
        # Column mapping for training data
        column_mapping = {
            'Training Name': 'Training_Name',
            'PMO Training Category': 'PMO_Training_Category',
            'PL Category': 'PL_Category',
            'BRSR SQ 1,2,3 Category': 'BRSR_SQ_123_Category',
            'Tni Status': 'Tni_Status',
            'Duration': 'learning_hours'
        }
        
        # Apply column mapping if key exists in dataframe
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['Training_Name', 'Tni_Status']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")
        
        # Clean data
        df = df.fillna('')
        
        # Connect to database
        conn = get_db_connection()
        
        try:
            with conn.cursor() as cursor:
                # Delete all existing records from training_names table
                cursor.execute("DELETE FROM training_names")
                
                # Insert new records
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO training_names 
                        (Training_Name, PMO_Training_Category, PL_Category, 
                         BRSR_SQ_123_Category, Tni_Status, learning_hours)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        row.get('Training_Name', ''),
                        row.get('PMO_Training_Category', ''),
                        row.get('PL_Category', ''),
                        row.get('BRSR_SQ_123_Category', ''),
                        row.get('Tni_Status', ''),
                        row.get('learning_hours', 0)
                    ))
                
                conn.commit()
                return True, f"Successfully processed {len(df)} training records"
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        return False, f"Error processing Training Excel: {str(e)}"

def get_eor_count(factory=None):
    """Get EOR count for a specific factory or all factories from database"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if factory and factory != "All":
                cursor.execute("""
                    SELECT COUNT(DISTINCT per_no) as count 
                    FROM eor_data 
                    WHERE UPPER(factory) = UPPER(%s)
                """, (factory.strip().upper(),))
            else:
                cursor.execute("SELECT COUNT(DISTINCT per_no) as count FROM eor_data")
            
            result = cursor.fetchone()
            return result['count'] if result else 0
            
    except Exception as e:
        print(f"Error in get_eor_count: {str(e)}")
        return 0
    finally:
        conn.close()

def load_eor_data():
    """Load EOR data from database"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM eor_data")
            eor_data = cursor.fetchall()
        return eor_data
    except Exception as e:
        print(f"Error loading EOR data: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()