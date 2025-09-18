from flask import Blueprint, render_template, request, jsonify, current_app
from datetime import datetime, timedelta, date, time
import pandas as pd
import os
import re
import pymysql
from utils import Config, Constants, get_db_connection, load_eor_data

attendance_bp = Blueprint('attendance', __name__, 
                         template_folder='templates',
                         static_folder='static')

def get_pmo_month(date_obj):
    """Determine PMO month based on date (cutoff is 20th of month)"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    if date_obj.day > 20:
        return (date_obj.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%B')
    return date_obj.strftime('%B')

def get_cd_month(date_obj):
    """Determine CD month based on date (cutoff is 26th of month)"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    if date_obj.day > 26:
        return (date_obj.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%B')
    return date_obj.strftime('%B')

def get_employee_details(per_no):
    """Get employee details from EOR database"""
    try:
        eor_data = load_eor_data()
        if not eor_data:
            return None
        # Find employee in EOR data
        emp = None
        for record in eor_data:
            if str(record.get('per_no', '')).strip() == str(per_no).strip():
                emp = record
                break
        
        if not emp:
            return None
        return {
            'participants_name': str(emp.get('participants_name', '')),
            'bc_no': str(emp.get('bc_no', emp.get('bc_no', ''))),
            'gender': str(emp.get('gender', '')),
            'employee_group': str(emp.get('employee_group', '')),
            'department': str(emp.get('department', '')),
            'factory': str(emp.get('factory', ''))
        }
    except Exception as e:
        current_app.logger.error(f"Error fetching employee {per_no}: {e}")
        return None

def validate_mobile_number(mobile):
    """Validate Indian mobile number format"""
    return re.match(r'^[6-9]\d{9}$', str(mobile)) is not None

def validate_email(email):
    """Validate email format"""
    if not email:  # Email is optional
        return True
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) is not None

def clean_value(val):
    """Clean and standardize values before database insertion"""
    if isinstance(val, str):
        val = val.strip()
        if val.lower() in ['none', 'null', '']:
            return None
    return str(val) if val is not None else None

def convert_to_date(date_val):
    """Convert date string or object to date object"""
    if not date_val:
        return None
    if isinstance(date_val, date):
        return date_val
    try:
        return datetime.strptime(date_val, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(date_val, '%d/%m/%Y').date()
        except (ValueError, TypeError):
            current_app.logger.error(f"Invalid date format: {date_val}")
            return None

def convert_to_time(time_val):
    """Convert time string, timedelta, or object to time object"""
    if not time_val:
        return None
    if isinstance(time_val, time):
        return time_val
    if isinstance(time_val, timedelta):
        # Convert timedelta to time (within 24 hours)
        total_seconds = int(time_val.total_seconds()) % (24 * 3600)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return time(hour=hours, minute=minutes, second=seconds)
    try:
        return datetime.strptime(time_val, '%H:%M').time()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(time_val, '%H:%M:%S').time()
        except (ValueError, TypeError):
            current_app.logger.error(f"Invalid time format: {time_val}")
            return None

def format_time_for_display(time_val):
    """Format time value for display as HH:MM"""
    if not time_val:
        return None
    time_obj = convert_to_time(time_val)
    if time_obj:
        return time_obj.strftime('%H:%M')
    return str(time_val)

def get_current_training_day(start_date, duration_days):
    """Determine current day of training (1, 2, or 3)"""
    if isinstance(start_date, str):
        start_date = convert_to_date(start_date)
    today = datetime.now().date()
    delta = (today - start_date).days
    
    if delta < 0:
        return None  # Training hasn't started
    elif delta >= duration_days:
        return None  # Training completed
    return delta + 1

def is_within_daily_time_window(start_time_str, end_time_str, buffer_minutes=15):
    """Check if current time is within valid attendance window"""
    now = datetime.now().time()
    try:
        start_time = convert_to_time(start_time_str)
        end_time = convert_to_time(end_time_str)
        
        if not start_time or not end_time:
            return False
            
        # Allow buffer before start time
        adjusted_start = (datetime.combine(datetime.today(), start_time) - 
                        timedelta(minutes=buffer_minutes)).time()
        
        return adjusted_start <= now <= end_time
    except ValueError:
        return False

def validate_attendance_time(program):
    """Validate if current time is within attendance window"""
    if not program.get('current_day'):
        return 'not_active', None
    
    if not is_within_daily_time_window(program['start_time'], program['end_time']):
        return 'invalid_time', f"{format_time_for_display(program['start_time'])} to {format_time_for_display(program['end_time'])}"
    
    return 'active', None

def calculate_learning_hours(program_hours, day1, day2, day3):
    """Calculate actual learning hours based on attendance"""
    if program_hours <= 8:
        # Single day program (2,4,6,8 hours)
        return program_hours if day1 else 0
    else:
        # Multi-day program (16,24 hours) - 8 hours per day
        attended_days = sum([1 for day in [day1, day2, day3] if day])
        return min(attended_days * 8, program_hours)

def get_program_by_qr(qr_code):
    """Get program details by QR code"""
    try:
        with get_db_connection().cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    id AS program_id, training_name, pmo_training_category, pl_category,
                    brsr_sq_123_category, location_hall, 
                    start_date,
                    end_date,
                    start_time,
                    end_time,
                    learning_hours, program_type as calendar_need_base_reschedule, 
                    tni_status as tni_non_tni, 
                    faculty_1, faculty_2, faculty_3, faculty_4,
                    qr_valid_from,
                    qr_valid_to,
                    qr_code_path,
                    duration_days
                FROM training_programs 
                WHERE qr_code_path = %s
            """, (qr_code,))
            program = cursor.fetchone()
            
            if program:
                # Convert date/time to strings for display
                program['start_date'] = program['start_date'].strftime('%Y-%m-%d') if isinstance(program['start_date'], date) else program['start_date']
                program['end_date'] = program['end_date'].strftime('%Y-%m-%d') if isinstance(program['end_date'], date) else program['end_date']
                program['start_time'] = format_time_for_display(program['start_time'])
                program['end_time'] = format_time_for_display(program['end_time'])
                
                start_date = convert_to_date(program['start_date'])
                program['calendar_month'] = start_date.strftime('%B') if start_date else None
                program['month_report_pmo_21_20'] = get_pmo_month(start_date) if start_date else None
                program['month_cd_key_26_25'] = get_cd_month(start_date) if start_date else None
                program['current_day'] = get_current_training_day(
                    program['start_date'],
                    program.get('duration_days', 3)
                )
                status, time = validate_attendance_time(program)
                program['attendance_status'] = status
                program['attendance_time'] = time
            return program
    except Exception as e:
        current_app.logger.error(f"Error fetching program by QR: {e}")
        return None

def get_program_by_id(program_id):
    """Get program details by program ID"""
    try:
        with get_db_connection().cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    id AS program_id, training_name, pmo_training_category, pl_category,
                    brsr_sq_123_category, location_hall, 
                    start_date,
                    end_date,
                    start_time,
                    end_time,
                    learning_hours, program_type as calendar_need_base_reschedule, 
                    tni_status as tni_non_tni, 
                    faculty_1, faculty_2, faculty_3, faculty_4,
                    qr_valid_from,
                    qr_valid_to,
                    qr_code_path,
                    duration_days
                FROM training_programs 
                WHERE id = %s
            """, (program_id,))
            program = cursor.fetchone()
            
            if program:
                # Convert date/time to strings for display
                program['start_date'] = program['start_date'].strftime('%Y-%m-%d') if isinstance(program['start_date'], date) else program['start_date']
                program['end_date'] = program['end_date'].strftime('%Y-%m-%d') if isinstance(program['end_date'], date) else program['end_date']
                program['start_time'] = format_time_for_display(program['start_time'])
                program['end_time'] = format_time_for_display(program['end_time'])
                
                start_date = convert_to_date(program['start_date'])
                program['calendar_month'] = start_date.strftime('%B') if start_date else None
                program['month_report_pmo_21_20'] = get_pmo_month(start_date) if start_date else None
                program['month_cd_key_26_25'] = get_cd_month(start_date) if start_date else None
                program['current_day'] = get_current_training_day(
                    program['start_date'],
                    program.get('duration_days', 3)
                )
                status, time = validate_attendance_time(program)
                program['attendance_status'] = status
                program['attendance_time'] = time
            return program
    except Exception as e:
        current_app.logger.error(f"Error fetching program by ID {program_id}: {e}")
        return None

def save_attendance(data):
    """Save attendance record to database"""
    conn = None
    try:
        required = [
            'per_no', 'mobile_no', 'cordi_name', 'training_name', 
            'start_date', 'end_date', 'program_id', 'current_day'
        ]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return {'error': f'Missing fields: {", ".join(missing)}'}, False
        if not validate_mobile_number(data['mobile_no']):
            return {'error': 'Invalid mobile number'}, False
        if not validate_email(data.get('email', '')):
            return {'error': 'Invalid email format'}, False
        # Ensure current_day is valid
        try:
            current_day = int(data['current_day'])
            if current_day < 1 or current_day > 3:
                return {'error': 'Invalid training day'}, False
        except (ValueError, TypeError):
            return {'error': 'Invalid training day'}, False
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            day_column = f"day_{data['current_day']}_attendance"
            
            # Check for existing attendance
            cursor.execute(f"""
                SELECT id, day_1_attendance, day_2_attendance, day_3_attendance, learning_hours as program_hours
                FROM master_data 
                WHERE program_id = %s AND per_no = %s
            """, (data['program_id'], data['per_no']))
            existing = cursor.fetchone()
            
            if existing and existing.get(day_column):
                return {'warning': 'Attendance already recorded for today'}, False
                
            # Calculate learning hours
            program_hours = float(data.get('learning_hours', 8))
            day1 = existing['day_1_attendance'] if existing else False
            day2 = existing['day_2_attendance'] if existing else False
            day3 = existing['day_3_attendance'] if existing else False
            
            # Update the day being marked today
            if current_day == 1:
                day1 = True
            elif current_day == 2:
                day2 = True
            else:
                day3 = True
                
            calculated_hours = calculate_learning_hours(program_hours, day1, day2, day3)
            
            # Convert date/time strings to proper objects
            start_date = convert_to_date(data.get('start_date'))
            end_date = convert_to_date(data.get('end_date'))
            start_time = convert_to_time(data.get('start_time'))
            end_time = convert_to_time(data.get('end_time'))
            
            if existing:
                # Update existing record
                cursor.execute(f"""
                    UPDATE master_data
                    SET {day_column} = TRUE,
                        learning_hours = %s,
                        email = %s,
                        cordi_name = %s,
                        last_updated = NOW()
                    WHERE id = %s
                """, (calculated_hours, data.get('email'), data.get('cordi_name'), existing['id']))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO master_data (
                        program_id, calendar_month, month_report_pmo_21_20, month_cd_key_26_25,
                        start_date, end_date, start_time, end_time, learning_hours,
                        training_name, pmo_training_category, pl_category, brsr_sq_123_category,
                        calendar_need_base_reschedule, tni_non_tni, location_hall,
                        faculty_1, faculty_2, faculty_3, faculty_4,
                        per_no, participants_name, bc_no, gender, employee_group,
                        department, factory,
                        mobile_no, cordi_name, email, {day_column}
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, TRUE
                    )
                """.format(day_column=day_column), (
                    clean_value(data.get('program_id')),
                    clean_value(data.get('calendar_month')),
                    clean_value(data.get('month_report_pmo_21_20')),
                    clean_value(data.get('month_cd_key_26_25')),
                    start_date,
                    end_date,
                    start_time,
                    end_time,
                    calculated_hours,
                    clean_value(data.get('training_name')),
                    clean_value(data.get('pmo_training_category')),
                    clean_value(data.get('pl_category')),
                    clean_value(data.get('brsr_sq_123_category')),
                    clean_value(data.get('calendar_need_base_reschedule')),
                    clean_value(data.get('tni_non_tni')),
                    clean_value(data.get('location_hall')),
                    clean_value(data.get('faculty_1')),
                    clean_value(data.get('faculty_2')),
                    clean_value(data.get('faculty_3')),
                    clean_value(data.get('faculty_4')),
                    clean_value(data.get('per_no')),
                    clean_value(data.get('participants_name')),
                    clean_value(data.get('bc_no')),
                    clean_value(data.get('gender')),
                    clean_value(data.get('employee_group')),
                    clean_value(data.get('department')),
                    clean_value(data.get('factory')),
                    clean_value(data.get('mobile_no')),
                    clean_value(data.get('cordi_name')),
                    clean_value(data.get('email'))
                ))
            
            conn.commit()
            return {'success': True, 'learning_hours': calculated_hours}, True
            
    except Exception as e:
        current_app.logger.error(f"Error in save_attendance: {e}")
        if conn:
            conn.rollback()
        return {'error': str(e)}, False
    finally:
        if conn:
            conn.close()

@attendance_bp.route('/qr/<qr_code>')
def qr_attendance(qr_code):
    program = get_program_by_qr(qr_code)
    if not program:
        return render_template('admin/error.html', message="Invalid QR or Program not found")
    
    if program.get('attendance_status') != 'active':
        message = 'No active training session today'
        if program.get('attendance_status') == 'invalid_time':
            message = f'Attendance only valid between {program["attendance_time"]}'
        return render_template('admin/attendance_closed.html', 
                            program=program,
                            status=program.get('attendance_status'),
                            message=message)
    return render_template('admin/attendance_form.html', 
                         program=program,
                         current_day=program['current_day'])

@attendance_bp.route('/<int:program_id>')
def program_attendance(program_id):
    program = get_program_by_id(program_id)
    if not program:
        return render_template('admin/error.html', message='Program not found')
    if program.get('attendance_status') != 'active':
        message = 'No active training session today'
        if program.get('attendance_status') == 'invalid_time':
            message = f'Attendance only valid between {program["attendance_time"]}'
        return render_template('admin/attendance_closed.html', 
                            program=program,
                            status=program.get('attendance_status'),
                            message=message)
    return render_template('admin/attendance_form.html', 
                         program=program,
                         current_day=program['current_day'])

@attendance_bp.route('/check_per_no', methods=['POST'])
def check_per_no():
    per_no = request.form.get('per_no')
    if not per_no:
        return jsonify({'error': 'PER No required'}), 400
    
    emp = get_employee_details(per_no)
    if not emp:
        return jsonify({'error': 'Employee not found in EOR data'}), 404
    
    # Ensure all values are strings for JSON serialization
    serialized_emp = {k: str(v) if v is not None else '' for k, v in emp.items()}
    return jsonify(serialized_emp)

@attendance_bp.route('/submit_attendance', methods=['POST'])
def submit_attendance():
    try:
        data = request.get_json() if request.is_json else request.form.to_dict()
        current_app.logger.debug(f"Received attendance submission: {data}")
        # Validate required fields
        required_fields = ['per_no', 'mobile_no', 'program_id', 'cordi_name']
        missing = [field for field in required_fields if not data.get(field)]
        if missing:
            return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
        if not validate_mobile_number(data['mobile_no']):
            return jsonify({'error': 'Invalid mobile number'}), 400
        if 'email' in data and not validate_email(data['email']):
            return jsonify({'error': 'Invalid email format'}), 400
        # Get program details
        program = get_program_by_id(data['program_id'])
        if not program:
            return jsonify({'error': 'Program not found'}), 404
        # Dynamically compute current_day if not set
        if not program.get('current_day'):
            program['current_day'] = get_current_training_day(
                program['start_date'],
                program.get('duration_days', 3)
            )
        if not program.get('current_day'):
            return jsonify({'error': 'Cannot determine current training day'}), 400
        # Validate attendance time
        if program.get('attendance_status') != 'active':
            message = 'No active training session today'
            if program.get('attendance_status') == 'invalid_time':
                message = f'Attendance only valid between {program["attendance_time"]}'
            return jsonify({'error': message}), 400
        # Get employee details
        emp = get_employee_details(data.get('per_no'))
        if not emp:
            return jsonify({'error': 'Employee not found in EOR data'}), 404
        # Prepare attendance data
        attendance_data = {
            **data,
            **emp,
            **{k: str(v) for k, v in program.items()
               if k in ['training_name', 'location_hall', 'start_date', 'end_date',
                        'start_time', 'end_time', 'calendar_month',
                        'month_report_pmo_21_20', 'month_cd_key_26_25',
                        'pmo_training_category', 'pl_category', 'brsr_sq_123_category',
                        'calendar_need_base_reschedule', 'tni_non_tni',
                        'faculty_1', 'faculty_2', 'faculty_3', 'faculty_4',
                        'learning_hours']},
            'current_day': program['current_day'],
            'email': data.get('email', '')
        }
        # Save attendance
        result, success = save_attendance(attendance_data)
        if success:
            return jsonify({
                'success': True,
                'message': f'Attendance recorded for Day {program["current_day"]}',
                'learning_hours': result.get('learning_hours', 0),
                'submission_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'employee_name': emp.get('participants_name', ''),
                'department': emp.get('department', ''),
                'current_day': program['current_day']
            })
        if 'warning' in result:
            return jsonify(result), 200
        return jsonify(result), 400
    except Exception as e:
        current_app.logger.error(f"Error in submit_attendance: {str(e)}", exc_info=True)
        return jsonify({'error': 'Server error processing attendance'}), 500