from flask import Blueprint, render_template, request, send_file, jsonify, flash, redirect, url_for, session
import pandas as pd
from io import BytesIO
from datetime import datetime, date, time, timedelta
import json
from utils import get_db_connection
factory_bp = Blueprint('factory_data', __name__, url_prefix='/factory-data')
def format_timedelta_to_time(td):
    """Convert timedelta to time string in HH:MM format"""
    if isinstance(td, timedelta):
        total_seconds = td.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        return f"{hours:02d}:{minutes:02d}"
    elif isinstance(td, time):
        return td.strftime('%H:%M')
    elif td is None:
        return ''
    else:
        return str(td)
def serialize_nomination(nom):
    """Convert nomination object to serializable format"""
    serialized = dict(nom)
    
    # Convert datetime objects to strings
    for key, value in serialized.items():
        if isinstance(value, (datetime, date)):
            serialized[key] = value.isoformat()
        elif isinstance(value, timedelta):
            # Convert timedelta to string representation
            serialized[key] = format_timedelta_to_time(value)
        elif value is None:
            serialized[key] = None
        # Handle numeric fields like she_hours and total_learning_hours
        elif isinstance(value, (int, float)):
            serialized[key] = value
    
    return serialized
def get_total_she_hours(per_no, cursor, exclude_training_name=None):
    """Calculate the total SHE (Safety+Health) learning hours from master_data for an employee"""
    query = """
        SELECT learning_hours
        FROM master_data
        WHERE per_no = %s
        AND pmo_training_category = 'SHE (Safety+Health)'
    """
    params = [per_no]
    
    # Exclude the current training they didn't attend
    if exclude_training_name:
        query += " AND training_name != %s"
        params.append(exclude_training_name)
    
    cursor.execute(query, params)
    records = cursor.fetchall()
    
    # Sum all SHE learning hours
    total_she_hours = sum(record['learning_hours'] or 0 for record in records)
    
    return total_she_hours
def get_total_learning_hours(per_no, cursor, exclude_training_name=None):
    """Calculate the total learning hours from all categories for an employee"""
    query = """
        SELECT learning_hours
        FROM master_data
        WHERE per_no = %s
    """
    params = [per_no]
    
    # Exclude the current training they didn't attend
    if exclude_training_name:
        query += " AND training_name != %s"
        params.append(exclude_training_name)
    
    cursor.execute(query, params)
    records = cursor.fetchall()
    
    # Sum all learning hours
    total_hours = sum(record['learning_hours'] or 0 for record in records)
    
    return total_hours
@factory_bp.before_request
def check_session():
    """Check if user is logged in and has factory location in session, except for endpoints that don't require it."""
    if 'logged_in' not in session or not session['logged_in']:
        flash("Please log in to access factory data", "error")
        return redirect(url_for('user_auth.login'))

    # Endpoints that do NOT require factory_location in session
    allowed_endpoints = [
        'factory_data.get_nominations',
        'factory_data.update_nomination_status',
        'factory_data.get_training_status'
    ]
    # Only require factory_location for other endpoints
    if request.endpoint not in allowed_endpoints:
        if 'factory_location' not in session or not session['factory_location']:
            flash("Factory location not found in session. Please log in again.", "error")
            return redirect(url_for('user_auth.login'))
@factory_bp.route('/', methods=['GET', 'POST'])
def factory_data():
    # Get factory location from session
    selected_factory = session['factory_location']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    scheduled_trainings = []
    completed_trainings = []
    selected_training_id = None
    training_details = None
    attendance_data = []
    completion_status = "Scheduled"
    completed_employees = []
    
    try:
        if request.method == 'POST':
            selected_training_id = request.form.get('training_id')
            if selected_training_id:
                try:
                    selected_training_id = int(selected_training_id)
                except ValueError:
                    selected_training_id = None
        
        # Load scheduled and completed trainings for the user's factory
        # Get current date and time for status calculation
        current_datetime = datetime.now()
        
        # Scheduled and in-progress trainings
        cursor.execute("""
            SELECT 
                tp.id,
                tp.training_name,
                tp.location_hall,
                tp.start_date,
                tp.end_date,
                tp.start_time,
                tp.end_time,
                tp.program_type,
                tp.duration_days,
                tp.learning_hours,
                CASE 
                    WHEN CURRENT_TIMESTAMP < CONCAT(tp.start_date, ' ', tp.start_time) THEN 'Scheduled'
                    WHEN CURRENT_TIMESTAMP BETWEEN CONCAT(tp.start_date, ' ', tp.start_time) 
                         AND CONCAT(tp.end_date, ' ', tp.end_time) THEN 'In Progress'
                    ELSE 'Completed'
                END as status
            FROM training_programs tp
            WHERE EXISTS (
                SELECT 1 FROM tni_data t 
                WHERE t.factory = %s 
                AND t.training_name = tp.training_name
            )
            ORDER BY tp.start_date DESC, tp.start_time DESC
        """, (selected_factory,))
        
        all_trainings = cursor.fetchall()
        # Format dates and times in Python instead of SQL
        for training in all_trainings:
            training['formatted_start_date'] = training['start_date'].strftime('%d/%m/%Y') if training['start_date'] else ''
            training['formatted_end_date'] = training['end_date'].strftime('%d/%m/%Y') if training['end_date'] else ''
            training['formatted_start_time'] = format_timedelta_to_time(training['start_time'])
            training['formatted_end_time'] = format_timedelta_to_time(training['end_time'])
        
        scheduled_trainings = [t for t in all_trainings if t['status'] in ['Scheduled', 'In Progress']]
        completed_trainings = [t for t in all_trainings if t['status'] == 'Completed']
        
        # Get detailed training information and attendance data
        if selected_training_id:
            # Get training details
            cursor.execute("""
                SELECT 
                    tp.*,
                    CASE 
                        WHEN CURRENT_TIMESTAMP < CONCAT(tp.start_date, ' ', tp.start_time) THEN 'Scheduled'
                        WHEN CURRENT_TIMESTAMP BETWEEN CONCAT(tp.start_date, ' ', tp.start_time) 
                             AND CONCAT(tp.end_date, ' ', tp.end_time) THEN 'In Progress'
                        ELSE 'Completed'
                    END as status
                FROM training_programs tp
                WHERE tp.id = %s
            """, (selected_training_id,))
            
            training_details = cursor.fetchone()
            if training_details:
                # Format dates and times in Python
                training_details['formatted_start_date'] = training_details['start_date'].strftime('%d/%m/%Y') if training_details['start_date'] else ''
                training_details['formatted_end_date'] = training_details['end_date'].strftime('%d/%m/%Y') if training_details['end_date'] else ''
                training_details['formatted_start_time'] = format_timedelta_to_time(training_details['start_time'])
                training_details['formatted_end_time'] = format_timedelta_to_time(training_details['end_time'])
            
            completion_status = training_details['status'] if training_details else "Scheduled"
            
            # Get attendance data for this training
            if training_details:
                # Fetch TNI employees who requested this training
                cursor.execute("""
                    SELECT per_no, name, training_name, factory
                    FROM tni_data
                    WHERE factory = %s AND training_name = %s
                """, (selected_factory, training_details['training_name']))
                tni_employees = cursor.fetchall()
                
                # Fetch attended employees from master_data
                cursor.execute("""
                    SELECT per_no, participants_name AS name, training_name
                    FROM master_data
                    WHERE training_name = %s
                """, (training_details['training_name'],))
                attended_employees = cursor.fetchall()
                
                # Fetch completed employees from master_data with available details
                cursor.execute("""
                    SELECT per_no, participants_name AS name, training_name, 
                           start_date, end_date, learning_hours
                    FROM master_data
                    WHERE factory = %s AND training_name = %s
                    ORDER BY participants_name
                """, (selected_factory, training_details['training_name']))
                completed_employees = cursor.fetchall()
                
                # Check nomination status for each employee
                cursor.execute("""
                    SELECT per_no, status 
                    FROM nominations 
                    WHERE training_id = %s AND factory_name = %s
                """, (selected_training_id, selected_factory))
                nomination_statuses = {row['per_no']: row['status'] for row in cursor.fetchall()}
                
                # Convert to DataFrames for comparison
                tni_df = pd.DataFrame(tni_employees)
                attended_df = pd.DataFrame(attended_employees)
                
                if not tni_df.empty:
                    if not attended_df.empty:
                        # Merge to find attendance status
                        merged = tni_df.merge(attended_df,
                                            on=['per_no', 'training_name'],
                                            how='left',
                                            suffixes=('_tni', '_attended'),
                                            indicator=True)
                        
                        # Create attendance data with status
                        attendance_data = []
                        for _, row in merged.iterrows():
                            status = "Attended" if row['_merge'] == 'both' else "Not Attended"
                            nomination_status = nomination_statuses.get(row['per_no'], None)
                            
                            # Get SHE and total learning hours for this employee
                            she_hours = get_total_she_hours(
                                row['per_no'], 
                                cursor, 
                                training_details['training_name']
                            )
                            total_hours = get_total_learning_hours(
                                row['per_no'], 
                                cursor, 
                                training_details['training_name']
                            )
                            
                            attendance_data.append({
                                'per_no': row['per_no'],
                                'name': row['name_tni'],
                                'training_name': row['training_name'],
                                'status': status,
                                'nomination_status': nomination_status,
                                'she_hours': she_hours,
                                'total_learning_hours': total_hours
                            })
                    else:
                        # If no one attended, all are not attended
                        attendance_data = []
                        for _, row in tni_df.iterrows():
                            # Get SHE and total learning hours for this employee
                            she_hours = get_total_she_hours(
                                row['per_no'], 
                                cursor, 
                                training_details['training_name']
                            )
                            total_hours = get_total_learning_hours(
                                row['per_no'], 
                                cursor, 
                                training_details['training_name']
                            )
                            
                            attendance_data.append({
                                'per_no': row['per_no'],
                                'name': row['name'],
                                'training_name': row['training_name'],
                                'status': 'Not Attended',
                                'nomination_status': nomination_statuses.get(row['per_no'], None),
                                'she_hours': she_hours,
                                'total_learning_hours': total_hours
                            })
    finally:
        conn.close()
    
    return render_template('user/factory_data.html',
                           scheduled_trainings=scheduled_trainings,
                           completed_trainings=completed_trainings,
                           selected_factory=selected_factory,
                           selected_training_id=selected_training_id,
                           training_details=training_details,
                           attendance_data=attendance_data,
                           completed_employees=completed_employees,
                           completion_status=completion_status)
@factory_bp.route('/download', methods=['POST'])
def download_factory_data():
    # Check if user is logged in and has factory_location in session
    if 'logged_in' not in session or not session['logged_in']:
        flash("Please log in to download factory data", "error")
        return redirect(url_for('user_auth.login'))
    
    if 'factory_location' not in session or not session['factory_location']:
        flash("Factory location not found in session. Please log in again.", "error")
        return redirect(url_for('user_auth.login'))
    
    selected_factory = session['factory_location']  # Use factory from session
    selected_training_id = request.form.get('training_id')
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get training name from ID
        cursor.execute("SELECT training_name FROM training_programs WHERE id = %s", (selected_training_id,))
        training_result = cursor.fetchone()
        
        if not training_result:
            return "Training not found", 404
            
        training_name = training_result['training_name']
        
        # Fetch TNI employees
        cursor.execute("""
            SELECT per_no, name, training_name
            FROM tni_data
            WHERE factory = %s AND training_name = %s
        """, (selected_factory, training_name))
        tni_employees = cursor.fetchall()
        
        # Fetch attended employees
        cursor.execute("""
            SELECT per_no, participants_name AS name, training_name
            FROM master_data
            WHERE training_name = %s
        """, (training_name,))
        attended_employees = cursor.fetchall()
        
        # Process data for download
        tni_df = pd.DataFrame(tni_employees)
        attended_df = pd.DataFrame(attended_employees)
        
        if not tni_df.empty:
            if not attended_df.empty:
                merged = tni_df.merge(attended_df,
                                    on=['per_no', 'training_name'],
                                    how='left',
                                    suffixes=('_tni', '_attended'),
                                    indicator=True)
                not_attended_df = merged[merged['_merge'] == 'left_only'][
                    ['per_no', 'name_tni', 'training_name']]
                not_attended_df = not_attended_df.rename(columns={'name_tni': 'name'})
            else:
                not_attended_df = tni_df
            
            # Add training hours information for each employee
            she_hours_list = []
            total_hours_list = []
            
            for _, row in not_attended_df.iterrows():
                she_hours = get_total_she_hours(
                    row['per_no'], 
                    cursor, 
                    training_name
                )
                total_hours = get_total_learning_hours(
                    row['per_no'], 
                    cursor, 
                    training_name
                )
                she_hours_list.append(she_hours)
                total_hours_list.append(total_hours)
            
            not_attended_df['she_hours'] = she_hours_list
            not_attended_df['total_learning_hours'] = total_hours_list
        else:
            not_attended_df = pd.DataFrame(columns=[
                'per_no', 'name', 'training_name', 'she_hours', 'total_learning_hours'
            ])
        
        # Convert to CSV
        output = BytesIO()
        not_attended_df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return send_file(output,
                     mimetype='text/csv',
                     as_attachment=True,
                     download_name=f'{selected_factory}_{training_name}_not_attended.csv')
        
    finally:
        conn.close()
@factory_bp.route('/get_training_status/<int:training_id>')
def get_training_status(training_id):
    # Check if user is logged in
    if 'logged_in' not in session or not session['logged_in']:
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN CURRENT_TIMESTAMP < CONCAT(start_date, ' ', start_time) THEN 'Scheduled'
                    WHEN CURRENT_TIMESTAMP BETWEEN CONCAT(start_date, ' ', start_time) 
                         AND CONCAT(end_date, ' ', tp.end_time) THEN 'In Progress'
                    ELSE 'Completed'
                END as status
            FROM training_programs
            WHERE id = %s
        """, (training_id,))
        
        result = cursor.fetchone()
        return jsonify({'status': result['status'] if result else 'Unknown'})
        
    finally:
        conn.close()
@factory_bp.route('/share_nomination', methods=['POST'])
def share_nomination():
    # Check if user is logged in
    if 'logged_in' not in session or not session['logged_in']:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    data = request.get_json()
    per_no = data.get('per_no')
    name = data.get('name')
    factory = session['factory_location']  # Use factory from session
    training_id = data.get('training_id')
    training_name = data.get('training_name')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if program hasn't started yet
        cursor.execute("""
            SELECT start_date, start_time 
            FROM training_programs 
            WHERE id = %s
        """, (training_id,))
        program = cursor.fetchone()
        
        if program:
            start_date = program['start_date']
            start_time = program['start_time']
            
            # Handle timedelta conversion to time
            if isinstance(start_time, timedelta):
                # Convert timedelta to time
                total_seconds = start_time.total_seconds()
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)
                seconds = int(total_seconds % 60)
                start_time = time(hour=hours, minute=minutes, second=seconds)
            
            program_start = datetime.combine(start_date, start_time)
            if datetime.now() >= program_start:
                return jsonify({'success': False, 'message': 'Cannot share nomination after program has started'})
        
        # Insert or update nomination
        cursor.execute("""
            INSERT INTO nominations (factory_name, training_id, per_no, name, status)
            VALUES (%s, %s, %s, %s, 'Processing')
            ON DUPLICATE KEY UPDATE status = 'Processing', updated_at = CURRENT_TIMESTAMP
        """, (factory, training_id, per_no, name))
        
        conn.commit()
        return jsonify({'success': True, 'message': 'Nomination shared successfully'})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        
    finally:
        conn.close()
@factory_bp.route('/get_nominations/<int:training_id>')
def get_nominations(training_id):
    # Check if user is logged in
    if 'logged_in' not in session or not session['logged_in']:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT n.*, tp.training_name, tp.start_date, tp.start_time
            FROM nominations n
            JOIN training_programs tp ON n.training_id = tp.id
            WHERE n.training_id = %s
            ORDER BY n.factory_name, n.shared_at DESC
        """, (training_id,))
        
        nominations = cursor.fetchall()
        
        # Calculate nomination counts
        total_received = len(nominations)
        total_accepted = sum(1 for nom in nominations if nom['status'] == 'Accepted')
        
        # Count nominations by factory
        factory_counts = {}
        for nom in nominations:
            factory = nom['factory_name']
            if factory not in factory_counts:
                factory_counts[factory] = 0
            factory_counts[factory] += 1
        
        # Add SHE Hours and Total Learning Hours to each nomination
        for nom in nominations:
            # Calculate SHE Hours for this employee
            she_hours = get_total_she_hours(
                nom['per_no'], 
                cursor, 
                nom['training_name']  # Exclude current training
            )
            
            # Calculate Total Learning Hours for this employee
            total_hours = get_total_learning_hours(
                nom['per_no'], 
                cursor, 
                nom['training_name']  # Exclude current training
            )
            
            # Add these values to the nomination dictionary
            nom['she_hours'] = she_hours
            nom['total_learning_hours'] = total_hours
        
        # Convert to serializable format
        serialized_nominations = []
        for nom in nominations:
            serialized_nominations.append(serialize_nomination(nom))
        
        # Group by factory
        grouped_nominations = {}
        for nom in serialized_nominations:
            factory = nom['factory_name']
            if factory not in grouped_nominations:
                grouped_nominations[factory] = []
            grouped_nominations[factory].append(nom)
            
        return jsonify({
            'success': True, 
            'nominations': grouped_nominations,
            'counts': {
                'total_received': total_received,
                'total_accepted': total_accepted,
                'factory_counts': factory_counts
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        
    finally:
        conn.close()
@factory_bp.route('/update_nomination_status', methods=['POST'])
def update_nomination_status():
    # Check if user is logged in
    if 'logged_in' not in session or not session['logged_in']:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    data = request.get_json()
    nomination_id = data.get('nomination_id')
    status = data.get('status')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE nominations 
            SET status = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (status, nomination_id))
        
        conn.commit()
        return jsonify({'success': True, 'message': f'Nomination {status.lower()} successfully'})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        
    finally:
        conn.close()