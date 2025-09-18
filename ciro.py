from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from datetime import datetime
from io import BytesIO
import pandas as pd
from utils import get_db_connection
import pymysql.cursors

ciro_bp = Blueprint('ciro', __name__, template_folder='templates/admin')

# Context processor to make 'now' available in all templates
@ciro_bp.context_processor
def inject_now():
    return {'now': datetime.now()}

def test_db_connection():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Database connection successful: {result}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

# Modified root route to redirect to dashboard
@ciro_bp.route('/')
def root():
    return redirect(url_for('ciro.dashboard'))

# Form submission route (now accessible at /form)
@ciro_bp.route('/form')
def form():
    return render_template('admin/ciro_form.html')

@ciro_bp.route('/success')
def success():
    return render_template('admin/ciro_success.html')

# Dashboard routes
@ciro_bp.route('/dashboard')
def dashboard():
    try:
        if not test_db_connection():
            flash("Database connection failed", "danger")
            return render_template('admin/ciro_dashboard.html', sessions=[], years=[], overall_avg_score=None)

        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Changed to use DictCursor
        
        # Get filter parameters
        month = request.args.get('month')
        year = request.args.get('year')
        trainer = request.args.get('trainer')
        search = request.args.get('search')
        
        # Base query for training sessions with grouping
        query = """
        SELECT 
            program_title,
            program_date,
            COUNT(*) as response_count,
            (
                SELECT GROUP_CONCAT(DISTINCT t.name SEPARATOR ', ')
                FROM (
                    SELECT trainer1_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer1_name != ''
                    UNION
                    SELECT trainer2_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer2_name != ''
                    UNION
                    SELECT trainer3_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer3_name != ''
                    UNION
                    SELECT trainer4_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer4_name != ''
                ) as t
                WHERE t.name IS NOT NULL AND t.name != ''
            ) as trainer_names,
            AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
            (
                SELECT AVG(avg_score)
                FROM (
                    SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer1_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer2_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer3_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer4_name IS NOT NULL
                ) as trainer_avgs
            ) as tfi,
            AVG(
                (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                 COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                 COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                 COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                 COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                (6 + 
                 CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
            ) as avg_score
        FROM feedback_responses fr
        WHERE 1=1
        """
        params = []
        
        # Apply filters
        if month:
            query += " AND MONTH(program_date) = %s"
            params.append(month)
        if year:
            query += " AND YEAR(program_date) = %s"
            params.append(year)
        if trainer:
            query += " AND (trainer1_name LIKE %s OR trainer2_name LIKE %s OR trainer3_name LIKE %s OR trainer4_name LIKE %s)"
            params.extend([f"%{trainer}%"] * 4)
        if search:
            query += " AND (program_title LIKE %s OR participants_name LIKE %s)"
            params.extend([f"%{search}%"] * 2)
        
        # Group by program and date
        query += " GROUP BY program_title, program_date ORDER BY program_date DESC"
        
        cursor.execute(query, params)
        sessions = cursor.fetchall()
        
        # Calculate overall average score
        overall_avg_score = None
        if sessions:
            avg_scores = [s['avg_score'] for s in sessions if s['avg_score'] is not None]
            if avg_scores:
                overall_avg_score = sum(avg_scores) / len(avg_scores)
        
        # Get unique years for filter dropdown
        cursor.execute("SELECT DISTINCT YEAR(program_date) as year FROM feedback_responses ORDER BY year DESC")
        years = [str(row['year']) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return render_template('admin/ciro_dashboard.html', sessions=sessions, years=years,
                              selected_month=month, selected_year=year,
                              selected_trainer=trainer, search_query=search,
                              overall_avg_score=overall_avg_score)
    
    except Exception as e:
        print(f"Error in dashboard: {str(e)}")
        flash(f"An error occurred: {str(e)}", "danger")
        return render_template('admin/ciro_dashboard.html', sessions=[], years=[], overall_avg_score=None)

@ciro_bp.route('/training/<program_title>/<program_date>')
def training_detail(program_title, program_date):
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)  # Changed to use DictCursor
    
    # Get session summary
    cursor.execute("""
    SELECT 
        program_title,
        program_date,
        COUNT(*) as response_count,
        AVG(sec1_q1) as sec1_q1_avg,
        AVG(sec1_q2) as sec1_q2_avg,
        AVG(sec2_q1) as sec2_q1_avg,
        AVG(sec2_q2) as sec2_q2_avg,
        AVG(sec2_q3) as sec2_q3_avg,
        AVG(sec3_q1) as sec3_q1_avg,
        AVG(sec5_q1) as sec5_q1_avg,
        AVG(sec5_q2) as sec5_q2_avg,
        AVG(sec6_q1) as sec6_q1_avg,
        AVG(sec6_q2) as sec6_q2_avg,
        AVG(sec7_q1) as sec7_q1_avg,
        AVG(sec7_q2) as sec7_q2_avg,
        AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
        (
            SELECT AVG(avg_score)
            FROM (
                SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = fr.program_title 
                AND program_date = fr.program_date
                AND trainer1_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = fr.program_title 
                AND program_date = fr.program_date
                AND trainer2_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = fr.program_title 
                AND program_date = fr.program_date
                AND trainer3_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = fr.program_title 
                AND program_date = fr.program_date
                AND trainer4_name IS NOT NULL
            ) as trainer_avgs
        ) as tfi,
        AVG(
            (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
             COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
             COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
             COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
             COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
             sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
            (6 + 
             CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
        ) as avg_score
    FROM feedback_responses fr
    WHERE program_title = %s AND program_date = %s
    GROUP BY program_title, program_date
    """, (program_title, program_date))
    
    session = cursor.fetchone()
    
    # Get trainer feedback
    cursor.execute("""
        SELECT 
            trainer1_name as name,
            AVG(trainer1_q1) as q1_avg,
            AVG(trainer1_q2) as q2_avg,
            AVG(trainer1_q3) as q3_avg,
            AVG(trainer1_q4) as q4_avg,
            COUNT(trainer1_q1) as response_count
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s AND trainer1_name IS NOT NULL
        GROUP BY trainer1_name
        
        UNION ALL
        
        SELECT 
            trainer2_name as name,
            AVG(trainer2_q1) as q1_avg,
            AVG(trainer2_q2) as q2_avg,
            AVG(trainer2_q3) as q3_avg,
            AVG(trainer2_q4) as q4_avg,
            COUNT(trainer2_q1) as response_count
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s AND trainer2_name IS NOT NULL
        GROUP BY trainer2_name
        
        UNION ALL
        
        SELECT 
            trainer3_name as name,
            AVG(trainer3_q1) as q1_avg,
            AVG(trainer3_q2) as q2_avg,
            AVG(trainer3_q3) as q3_avg,
            AVG(trainer3_q4) as q4_avg,
            COUNT(trainer3_q1) as response_count
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s AND trainer3_name IS NOT NULL
        GROUP BY trainer3_name
        
        UNION ALL
        
        SELECT 
            trainer4_name as name,
            AVG(trainer4_q1) as q1_avg,
            AVG(trainer4_q2) as q2_avg,
            AVG(trainer4_q3) as q3_avg,
            AVG(trainer4_q4) as q4_avg,
            COUNT(trainer4_q1) as response_count
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s AND trainer4_name IS NOT NULL
        GROUP BY trainer4_name
    """, (program_title, program_date, program_title, program_date, 
          program_title, program_date, program_title, program_date))
    
    trainers = cursor.fetchall()
    
    # Get all individual responses with all questions
    cursor.execute("""
        SELECT *,
            (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
             sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0 as csi_score,
            (
                COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0)
            ) / NULLIF(
                (CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END), 0) as tfi_score
    FROM feedback_responses
    WHERE program_title = %s AND program_date = %s
    ORDER BY created_at DESC
""", (program_title, program_date))
    
    individual_responses = cursor.fetchall()
    
    # Get text feedback
    cursor.execute("""
        SELECT 
            participants_name,
            sec7_q3_text,
            sec7_q4_text,
            suggestions
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s
        AND (sec7_q3_text != '' OR sec7_q4_text != '' OR suggestions != '')
    """, (program_title, program_date))
    
    text_feedback = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('admin/ciro_training_detail.html', session=session, trainers=trainers, 
                          individual_responses=individual_responses, text_feedback=text_feedback,
                          program_title=program_title, program_date=program_date)

# [Rest of the code remains the same...]

@ciro_bp.route('/export/summary')
def export_summary():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.begin()  # Start transaction
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get filter parameters
        month = request.args.get('month')
        year = request.args.get('year')
        trainer = request.args.get('trainer')
        search = request.args.get('search')
        
        # Base query (same as dashboard)
        query = """
        SELECT 
            program_title,
            program_date,
            COUNT(*) as response_count,
            AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
            (
                SELECT AVG(avg_score)
                FROM (
                    SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer1_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer2_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer3_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer4_name IS NOT NULL
                ) as trainer_avgs
            ) as tfi,
            AVG(
                (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                 COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                 COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                 COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                 COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                (6 + 
                 CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
            ) as avg_score
        FROM feedback_responses fr
        WHERE 1=1
        """
        
        params = []
        
        # Apply filters
        if month:
            query += " AND MONTH(program_date) = %s"
            params.append(month)
        if year:
            query += " AND YEAR(program_date) = %s"
            params.append(year)
        if trainer:
            query += " AND (trainer1_name LIKE %s OR trainer2_name LIKE %s OR trainer3_name LIKE %s OR trainer4_name LIKE %s)"
            params.extend([f"%{trainer}%"] * 4)
        if search:
            query += " AND (program_title LIKE %s OR participants_name LIKE %s)"
            params.extend([f"%{search}%"] * 2)
        
        # Group by program and date
        query += " GROUP BY program_title, program_date ORDER BY program_date DESC"
        
        # Execute query and fetch data
        cursor.execute(query, params)
        data = cursor.fetchall()
        conn.commit()  # Commit transaction
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for the selected filters", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Summary']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='CIRO_Feedback_Summary.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        print(f"Error in export_summary: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/detail/<program_title>/<program_date>')
def export_detail(program_title, program_date):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.begin()  # Start transaction
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get all responses for this session
        query = """
            SELECT * FROM feedback_responses 
            WHERE program_title = %s AND program_date = %s
            ORDER BY created_at DESC
        """
        
        cursor.execute(query, (program_title, program_date))
        data = cursor.fetchall()
        conn.commit()  # Commit transaction
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for this training session", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Responses', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Responses']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Feedback_{program_title}_{program_date}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        print(f"Error in export_detail: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/individual/<int:response_id>')
def export_individual(response_id):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.begin()  # Start transaction
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get the specific response
        query = """
            SELECT * FROM feedback_responses 
            WHERE id = %s
        """
        
        cursor.execute(query, (response_id,))
        data = cursor.fetchall()
        conn.commit()  # Commit transaction
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for this response ID", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Response', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Response']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Feedback_Individual_{response_id}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        print(f"Error in export_individual: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/summary-report/<program_title>/<program_date>')
def export_summary_report(program_title, program_date):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        conn.begin()  # Start transaction
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Create Excel file with multiple sheets
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Sheet 1: Summary
            summary_query = """
                SELECT 
                    program_title,
                    program_date,
                    COUNT(*) as response_count,
                    AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                        sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
                    (
                        SELECT AVG(avg_score)
                        FROM (
                            SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                            FROM feedback_responses 
                            WHERE program_title = fr.program_title 
                            AND program_date = fr.program_date
                            AND trainer1_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                            FROM feedback_responses 
                            WHERE program_title = fr.program_title 
                            AND program_date = fr.program_date
                            AND trainer2_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                            FROM feedback_responses 
                            WHERE program_title = fr.program_title 
                            AND program_date = fr.program_date
                            AND trainer3_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                            FROM feedback_responses 
                            WHERE program_title = fr.program_title 
                            AND program_date = fr.program_date
                            AND trainer4_name IS NOT NULL
                        ) as trainer_avgs
                    ) as tfi,
                    AVG(
                        (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                         COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                         COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                         COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                         COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                         sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                        (6 + 
                         CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
                    ) as avg_score,
                    AVG(sec1_q1) as sec1_q1_avg,
                    AVG(sec1_q2) as sec1_q2_avg,
                    AVG(sec2_q1) as sec2_q1_avg,
                    AVG(sec2_q2) as sec2_q2_avg,
                    AVG(sec2_q3) as sec2_q3_avg,
                    AVG(sec3_q1) as sec3_q1_avg,
                    AVG(sec5_q1) as sec5_q1_avg,
                    AVG(sec5_q2) as sec5_q2_avg,
                    AVG(sec6_q1) as sec6_q1_avg,
                    AVG(sec6_q2) as sec6_q2_avg,
                    AVG(sec7_q1) as sec7_q1_avg,
                    AVG(sec7_q2) as sec7_q2_avg
                FROM feedback_responses fr
                WHERE program_title = %s AND program_date = %s
                GROUP BY program_title, program_date
            """
            
            cursor.execute(summary_query, (program_title, program_date))
            summary_data = cursor.fetchall()
            summary_df = pd.DataFrame(summary_data)
            
            if summary_df.empty:
                flash("No summary data found for this training session", "warning")
                return redirect(url_for('ciro.dashboard'))
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: Trainer Feedback
            trainer_query = """
                SELECT 
                    trainer1_name as name,
                    AVG(trainer1_q1) as knowledge_avg,
                    AVG(trainer1_q2) as presentation_avg,
                    AVG(trainer1_q3) as query_handling_avg,
                    AVG(trainer1_q4) as overall_avg,
                    COUNT(trainer1_q1) as response_count
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s AND trainer1_name IS NOT NULL
                GROUP BY trainer1_name
                
                UNION ALL
                
                SELECT 
                    trainer2_name as name,
                    AVG(trainer2_q1) as knowledge_avg,
                    AVG(trainer2_q2) as presentation_avg,
                    AVG(trainer2_q3) as query_handling_avg,
                    AVG(trainer2_q4) as overall_avg,
                    COUNT(trainer2_q1) as response_count
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s AND trainer2_name IS NOT NULL
                GROUP BY trainer2_name
                
                UNION ALL
                
                SELECT 
                    trainer3_name as name,
                    AVG(trainer3_q1) as knowledge_avg,
                    AVG(trainer3_q2) as presentation_avg,
                    AVG(trainer3_q3) as query_handling_avg,
                    AVG(trainer3_q4) as overall_avg,
                    COUNT(trainer3_q1) as response_count
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s AND trainer3_name IS NOT NULL
                GROUP BY trainer3_name
                
                UNION ALL
                
                SELECT 
                    trainer4_name as name,
                    AVG(trainer4_q1) as knowledge_avg,
                    AVG(trainer4_q2) as presentation_avg,
                    AVG(trainer4_q3) as query_handling_avg,
                    AVG(trainer4_q4) as overall_avg,
                    COUNT(trainer4_q1) as response_count
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s AND trainer4_name IS NOT NULL
                GROUP BY trainer4_name
            """
            
            cursor.execute(trainer_query, (
                program_title, program_date, program_title, program_date, 
                program_title, program_date, program_title, program_date
            ))
            trainer_data = cursor.fetchall()
            trainer_df = pd.DataFrame(trainer_data)
            trainer_df.to_excel(writer, sheet_name='Trainer Feedback', index=False)
            
            # Sheet 3: Section Averages
            if not summary_df.empty:
                section_df = pd.DataFrame({
                    'Section': ['1. Trainee Preparation', '2. Program Content', '3. Application in Work',
                              '5. Program Arrangements', '6. About Program', '7. Overall Feedback'],
                    'Average Score': [
                        (summary_df.iloc[0]['sec1_q1_avg'] + summary_df.iloc[0]['sec1_q2_avg'])/2,
                        (summary_df.iloc[0]['sec2_q1_avg'] + summary_df.iloc[0]['sec2_q2_avg'] + summary_df.iloc[0]['sec2_q3_avg'])/3,
                        summary_df.iloc[0]['sec3_q1_avg'],
                        (summary_df.iloc[0]['sec5_q1_avg'] + summary_df.iloc[0]['sec5_q2_avg'])/2,
                        (summary_df.iloc[0]['sec6_q1_avg'] + summary_df.iloc[0]['sec6_q2_avg'])/2,
                        (summary_df.iloc[0]['sec7_q1_avg'] + summary_df.iloc[0]['sec7_q2_avg'])/2
                    ]
                })
                section_df.to_excel(writer, sheet_name='Section Averages', index=False)
            
            # Sheet 4: Text Feedback
            text_query = """
                SELECT 
                    participants_name,
                    sec7_q3_text as most_relevant_topic,
                    sec7_q4_text as missing_topics,
                    suggestions
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s
                AND (sec7_q3_text != '' OR sec7_q4_text != '' OR suggestions != '')
            """
            
            cursor.execute(text_query, (program_title, program_date))
            text_data = cursor.fetchall()
            text_df = pd.DataFrame(text_data)
            text_df.to_excel(writer, sheet_name='Text Feedback', index=False)
        
        conn.commit()  # Commit all transactions
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Summary_Report_{program_title}_{program_date}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        print(f"Error in export_summary_report: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()