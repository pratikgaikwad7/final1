from flask import Blueprint, render_template, request, redirect, url_for, flash
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from collections import defaultdict

target_bp = Blueprint('target', __name__, url_prefix='/target')

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'masterdata'
}

def get_month_index():
    """Get the current month index where April=1, May=2, ..., January=10"""
    current_month = datetime.now().month
    if current_month >= 4:
        return current_month - 3
    else:
        return min(current_month + 9, 10)

def create_connection():
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def get_available_years():
    """Get all available years from the database"""
    conn = create_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT DISTINCT target_year FROM training_targets ORDER BY target_year DESC")
        db_years = [year['target_year'] for year in cursor.fetchall()]
        return db_years
    except Error as e:
        print(f"Error getting available years: {e}")
        return []
    finally:
        if conn:
            conn.close()

def initialize_new_year(target_year, conn, source_year=None):
    """Initialize a new year by copying structure from specified source year"""
    if source_year is None:
        # If no source year specified, use the latest available year
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT MAX(target_year) as latest_year FROM training_targets")
        result = cursor.fetchone()
        source_year = result['latest_year'] if result['latest_year'] else target_year - 1
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO training_targets (
                training_name, pmo_category, pl_category, target_year,
                target, batch_size, is_total, is_grand_total
            )
            SELECT 
                training_name, pmo_category, pl_category, %s,
                0 as target, 0 as batch_size,  -- Set defaults to 0
                FALSE as is_total, FALSE as is_grand_total
            FROM training_targets 
            WHERE target_year = %s
            AND is_total = FALSE 
            AND is_grand_total = FALSE
            ON DUPLICATE KEY UPDATE
                target = VALUES(target),
                batch_size = VALUES(batch_size)
        """, (target_year, source_year))
        conn.commit()
        return True
    except Error as e:
        print(f"Error initializing new year: {e}")
        return False

def sync_training_data_from_master(target_year, conn):
    """Sync training data from training_names table to training_targets table"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                Training_Name, 
                PMO_Training_Category, 
                PL_Category
            FROM training_names
            WHERE Tni_Status = 'TNI'
        """)
        training_data = cursor.fetchall()
        
        for training in training_data:
            cursor.execute("""
                INSERT INTO training_targets 
                (training_name, pmo_category, pl_category, target_year, target, batch_size, is_total, is_grand_total)
                VALUES (%s, %s, %s, %s, 0, 0, FALSE, FALSE)
                ON DUPLICATE KEY UPDATE
                    target = VALUES(target),
                    batch_size = VALUES(batch_size)
            """, (
                training['Training_Name'],
                training['PMO_Training_Category'],
                training['PL_Category'],
                target_year
            ))
        
        conn.commit()
        return True, f"Synced {len(training_data)} training records"
        
    except Error as e:
        conn.rollback()
        return False, f"Error syncing training data: {str(e)}"
    except Exception as e:
        conn.rollback()
        return False, f"Unexpected error: {str(e)}"


def update_training_completion_counts(conn, training_name=None, tni_status=None):
    """
    Update training completion counts in training_targets table based on master_data attendance.
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        # First reset all counts to 0 for current year (except totals)
        current_year = datetime.now().year
        cursor.execute("""
            UPDATE training_targets 
            SET 
                ytd_actual = 0,
                april = 0,
                may = 0,
                june = 0,
                july = 0,
                august = 0,
                september = 0,
                october = 0,
                november = 0,
                december = 0,
                january = 0,
                february = 0,
                march = 0
            WHERE target_year = %s
            AND is_total = FALSE 
            AND is_grand_total = FALSE
        """, (current_year,))
        
        # Build query to count attendance records by training and month
        query = """
            SELECT 
                training_name,
                pmo_training_category as pmo_category,
                pl_category,
                calendar_month,
                COUNT(DISTINCT per_no) as attendance_count
            FROM master_data
            WHERE calendar_month IS NOT NULL
        """
        
        query_params = []
        
        # Add filters if provided
        if training_name:
            query += " AND training_name LIKE %s"
            query_params.append(f"%{training_name}%")
        
        if tni_status and tni_status != 'All':
            query += " AND tni_non_tni = %s"
            query_params.append(tni_status)
        
        # Group by training and month
        query += """
            GROUP BY 
                training_name, 
                pmo_training_category,
                pl_category,
                calendar_month
        """
        
        cursor.execute(query, query_params)
        results = cursor.fetchall()
        
        if not results:
            print("No attendance records found matching criteria")
            return False
        
        # Organize data by training and month
        training_data = defaultdict(dict)
        for row in results:
            key = (row['training_name'], row['pmo_category'], row['pl_category'])
            month = row['calendar_month'].lower() if row['calendar_month'] else None
            if month:
                training_data[key][month] = row['attendance_count']
        
        # Update training_targets table with the counts
        for (training_name, pmo_category, pl_category), month_counts in training_data.items():
            # Calculate derived fields
            ytd_actual = sum(month_counts.values())
            
            # Get the target and batch_size values for this training
            cursor.execute("""
                SELECT target, batch_size FROM training_targets
                WHERE training_name = %s 
                AND pmo_category = %s 
                AND pl_category = %s 
                AND target_year = %s
                LIMIT 1
            """, (training_name, pmo_category, pl_category, current_year))
            target_result = cursor.fetchone()
            target = target_result['target'] if target_result else 0
            batch_size = target_result['batch_size'] if target_result else 0  # Changed from 1 to 0
            
            # Calculate derived fields with no negative values
            balance = max(target - ytd_actual, 0) if target else 0
            programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
            month_index = get_month_index()
            ytd_target = (target // 10) * month_index if target else 0
            
            # Prepare the update query
            update_query = """
                UPDATE training_targets
                SET 
                    ytd_actual = %s,
                    balance = %s,
                    programs_to_run = %s,
                    ytd_target = %s,
                    april = %s,
                    may = %s,
                    june = %s,
                    july = %s,
                    august = %s,
                    september = %s,
                    october = %s,
                    november = %s,
                    december = %s,
                    january = %s,
                    february = %s,
                    march = %s
                WHERE 
                    training_name = %s 
                    AND pmo_category = %s 
                    AND pl_category = %s 
                    AND target_year = %s
                    AND is_total = FALSE 
                    AND is_grand_total = FALSE
            """
            
            # Prepare parameters
            params = (
                ytd_actual,
                balance,
                programs_to_run,
                ytd_target,
                month_counts.get('april', 0),
                month_counts.get('may', 0),
                month_counts.get('june', 0),
                month_counts.get('july', 0),
                month_counts.get('august', 0),
                month_counts.get('september', 0),
                month_counts.get('october', 0),
                month_counts.get('november', 0),
                month_counts.get('december', 0),
                month_counts.get('january', 0),
                month_counts.get('february', 0),
                month_counts.get('march', 0),
                training_name,
                pmo_category,
                pl_category,
                current_year
            )
            
            cursor.execute(update_query, params)
        
        # After updating individual records, update the totals
        update_totals_in_db(current_year, conn)
        
        conn.commit()
        return True
        
    except Error as e:
        print(f"Error updating training completion counts: {e}")
        conn.rollback()
        return False

def update_totals_in_db(target_year, conn):
    """Calculate and store category/grand totals in database"""
    try:
        cursor = conn.cursor()
        
        # Delete old totals for this year
        cursor.execute("""
            DELETE FROM training_targets 
            WHERE target_year = %s 
            AND (is_total = TRUE OR is_grand_total = TRUE)
        """, (target_year,))
        
        # Get month index for YTD Target calculation
        month_index = get_month_index()
        
        # Insert category totals
        cursor.execute("""
            INSERT INTO training_targets (
                training_name, pmo_category, pl_category, target_year,
                target, batch_size, ytd_target, ytd_actual, balance, programs_to_run,
                april, may, june, july, august, september, october, november, december,
                january, february, march, is_total
            )
            SELECT 
                CONCAT(pmo_category, ' (Total)') as training_name,
                '' as pmo_category,
                '—' as pl_category,
                target_year,
                SUM(target) as target,
                SUM(batch_size) as batch_size,
                SUM(target) / 10 * %s as ytd_target,
                SUM(ytd_actual) as ytd_actual,
                GREATEST(SUM(target) - SUM(ytd_actual), 0) as balance,
                ROUND(CASE WHEN SUM(batch_size) > 0 THEN GREATEST(SUM(target) - SUM(ytd_actual), 0) / SUM(batch_size) ELSE 0 END, 1) as programs_to_run,
                SUM(april) as april,
                SUM(may) as may,
                SUM(june) as june,
                SUM(july) as july,
                SUM(august) as august,
                SUM(september) as september,
                SUM(october) as october,
                SUM(november) as november,
                SUM(december) as december,
                SUM(january) as january,
                SUM(february) as february,
                SUM(march) as march,
                TRUE as is_total
            FROM training_targets
            WHERE target_year = %s 
            AND is_total = FALSE 
            AND is_grand_total = FALSE
            GROUP BY pmo_category, target_year
        """, (month_index, target_year))
        
        # Insert grand total
        cursor.execute("""
            INSERT INTO training_targets (
                training_name, pmo_category, pl_category, target_year,
                target, batch_size, ytd_target, ytd_actual, balance, programs_to_run,
                april, may, june, july, august, september, october, november, december,
                january, february, march, is_grand_total
            )
            SELECT 
                'Grand Total' as training_name,
                '' as pmo_category,
                '—' as pl_category,
                target_year,
                SUM(target) as target,
                SUM(batch_size) as batch_size,
                SUM(target) / 10 * %s as ytd_target,
                SUM(ytd_actual) as ytd_actual,
                GREATEST(SUM(target) - SUM(ytd_actual), 0) as balance,
                ROUND(CASE WHEN SUM(batch_size) > 0 THEN GREATEST(SUM(target) - SUM(ytd_actual), 0) / SUM(batch_size) ELSE 0 END, 1) as programs_to_run,
                SUM(april) as april,
                SUM(may) as may,
                SUM(june) as june,
                SUM(july) as july,
                SUM(august) as august,
                SUM(september) as september,
                SUM(october) as october,
                SUM(november) as november,
                SUM(december) as december,
                SUM(january) as january,
                SUM(february) as february,
                SUM(march) as march,
                TRUE as is_grand_total
            FROM training_targets
            WHERE target_year = %s 
            AND is_total = FALSE 
            AND is_grand_total = FALSE
            GROUP BY target_year
        """, (month_index, target_year))
        
        conn.commit()
        return True
    except Error as e:
        print(f"Error updating totals: {e}")
        return False

def get_training_data(target_year):
    """Fetch and prepare training data for a given year."""
    conn = create_connection()
    if not conn:
        print("❌ Failed to create database connection")
        return []
    
    try:
        cursor = conn.cursor(dictionary=True)

        # Fetch rows for the selected year
        cursor.execute("""
            SELECT 
                id, training_name, pmo_category, pl_category,
                tni, batch_size, target, ytd_actual,
                april, may, june, july, august, september,
                october, november, december, january, february, march,
                target_year, is_total, is_grand_total
            FROM training_targets 
            WHERE target_year = %s 
            ORDER BY 
                is_grand_total ASC,
                is_total ASC,
                pmo_category ASC,
                training_name ASC
        """, (target_year,))
        
        rows = cursor.fetchall()

        output = []
        row_counter = 1
        month_index = get_month_index()  # your helper

        for row in rows:
            # Defensive defaults - FIXED: batch_size defaults to 0 instead of 1
            target = row.get('target') or 0
            ytd_actual = row.get('ytd_actual') or 0
            batch_size = row.get('batch_size') or 0  # Changed from 1 to 0

            # Calculations
            balance = max(target - ytd_actual, 0)
            programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
            ytd_target = (target // 10) * month_index

            is_total_row = row.get('is_total') or row.get('is_grand_total')

            output.append({
                'id': row['id'],
                'display_id': '' if is_total_row else row_counter,
                'training_name': row.get('training_name'),
                'pmo_category': row.get('pmo_category'),
                'pl_category': row.get('pl_category'),
                'is_total_row': is_total_row,
                'is_grand_total': row.get('is_grand_total'),
                'target_year': row.get('target_year'),
                'tni': row.get('tni') or 0,
                'target': target,
                'batch_size': batch_size,
                'ytd_target': ytd_target,
                'ytd_actual': ytd_actual,
                'balance': balance,
                'programs_to_run': programs_to_run,
                'april': row.get('april') or 0,
                'may': row.get('may') or 0,
                'june': row.get('june') or 0,
                'july': row.get('july') or 0,
                'august': row.get('august') or 0,
                'september': row.get('september') or 0,
                'october': row.get('october') or 0,
                'november': row.get('november') or 0,
                'december': row.get('december') or 0,
                'january': row.get('january') or 0,
                'february': row.get('february') or 0,
                'march': row.get('march') or 0,
            })

            if not is_total_row:
                row_counter += 1

        return output

    except Error as e:
        print(f"❌ Database error in get_training_data: {e}")
        return []

    finally:
        if conn:
            conn.close()

def get_year_range(start: int = 1900, end: int = 2100):
    """Return a list of years for dropdown (default: 1900–2100)."""
    return list(range(end, start - 1, -1))  # descending order for dropdown

@target_bp.route('/edit', methods=['GET', 'POST'])
def edit_data():
    current_year = datetime.now().year
    target_year = request.args.get('target_year', default=current_year, type=int)

    # Get available years from database
    available_years = get_available_years()

    # Always include current year
    if current_year not in available_years:
        available_years.append(current_year)

    # Build full year range for dropdown (1900–2100)
    year_range = get_year_range()

    if request.method == 'POST':
        # Handle form submission for updates
        data = request.form
        try:
            target_year = int(data.get('target_year'))
        except (ValueError, TypeError):
            target_year = current_year
        
        conn = create_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('target.dashboard'))

        try:
            cursor = conn.cursor()

            training_ids = []
            for key in data.keys():
                if (key.startswith('target_') or key.startswith('batch_size_')) and key.split('_')[-1].isdigit():
                    training_id = key.split('_')[-1]
                    if training_id not in training_ids:
                        training_ids.append(training_id)

            for training_id in training_ids:
                def get_int_value(field_name):
                    try:
                        value = data.get(f'{field_name}_{training_id}')
                        return int(value) if value else 0
                    except (ValueError, TypeError):
                        return 0
                
                target = get_int_value('target')
                batch_size = get_int_value('batch_size')
                ytd_actual = get_int_value('ytd_actual')

                balance = max(target - ytd_actual, 0)
                programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
                month_index = get_month_index()
                ytd_target = (target // 10) * month_index

                cursor.execute("""
                    UPDATE training_targets SET
                        target = %s,
                        batch_size = %s,
                        ytd_target = %s,
                        ytd_actual = %s,
                        balance = %s,
                        programs_to_run = %s
                    WHERE id = %s AND target_year = %s
                    AND is_total = FALSE AND is_grand_total = FALSE
                """, (target, batch_size, ytd_target, ytd_actual, balance, programs_to_run, training_id, target_year))

            update_totals_in_db(target_year, conn)
            conn.commit()
            flash('Data updated successfully', 'success')
            return redirect(url_for('target.dashboard', target_year=target_year))

        except Error as e:
            conn.rollback()
            flash(f'Database error: {str(e)}', 'error')
            return redirect(url_for('target.edit_data', target_year=target_year))
        finally:
            conn.close()

    else:
        conn = create_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('target.dashboard'))

        try:
            # Check if year exists, initialize if not
            if target_year not in available_years:
                source_year = max(available_years) if available_years else current_year
                if initialize_new_year(target_year, conn, source_year=source_year):
                    flash(f'Initialized year {target_year} from {source_year}', 'info')
                    if target_year not in available_years:
                        available_years.append(target_year)
                        available_years.sort(reverse=True)
                else:
                    flash('Failed to initialize year structure', 'error')
                    target_year = current_year
            
            # Only sync training data if the year was just initialized
            # This prevents creating duplicates when editing existing years
            if target_year not in get_available_years():
                sync_training_data_from_master(target_year, conn)
            
            trainings = get_training_data(target_year)
            has_data = check_year_has_data(target_year)


            return render_template('admin/tni_edit.html',
                                   trainings=trainings,
                                   current_year=current_year,
                                   target_year=target_year,
                                   available_years=available_years,
                                   year_range=year_range,
                                   has_data=has_data)
        except Error as e:
            flash('Database error occurred', 'error')
            return redirect(url_for('target.dashboard'))
        finally:
            conn.close()

@target_bp.route('/')
def dashboard():
    current_year = datetime.now().year
    target_year = request.args.get('target_year', default=current_year, type=int)

    # Get available years from DB + include current year
    available_years = get_available_years()
    if current_year not in available_years:
        available_years.append(current_year)

    # Build full year range for dropdown (1900–2100)
    year_range = get_year_range()

    if target_year == current_year:
        conn = create_connection()
        if conn:
            try:
                update_training_completion_counts(conn)
            except Exception as e:
                print(f"Error updating completion counts: {e}")
            finally:
                conn.close()

    conn = create_connection()
    if not conn:
        flash('Database connection error', 'error')
        return render_template('admin/tni_dashboard.html',
                               trainings=[],
                               current_year=current_year,
                               target_year=target_year,
                               available_years=available_years,
                               year_range=year_range)

    try:
        if target_year not in available_years:
            source_year = max(available_years) if available_years else current_year
            if initialize_new_year(target_year, conn, source_year=source_year):
                flash(f'Initialized year {target_year} from {source_year}', 'info')
                if target_year not in available_years:
                    available_years.append(target_year)
                    available_years.sort(reverse=True)
            else:
                flash('Failed to initialize year structure', 'error')
                target_year = current_year

        # Only sync training data if the year was just initialized
        if target_year not in get_available_years():
            sync_training_data_from_master(target_year, conn)
        
        trainings = get_training_data(target_year)

        return render_template('admin/tni_dashboard.html',
                               trainings=trainings,
                               current_year=current_year,
                               target_year=target_year,
                               available_years=available_years,
                               year_range=year_range)

    except Error as e:
        flash('Database error occurred', 'error')
        return render_template('admin/tni_dashboard.html',
                               trainings=[],
                               current_year=current_year,
                               target_year=current_year,
                               available_years=available_years,
                               year_range=year_range)
    finally:
        conn.close()

@target_bp.route('/sync_training_data', methods=['POST'])
def sync_training_data():
    """Sync training data from training_names table to training_targets"""
    target_year = request.form.get('target_year', type=int)
    if not target_year:
        target_year = datetime.now().year
    
    conn = create_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.edit_data', target_year=target_year))
    
    try:
        success, message = sync_training_data_from_master(target_year, conn)
        if success:
            flash(message, 'success')
        else:
            flash(message, 'error')
    except Exception as e:
        flash(f'Error syncing training data: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.edit_data', target_year=target_year))

@target_bp.route('/update_completion_counts', methods=['POST'])
def update_completion_counts():
    training_name = request.form.get('training_name')
    tni_status = request.form.get('tni_status', 'All')
    
    conn = create_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.dashboard'))
    
    try:
        if update_training_completion_counts(conn, training_name, tni_status):
            flash('Successfully updated training completion counts', 'success')
        else:
            flash('No matching records found or error occurred', 'warning')
    except Exception as e:
        flash(f'Error updating completion counts: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.dashboard'))

@target_bp.errorhandler(405)
def method_not_allowed(e):
    flash('Invalid request method - please use the provided forms', 'error')
    return redirect(url_for('target.dashboard'))

@target_bp.route('/initialize_year', methods=['POST'])
def initialize_year():
    """Initialize a new year with current training data"""
    target_year = request.form.get('target_year', type=int)
    if not target_year:
        flash('Invalid year specified', 'error')
        return redirect(url_for('target.edit_data'))
    
    conn = create_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.edit_data', target_year=target_year))
    
    try:
        # Initialize the year structure
        if initialize_new_year(target_year, conn):
            # Sync with current training names
            success, message = sync_training_data_from_master(target_year, conn)
            if success:
                flash(f'Successfully initialized {target_year} with current training list', 'success')
            else:
                flash(f'Initialized {target_year} but could not sync training data: {message}', 'warning')
        else:
            flash('Failed to initialize year', 'error')
    except Exception as e:
        flash(f'Error initializing year: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.edit_data', target_year=target_year))

# Modify your edit_data function to check if data exists
def check_year_has_data(target_year):
    """Check if a year has any data in the database"""
    conn = create_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM training_targets WHERE target_year = %s", (target_year,))
        count = cursor.fetchone()[0]
        return count > 0
    except Error:
        return False
    finally:
        conn.close()