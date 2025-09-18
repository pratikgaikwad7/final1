# cd_data_store.py
from flask import Blueprint, request, jsonify, render_template, flash, redirect, url_for
import pandas as pd
import pymysql
from utils import get_db_connection
import os
from datetime import datetime

bp = Blueprint('cd_data_store', __name__, url_prefix='/cd_data_store')

# Flexible table configuration - each table can have different columns
TABLE_CONFIGS = {
    'induction': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Induction Data'
    },
    'fta': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'FTA Data'
    },
    'jta': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'JTA Data'
    },
    'kaushalya': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Kaushalya Data'
    },
    'pragati': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Pragati Data'
    },
    'lakshya': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Lakshya Data'
    },
    'live_trainer': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Live Trainer Data'
    }
}

def validate_file(file):
    """Validate uploaded file"""
    if not file or file.filename == '':
        return False, 'No file selected'
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return False, 'Only Excel files are allowed'
    
    return True, 'File validated'

def validate_columns(df, required_columns):
    """Validate if Excel contains required columns"""
    missing = [col for col in required_columns if col not in df.columns]
    return missing

def clean_value(value):
    """Clean data values"""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip()

def parse_date(date_val):
    """Parse date from various formats"""
    if pd.isna(date_val):
        return None
    
    try:
        if isinstance(date_val, str):
            return datetime.strptime(date_val, '%Y-%m-%d').date()
        elif isinstance(date_val, datetime):
            return date_val.date()
        elif hasattr(date_val, 'date'):
            return date_val.date()
        else:
            return None
    except (ValueError, TypeError):
        return None

def process_data(df, table_config):
    """Process Excel data for database insertion"""
    processed = []
    errors = []
    
    for idx, row in df.iterrows():
        try:
            data_row = {}
            for col in table_config['columns']:
                if col in df.columns:
                    if col == 'doj':
                        data_row[col] = parse_date(row[col])
                    else:
                        data_row[col] = clean_value(row[col])
                else:
                    data_row[col] = None
            
            # Check required fields
            missing_req = [req for req in table_config['required_columns'] if not data_row.get(req)]
            if missing_req:
                errors.append(f"Row {idx+2}: Missing {', '.join(missing_req)}")
                continue
                
            processed.append(data_row)
                
        except Exception as e:
            errors.append(f"Row {idx+2}: Error - {str(e)}")
    
    return processed, errors

def insert_data(table_name, data):
    """Insert data into specified table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        columns = TABLE_CONFIGS[table_name]['columns']
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        for row in data:
            values = [row.get(col) for col in columns]
            cursor.execute(sql, values)
        
        conn.commit()
        return True, f"Inserted {len(data)} records into {table_name}"
        
    except Exception as e:
        conn.rollback()
        return False, f"Database error: {str(e)}"
    
    finally:
        cursor.close()
        conn.close()

# HTML page route
@bp.route('/upload_page')
def upload_page():
    """Render the upload page with all table options"""
    return render_template('admin_upload_files.html', table_configs=TABLE_CONFIGS)

# Upload route for HTML form
@bp.route('/upload', methods=['POST'])
def upload_data():
    """Handle file upload from HTML form"""
    try:
        table_name = request.form.get('table_name')
        if not table_name or table_name not in TABLE_CONFIGS:
            flash('Invalid table selected', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))
        
        if 'file' not in request.files:
            flash('No file provided', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))
        
        file = request.files['file']
        
        # Validate file
        is_valid, msg = validate_file(file)
        if not is_valid:
            flash(msg, 'danger')
            return redirect(url_for('cd_data_store.upload_page'))
        
        # Read Excel
        try:
            df = pd.read_excel(file)
        except Exception as e:
            flash(f'Error reading Excel: {str(e)}', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))
        
        # Validate columns
        required = TABLE_CONFIGS[table_name]['required_columns']
        missing = validate_columns(df, required)
        if missing:
            flash(f'Missing columns: {", ".join(missing)}', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))
        
        # Process data
        processed_data, errors = process_data(df, TABLE_CONFIGS[table_name])
        
        if errors:
            flash(f'Found {len(errors)} errors in data. First error: {errors[0]}', 'warning')
            return redirect(url_for('cd_data_store.upload_page'))
        
        if not processed_data:
            flash('No valid data to process', 'warning')
            return redirect(url_for('cd_data_store.upload_page'))
        
        # Insert data
        success, msg = insert_data(table_name, processed_data)
        
        if success:
            flash(msg, 'success')
        else:
            flash(msg, 'danger')
            
        return redirect(url_for('cd_data_store.upload_page'))
            
    except Exception as e:
        flash(f'Unexpected error: {str(e)}', 'danger')
        return redirect(url_for('cd_data_store.upload_page'))

# API routes
@bp.route('/api/upload/<table_name>', methods=['POST'])
def api_upload_data(table_name):
    """API endpoint for uploading data"""
    try:
        if table_name not in TABLE_CONFIGS:
            return jsonify({'success': False, 'message': 'Invalid table name'}), 400
        
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file provided'}), 400
        
        file = request.files['file']
        
        is_valid, msg = validate_file(file)
        if not is_valid:
            return jsonify({'success': False, 'message': msg}), 400
        
        # Read Excel
        try:
            df = pd.read_excel(file)
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error reading Excel: {str(e)}'}), 400
        
        # Validate columns
        required = TABLE_CONFIGS[table_name]['required_columns']
        missing = validate_columns(df, required)
        if missing:
            return jsonify({'success': False, 'message': f'Missing columns: {missing}'}), 400
        
        # Process data
        processed_data, errors = process_data(df, TABLE_CONFIGS[table_name])
        
        if errors:
            return jsonify({
                'success': False, 
                'message': 'Data validation errors',
                'errors': errors[:5],
                'valid_records': len(processed_data)
            }), 400
        
        if not processed_data:
            return jsonify({'success': False, 'message': 'No valid data to process'}), 400
        
        # Insert data
        success, msg = insert_data(table_name, processed_data)
        
        if success:
            return jsonify({
                'success': True,
                'message': msg,
                'records_processed': len(processed_data)
            }), 200
        else:
            return jsonify({'success': False, 'message': msg}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@bp.route('/api/tables', methods=['GET'])
def api_get_tables():
    """Get available tables"""
    tables_info = {
        name: {
            'display_name': config['display_name'],
            'columns': config['columns'],
            'required_columns': config['required_columns']
        }
        for name, config in TABLE_CONFIGS.items()
    }
    
    return jsonify({'success': True, 'tables': tables_info}), 200