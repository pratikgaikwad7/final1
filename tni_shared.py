from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for
import pandas as pd
import mysql.connector
import os
import math
from werkzeug.utils import secure_filename
from datetime import datetime

tni_shared_bp = Blueprint('training', __name__, template_folder='templates/admin')

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'masterdata',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_bin'
}

def get_available_years():
    """Get all available years from the database"""
    conn = mysql.connector.connect(**db_config)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT year FROM (
                SELECT DISTINCT target_year as year FROM training_targets
                UNION 
                SELECT DISTINCT year FROM tni_data WHERE year IS NOT NULL
                UNION
                SELECT DISTINCT year FROM final_tni_data WHERE year IS NOT NULL
            ) years ORDER BY year DESC
        """)
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

def create_final_tni_data_table():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS final_tni_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            per_no VARCHAR(50) COLLATE utf8mb4_bin,
            name VARCHAR(100) COLLATE utf8mb4_bin,
            factory VARCHAR(100) COLLATE utf8mb4_bin,
            bc_no VARCHAR(50) COLLATE utf8mb4_bin,
            training_name VARCHAR(100) COLLATE utf8mb4_bin,
            hours DECIMAL(10,2),
            year INT,
            UNIQUE KEY unique_entry_year (per_no, training_name, year)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def process_training_data(year=None):
    if year is None:
        year = datetime.now().year
    
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    try:
        # Delete existing data for this year
        cursor.execute("DELETE FROM final_tni_data WHERE year = %s", (year,))
        conn.commit()
        
        # Get all training targets for this year
        target_query = """
            SELECT t.training_name COLLATE utf8mb4_bin, t.target, 
                   COUNT(DISTINCT d.per_no) as actual_count
            FROM training_targets t
            LEFT JOIN tni_data d ON t.training_name COLLATE utf8mb4_bin = d.training_name COLLATE utf8mb4_bin
                               AND d.factory IS NOT NULL 
                               AND d.factory != ''
                               AND d.year = %s
            WHERE t.target_year = %s
            GROUP BY t.training_name, t.target
        """
        cursor.execute(target_query, (year, year))
        target_data = cursor.fetchall()
        
        total_inserted = 0
        
        for training_name, target, actual_count in target_data:
            # Safely handle NULL targets
            target = int(target or 0)
            
            if target == 0:
                # Skip trainings with zero target
                continue
                
            if actual_count <= target:
                # If we have less or equal to target, insert all available records
                query = """
                    INSERT IGNORE INTO final_tni_data (per_no, name, factory, bc_no, training_name, hours, year)
                    SELECT DISTINCT per_no, name, factory, bc_no, training_name, hours, %s
                    FROM tni_data
                    WHERE training_name COLLATE utf8mb4_bin = %s
                    AND factory IS NOT NULL 
                    AND factory != ''
                    AND year = %s
                """
                cursor.execute(query, (year, training_name, year))
                inserted_count = min(actual_count, target)
                
            else:
                # FACTORY-BALANCED LOGIC (from old code)
                overcount = actual_count - target
                
                factory_query = """
                    SELECT factory, COUNT(DISTINCT per_no) as factory_count
                    FROM tni_data
                    WHERE training_name COLLATE utf8mb4_bin = %s
                    AND factory IS NOT NULL 
                    AND factory != ''
                    AND year = %s
                    GROUP BY factory
                """
                cursor.execute(factory_query, (training_name, year))
                factory_counts = cursor.fetchall()
                
                total_participants = sum(fc[1] for fc in factory_counts)
                reductions = []
                
                for factory, count in factory_counts:
                    reduction = math.floor(count / total_participants * overcount)
                    reductions.append((factory, count, reduction))
                
                total_reduction = sum(r[2] for r in reductions)
                remaining_adjustment = overcount - total_reduction
                
                reductions.sort(key=lambda x: x[1], reverse=True)
                
                for i in range(remaining_adjustment):
                    if i < len(reductions):
                        factory, count, reduction = reductions[i]
                        reductions[i] = (factory, count, reduction + 1)
                
                for factory, count, reduction in reductions:
                    to_take = count - reduction
                    
                    if to_take > 0:
                        insert_query = """
                            INSERT IGNORE INTO final_tni_data (per_no, name, factory, bc_no, training_name, hours, year)
                            SELECT DISTINCT per_no, name, factory, bc_no, training_name, hours, %s
                            FROM tni_data
                            WHERE training_name COLLATE utf8mb4_bin = %s
                            AND factory COLLATE utf8mb4_bin = %s
                            AND factory IS NOT NULL 
                            AND factory != ''
                            AND year = %s
                            ORDER BY RAND()
                            LIMIT %s
                        """
                        cursor.execute(insert_query, (year, training_name, factory, year, to_take))
                
                inserted_count = target
            
            total_inserted += inserted_count
            conn.commit()
        
        print(f"Total records inserted: {total_inserted}")
        
        # FINAL VERIFICATION AND ADJUSTMENT (from old code)
        verify_query = """
            SELECT t.training_name COLLATE utf8mb4_bin, t.target, 
                   COUNT(f.per_no) as final_count
            FROM training_targets t
            LEFT JOIN final_tni_data f ON t.training_name COLLATE utf8mb4_bin = f.training_name COLLATE utf8mb4_bin
                                     AND f.year = %s
            WHERE t.target_year = %s
            GROUP BY t.training_name, t.target
            HAVING final_count != t.target
        """
        cursor.execute(verify_query, (year, year))
        mismatches = cursor.fetchall()
        
        for training, target, current_count in mismatches:
            target = int(target or 0)
            difference = target - current_count
            if difference > 0:
                add_query = """
                    INSERT IGNORE INTO final_tni_data (per_no, name, factory, bc_no, training_name, hours, year)
                    SELECT DISTINCT t.per_no, t.name, t.factory, t.bc_no, t.training_name, t.hours, %s
                    FROM tni_data t
                    WHERE t.training_name COLLATE utf8mb4_bin = %s
                    AND t.factory IS NOT NULL 
                    AND t.factory != ''
                    AND t.year = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM final_tni_data f 
                        WHERE f.training_name COLLATE utf8mb4_bin = t.training_name COLLATE utf8mb4_bin
                        AND f.per_no = t.per_no
                        AND f.year = %s
                    )
                    ORDER BY RAND()
                    LIMIT %s
                """
                cursor.execute(add_query, (year, training, year, year, difference))
            elif difference < 0:
                remove_query = """
                    DELETE FROM final_tni_data
                    WHERE training_name COLLATE utf8mb4_bin = %s
                    AND year = %s
                    ORDER BY RAND()
                    LIMIT %s
                """
                cursor.execute(remove_query, (training, year, abs(difference)))
            
            conn.commit()
        
        # Final verification
        final_verify_query = """
            SELECT t.training_name COLLATE utf8mb4_bin, t.target, 
                   COUNT(f.per_no) as final_count
            FROM training_targets t
            LEFT JOIN final_tni_data f ON t.training_name COLLATE utf8mb4_bin = f.training_name COLLATE utf8mb4_bin
                                     AND f.year = %s
            WHERE t.target_year = %s
            GROUP BY t.training_name, t.target
        """
        cursor.execute(final_verify_query, (year, year))
        results = cursor.fetchall()
        
        for training, target, final_count in results:
            target = int(target or 0)
            if final_count != target:
                print(f"Warning: {training} has {final_count} records but target is {target}")
        
        # Verify grand total
        grand_total_query = "SELECT COUNT(*) FROM final_tni_data WHERE year = %s"
        cursor.execute(grand_total_query, (year,))
        grand_total = cursor.fetchone()[0]
        print(f"Final grand total: {grand_total}")
        
    finally:
        cursor.close()
        conn.close()
def get_training_summary(year=None):
    if year is None:
        year = datetime.now().year
    
    conn = mysql.connector.connect(**db_config)
    
    try:
        query = """
            SELECT 
                training_name COLLATE utf8mb4_bin as training_name,
                COUNT(per_no) as count
            FROM tni_data
            WHERE factory IS NOT NULL AND factory != '' AND year = %s
            GROUP BY training_name
            ORDER BY training_name
        """
        training_df = pd.read_sql(query, conn, params=[year])
        
        training_df['training_name'] = (
            training_df['training_name']
            .str.replace('\n', ' ')
            .str.replace('\r', ' ')
            .str.strip()
        )
        
        target_query = """
            SELECT training_name COLLATE utf8mb4_bin as training_name, target 
            FROM training_targets 
            WHERE target_year = %s
        """
        target_df = pd.read_sql(target_query, conn, params=[year])
        
        if not target_df.empty:
            target_df['training_name'] = (
                target_df['training_name']
                .str.replace('\n', ' ')
                .str.replace('\r', ' ')
                .str.strip()
            )
            training_df = pd.merge(
                training_df, 
                target_df, 
                on='training_name', 
                how='left'
            )
            training_df['target'] = training_df['target'].fillna(0).astype(int)
        else:
            training_df['target'] = 0
        
        grand_total_count = training_df['count'].sum()
        grand_total_target = training_df['target'].sum()
        
        return (
            training_df.to_dict('records'),
            grand_total_count,
            grand_total_target
        )
    
    finally:
        conn.close()

def get_final_factory_summary(year=None):
    if year is None:
        year = datetime.now().year
    
    conn = mysql.connector.connect(**db_config)
    
    try:
        training_query = """
            SELECT DISTINCT training_name COLLATE utf8mb4_bin as training_name 
            FROM final_tni_data 
            WHERE year = %s
        """
        trainings = pd.read_sql(training_query, conn, params=[year])['training_name'].tolist()
        
        query = """
            SELECT 
                factory COLLATE utf8mb4_bin as factory,
                training_name COLLATE utf8mb4_bin as training_name, 
                COUNT(per_no) as count
            FROM final_tni_data
            WHERE year = %s
            GROUP BY factory, training_name
        """
        df = pd.read_sql(query, conn, params=[year])
        
        if not df.empty:
            pivot = df.pivot_table(index='factory',
                                 columns='training_name',
                                 values='count',
                                 aggfunc='sum',
                                 fill_value=0)
            
            pivot['Total'] = pivot.sum(axis=1)
            grand_total = pivot.sum().to_frame().T
            grand_total.index = ['Grand Total']
            pivot = pd.concat([pivot, grand_total])
            
            return pivot.to_dict('index'), trainings
        return None, None
    
    finally:
        conn.close()

def get_original_factory_summary(year=None):
    if year is None:
        year = datetime.now().year
    
    conn = mysql.connector.connect(**db_config)
    
    try:
        training_query = """
            SELECT DISTINCT training_name COLLATE utf8mb4_bin as training_name 
            FROM tni_data 
            WHERE factory IS NOT NULL AND factory != '' AND year = %s
        """
        trainings = pd.read_sql(training_query, conn, params=[year])['training_name'].tolist()
        
        query = """
            SELECT 
                factory COLLATE utf8mb4_bin as factory,
                training_name COLLATE utf8mb4_bin as training_name, 
                COUNT(DISTINCT per_no) as count
            FROM tni_data
            WHERE factory IS NOT NULL AND factory != '' AND year = %s
            GROUP BY factory, training_name
        """
        df = pd.read_sql(query, conn, params=[year])
        
        if not df.empty:
            pivot = df.pivot_table(index='factory',
                                 columns='training_name',
                                 values='count',
                                 aggfunc='sum',
                                 fill_value=0)
            
            pivot['Total'] = pivot.sum(axis=1)
            grand_total = pivot.sum().to_frame().T
            grand_total.index = ['Grand Total']
            pivot = pd.concat([pivot, grand_total])
            
            return pivot.to_dict('index'), trainings
        return None, None
    
    finally:
        conn.close()

@tni_shared_bp.route('/training', methods=['GET', 'POST'])
def upload_and_summary():
    create_final_tni_data_table()
    available_years = get_available_years()
    current_year = datetime.now().year
    
    # Get selected year from request
    selected_year = request.args.get('year', current_year, type=int)
    if selected_year not in available_years and available_years:
        selected_year = available_years[0]
    
    if request.method == 'POST':
        file = request.files['file']
        upload_year = request.form.get('upload_year', current_year, type=int)
        
        if file and file.filename.endswith('.xlsx'):
            filename = secure_filename(file.filename)
            upload_folder = os.path.join(current_app.root_path, 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)

            df = pd.read_excel(filepath)
            df.columns = df.columns.str.strip()
            
            # Explicit column mapping based on your Excel format
            column_mapping = {
                'Sr. no': 'sr_no',
                'Per. No': 'per_no',
                'BC. No': 'bc_no',
                'Name': 'name',
                'Factory': 'factory'
            }
            
            # Apply the mapping only to columns that exist
            rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_map)

            # Explicitly list standard columns (non-training columns)
            standard_columns = ['per_no', 'name', 'factory', 'bc_no']
            if 'sr_no' in df.columns:
                standard_columns.append('sr_no')

            # Training columns are all remaining columns that contain hours data
            training_columns = [col for col in df.columns 
                               if col not in standard_columns 
                               and not col.lower().startswith('unnamed')]

            # Validate we have training columns
            if not training_columns:
                flash("No training columns found in the uploaded file", "error")
                return redirect(url_for('training.upload_and_summary'))

            # Melt the dataframe to transform training columns into rows
            df_long = df.melt(id_vars=standard_columns, 
                            value_vars=training_columns,
                            var_name='training_name',
                            value_name='hours')

            # Clean and filter the data
            df_long = df_long.dropna(subset=['hours'])
            df_long['hours'] = pd.to_numeric(df_long['hours'], errors='coerce')
            df_long = df_long[df_long['hours'] > 0]
            df_long['per_no'] = df_long['per_no'].astype(str).str.replace(r'\.0$', '', regex=True)
            
            # Clean factory and bc_no fields
            df_long['factory'] = df_long['factory'].fillna('').str.strip()
            df_long['bc_no'] = df_long['bc_no'].fillna('').astype(str).str.strip()
            df_long['year'] = upload_year


            # Store in database
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            
            # Delete existing data for this year
            cursor.execute("DELETE FROM tni_data WHERE year = %s", (upload_year,))
            
            for _, row in df_long.iterrows():
                cursor.execute("""
                    INSERT IGNORE INTO tni_data (per_no, name, factory, bc_no, training_name, hours, year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['per_no'], 
                    row['name'], 
                    row['factory'], 
                    row['bc_no'], 
                    row['training_name'], 
                    row['hours'],
                    row['year']
                ))

            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Process the training data for the uploaded year
            process_training_data(upload_year)
            
            flash(f"Data for year {upload_year} uploaded and processed successfully", "success")
            selected_year = upload_year

    training_table, grand_total_count, grand_total_target = get_training_summary(selected_year)
    final_factory_table, final_trainings = get_final_factory_summary(selected_year)
    original_factory_table, original_trainings = get_original_factory_summary(selected_year)

    return render_template('admin/set_target.html', 
                         training_table=training_table,
                         final_factory_table=final_factory_table,
                         original_factory_table=original_factory_table,
                         grand_total_count=grand_total_count,
                         grand_total_target=grand_total_target,
                         trainings=final_trainings if final_trainings else original_trainings,
                         available_years=available_years,
                         selected_year=selected_year,
                         current_year=current_year)