import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import RealDictCursor, execute_batch
from flask import Flask, render_template, request, redirect, url_for, flash, Response

app = Flask(__name__)
app.secret_key = 'super_secret_key_change_me'

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/mishloach_db")

def get_db_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

# --- פונקציות עזר לניקוי נתונים ---
def clean_int_str(val):
    """מנקה מספרים מייצוג עשרוני או טקסט למחרוזת של מספר שלם נקי"""
    try:
        if pd.isna(val) or val is None or str(val).strip() == '':
            return None
        # המרה לפלוט ואז לאינט כדי להעיף .0
        return str(int(float(str(val).strip())))
    except:
        return str(val).strip() # אם זה לא מספר, מחזיר את הטקסט כמו שהוא

def safe_int(val):
    """מנסה להמיר למספר שלם, מחזיר 0 אם נכשל"""
    try:
        if pd.isna(val): return 0
        return int(float(str(val).strip()))
    except:
        return 0

def extract_clean_data(df):
    """בונה טבלה נקייה מתושבים ללא כפילויות"""
    clean_rows = []
    # המרת שמות עמודות לאותיות קטנות
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # מיפוי שדות
    field_options = {
        'code': ['code', 'order_code', 'קוד', 'קוד מזמין'],
        'lastname': ['lastname', 'last_name', 'משפחה', 'שם משפחה'],
        'father_name': ['father_name', 'father_first_name', 'שם אבא', 'שם פרטי', 'פרטי'],
        'mother_name': ['mother_name', 'mother_first_name', 'שם אמא', 'שם האשה', 'אשה'],
        'streetname': ['streetname', 'street', 'רחוב', 'כתובת'],
        'buildingnumber': ['buildingnumber', 'building_number', 'בנין', 'מס בית', 'מספר בית'],
        'entrance': ['entrance', 'כניסה'],
        'apartmentnumber': ['apartmentnumber', 'apartment_number', 'דירה', 'מספר דירה'],
        'phone': ['phone', 'home_phone', 'טלפון', 'טלפון בבית'],
        'mobile': ['mobile', 'נייד', 'נייד 1', 'סלולרי'],
        'mobile2': ['mobile2', 'נייד 2', 'נייד אשה', 'סלולרי 2'],
        'email': ['email', 'מייל', 'אימייל', 'דואר אלקטרוני'],
        'standing_order': ['standing_order', 'הוראת קבע']
    }

    for _, row in df.iterrows():
        new_row = {}
        for field, options in field_options.items():
            value = None
            for opt in options:
                if opt in df.columns:
                    val = row[opt]
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    if pd.notnull(val) and str(val).strip() != '':
                        value = val
                        break
            new_row[field] = value
        
        # ניקוי וטיוב נתונים קריטי
        new_row['code'] = safe_int(new_row['code']) if new_row['code'] else None
        new_row['standing_order'] = safe_int(new_row['standing_order'])
        
        # המרה למחרוזות
        for k in ['lastname', 'father_name', 'mother_name', 'streetname', 'buildingnumber', 
                  'entrance', 'apartmentnumber', 'phone', 'mobile', 'mobile2', 'email']:
            new_row[k] = str(new_row[k]) if new_row[k] is not None else ''
                
        clean_rows.append(new_row)
        
    return clean_rows

# --- פונקציה לתיקון אוטומטי של המסד ---
def auto_fix_database(cur):
    try:
        cur.execute("DROP INDEX IF EXISTS public.ux_person_unique_phone_address;")
        cur.execute("ALTER TABLE public.temp_residents_csv ADD COLUMN IF NOT EXISTS code integer;")
        cur.execute("ALTER TABLE public.outerapporder ADD COLUMN IF NOT EXISTS sender_phone text;")
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")

        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."raw_to_temp_stage"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        BEGIN
          TRUNCATE TABLE public.temp_residents_csv RESTART IDENTITY;
          INSERT INTO public.street (streetname)
          SELECT DISTINCT TRIM(r.streetname) FROM public.raw_residents_csv r
          WHERE TRIM(r.streetname) IS NOT NULL AND TRIM(r.streetname) <> ''
            AND NOT EXISTS (SELECT 1 FROM public.street s WHERE TRIM(s.streetname) = TRIM(r.streetname));

          INSERT INTO public.temp_residents_csv (
              code, lastname, father_name, mother_name, streetname, streetcode,
              buildingnumber, entrance, apartmentnumber, email, phone, mobile, mobile2, standing_order
          )
          SELECT 
              NULLIF(regexp_replace(r.code, '[^0-9]', '', 'g'), '')::int,
              TRIM(r.lastname), TRIM(r.father_name), TRIM(r.mother_name), TRIM(r.streetname),
              s.streetcode,
              TRIM(r.buildingnumber), TRIM(r.entrance), TRIM(r.apartmentnumber),
              normalize_email(r.email), format_il_phone(r.phone), format_il_phone(r.mobile), format_il_phone(r.mobile2),
              r.standing_order
          FROM public.raw_residents_csv r
          LEFT JOIN public.street s ON TRIM(s.streetname) = TRIM(r.streetname);
        END;
        $func$;
        """)

        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."process_residents_csv"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        DECLARE
            rec RECORD;
            existing_person INT;
            target_id INT;
        BEGIN
            PERFORM setval('public.person_personid_seq', (SELECT COALESCE(MAX(personid), 0) + 1 FROM public.person), false);

            FOR rec IN SELECT * FROM temp_residents_csv LOOP
                target_id := rec.code;
                existing_person := NULL;

                IF target_id IS NOT NULL THEN
                    SELECT personid INTO existing_person FROM person WHERE personid = target_id;
                END IF;

                IF existing_person IS NULL THEN
                    SELECT personid INTO existing_person FROM person 
                    WHERE format_il_phone(phone) = format_il_phone(rec.phone) LIMIT 1;
                END IF;

                IF existing_person IS NOT NULL THEN
                    UPDATE person SET 
                        lastname = COALESCE(rec.lastname, lastname),
                        email = COALESCE(rec.email, email),
                        standing_order = COALESCE(rec.standing_order, standing_order),
                        streetcode = COALESCE(rec.streetcode, streetcode)
                    WHERE personid = existing_person;
                    UPDATE temp_residents_csv SET status = 'עודכן' WHERE temp_id = rec.temp_id;
                ELSE
                    IF target_id IS NOT NULL THEN
                        INSERT INTO person(
                            personid, lastname, father_name, mother_name, streetcode, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order
                        ) VALUES (
                            target_id, rec.lastname, rec.father_name, rec.mother_name, rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber, rec.phone, rec.mobile, rec.mobile2, rec.email, rec.standing_order
                        ) 
                        ON CONFLICT (personid) DO UPDATE SET lastname = EXCLUDED.lastname;
                        PERFORM setval('public.person_personid_seq', (SELECT COALESCE(MAX(personid), 0) + 1 FROM public.person), false);
                    ELSE
                        PERFORM setval('public.person_personid_seq', (SELECT COALESCE(MAX(personid), 0) + 1 FROM public.person), false);
                        INSERT INTO person(
                            lastname, father_name, mother_name, streetcode, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order
                        ) VALUES (
                            rec.lastname, rec.father_name, rec.mother_name, rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber, rec.phone, rec.mobile, rec.mobile2, rec.email, rec.standing_order
                        );
                    END IF;
                    UPDATE temp_residents_csv SET status = 'נוסף' WHERE temp_id = rec.temp_id;
                END IF;
            END LOOP;
        END;
        $func$;
        """)
    except Exception as e:
        print(f"Auto-fix warning: {e}")

@app.route('/')
def index():
    conn = get_db_connection()
    if not conn: return "DB Error", 500
    cur = conn.cursor(cursor_factory=RealDictCursor)
    auto_fix_database(cur)
    cur.execute("SELECT COUNT(*) as count FROM person")
    families = cur.fetchone()['count']
    cur.execute('SELECT COUNT(*) as count FROM "Order"')
    orders = cur.fetchone()['count']
    cur.execute("SELECT COUNT(*) as count FROM outerapporder WHERE status='waiting'")
    pending = cur.fetchone()['count']
    cur.close()
    conn.close()
    return render_template('index.html', families=families, orders=orders, pending=pending)

@app.route('/reset_db', methods=['POST'])
def reset_db():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('TRUNCATE TABLE "payment_ledger", "Order", "outerapporder", "person_archive", "raw_residents_csv", "temp_residents_csv" CASCADE;')
        cur.execute('DELETE FROM "person";')
        cur.execute("ALTER SEQUENCE public.person_personid_seq RESTART WITH 1;")
        cur.execute("ALTER SEQUENCE public.order_id_seq RESTART WITH 1;")
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")
        flash('המערכת אופסה בהצלחה!', 'success')
    except Exception as e:
        flash(f'שגיאה: {e}', 'danger')
    conn.close()
    return redirect(url_for('index'))

@app.route('/residents', methods=['GET', 'POST'])
def residents():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    auto_fix_database(cur)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            try:
                df = None
                filename = file.filename.lower()
                
                if filename.endswith('.xlsx'):
                    try: df = pd.read_excel(file, engine='openpyxl', header=None)
                    except: pass
                
                if df is None:
                    encodings = ['utf-8', 'cp1255', 'windows-1252']
                    for enc in encodings:
                        try:
                            file.seek(0)
                            df = pd.read_csv(file, encoding=enc, header=None)
                            if df.shape[1] > 1: break
                        except: continue
                
                if df is None:
                    flash('לא ניתן לקרוא את הקובץ.', 'danger')
                else:
                    header_index = -1
                    for i, row in df.head(20).iterrows():
                        row_text = " ".join([str(val) for val in row.values]).lower()
                        if 'lastname' in row_text or 'last_name' in row_text or 'משפחה' in row_text or 'code' in row_text:
                            header_index = i
                            break
                    
                    if header_index > -1:
                        df.columns = df.iloc[header_index]
                        df = df[header_index + 1:].reset_index(drop=True)
                    else:
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)

                    clean_data = extract_clean_data(df)

                    cur.execute("TRUNCATE TABLE raw_residents_csv RESTART IDENTITY")
                    data_values = []
                    for row in clean_data:
                        data_values.append((
                            row['code'], row['lastname'], row['father_name'], row['mother_name'],
                            row['streetname'], row['buildingnumber'], row['entrance'], row['apartmentnumber'],
                            row['phone'], row['mobile'], row['mobile2'], row['email'], row['standing_order']
                        ))

                    insert_query = """
                    INSERT INTO raw_residents_csv 
                    (code, lastname, father_name, mother_name, streetname, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    execute_batch(cur, insert_query, data_values)
                    cur.execute("SELECT raw_to_temp_stage()")
                    cur.execute("SELECT process_residents_csv()")
                    
                    flash(f'קובץ {filename} נקלט בהצלחה! {len(data_values)} רשומות.', 'success')
            except Exception as e:
                flash(f'שגיאה: {str(e)}', 'danger')

    cur.execute("SELECT * FROM missing_streets_log ORDER BY id DESC LIMIT 10")
    missing_streets = cur.fetchall()
    cur.execute("SELECT * FROM person_archive ORDER BY created_at DESC LIMIT 20")
    archive_log = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('residents.html', missing_streets=missing_streets, archive_log=archive_log)

@app.route('/orders', methods=['GET', 'POST'])
def orders():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'upload':
            file = request.files['file']
            if file:
                try:
                    df = None
                    if file.filename.lower().endswith('.csv'):
                        try: file.seek(0); df = pd.read_csv(file, encoding='utf-8')
                        except: file.seek(0); df = pd.read_csv(file, encoding='cp1255')
                    else: file.seek(0); df = pd.read_excel(file, engine='openpyxl')

                    # נרמול עמודות הזמנות
                    df.columns = [str(c).strip().lower() for c in df.columns]
                    data_values = []
                    
                    # חיפוש עמודות
                    col_sender = next((c for c in df.columns if 'sender' in c or 'order_code' in c or 'code' in c or 'קוד' in c), None)
                    col_invitees = next((c for c in df.columns if 'invite' in c or 'guest' in c or 'מוזמנים' in c), None)
                    col_phone = next((c for c in df.columns if 'phone' in c or 'mobile' in c or 'טלפון' in c), None)
                    col_pkg = next((c for c in df.columns if 'package' in c or 'גודל' in c), None)

                    for _, row in df.iterrows():
                        # === התיקון הקריטי: ניקוי ה-".0" מקוד השולח ===
                        sender = clean_int_str(row[col_sender]) if col_sender else ''
                        invitees = str(row[col_invitees]) if col_invitees and pd.notnull(row[col_invitees]) else ''
                        phone = str(row[col_phone]) if col_phone and pd.notnull(row[col_phone]) else None
                        pkg = str(row[col_pkg]) if col_pkg and pd.notnull(row[col_pkg]) else 'סמלי'
                        
                        if sender or invitees:
                            data_values.append((sender, invitees, pkg, 'upload', phone))

                    insert_query = "INSERT INTO outerapporder (sender_code, invitees, package_size, origin, sender_phone) VALUES (%s, %s, %s, %s, %s)"
                    execute_batch(cur, insert_query, data_values)
                    flash(f'נטענו {len(data_values)} הזמנות.', 'info')
                except Exception as e: flash(f'שגיאה: {e}', 'danger')
        elif action == 'distribute':
            try: cur.execute("SELECT distribute_all_outer_orders()"); flash('הפצה בוצעה!', 'success')
            except Exception as e: flash(f'שגיאה: {e}', 'danger')
    
    cur.execute("SELECT * FROM v_outer_distribution_status LIMIT 50")
    status_rows = cur.fetchall()
    cur.execute("SELECT * FROM outerapporder_error_log ORDER BY id DESC LIMIT 20")
    errors = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('orders.html', status_rows=status_rows, errors=errors)

@app.route('/report/<view_name>')
def report(view_name):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    search_term = request.args.get('search', '')
    query = f'SELECT * FROM "{view_name}"'
    params = {}
    if search_term:
        if view_name == 'v_accounts_summary': query += " WHERE sender_name LIKE %(search)s"
        elif view_name == 'v_families_balance': query += " WHERE lastname LIKE %(search)s"
        elif view_name == 'v_orders_details': query += " WHERE sender_name LIKE %(search)s OR getter_name LIKE %(search)s"
        elif view_name == 'v_packages_per_building': query += " WHERE streetname LIKE %(search)s"
        params['search'] = f'%{search_term}%'
    else: query += " LIMIT 100"
    
    try: cur.execute(query, params); rows = cur.fetchall(); columns = rows[0].keys() if rows else []
    except: rows, columns = [], []
    cur.close()
    conn.close()
    return render_template('report.html', view_name=view_name, rows=rows, columns=columns)

@app.route('/export/<view_name>')
def export_csv(view_name):
    conn = get_db_connection()
    df = pd.read_sql_query(f'SELECT * FROM "{view_name}"', conn)
    conn.close()
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={view_name}.csv"})

@app.route('/apply_autoreturn', methods=['POST'])
def apply_autoreturn():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.callproc('apply_autoreturn_for', [int(request.form.get('family_id'))])
    conn.close()
    return redirect(url_for('report', view_name='v_families_balance'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)