import os
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor, execute_batch
from flask import Flask, render_template, request, redirect, url_for, flash, Response

app = Flask(__name__)
app.secret_key = 'super_secret_key_change_me'

# הגדרת חיבור למסד הנתונים
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/mishloach_db")

def get_db_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

# --- פונקציה לתיקון אוטומטי של המסד ---
def auto_fix_database(cur):
    """
    פונקציה זו רצה בכל טעינה.
    היא מוודאת שהטבלאות תקינות, מסירה אילוצים בעייתיים, מעדכנת פונקציות ומסנכרנת מונים.
    """
    try:
        # 1. הסרת האינדקס הייחודי המגביל (כתובת+טלפון)
        cur.execute("DROP INDEX IF EXISTS public.ux_person_unique_phone_address;")

        # 2. הוספת עמודות חסרות לטבלאות זמניות
        cur.execute("ALTER TABLE public.temp_residents_csv ADD COLUMN IF NOT EXISTS code integer;")
        cur.execute("ALTER TABLE public.outerapporder ADD COLUMN IF NOT EXISTS sender_phone text;")

        # 3. יצירת רחוב 999 לגיבוי
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")

        # 4. עדכון פונקציית העברת הנתונים
        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."raw_to_temp_stage"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        BEGIN
          TRUNCATE TABLE public.temp_residents_csv RESTART IDENTITY;
          
          INSERT INTO public.street (streetname)
          SELECT DISTINCT TRIM(r.streetname)
          FROM public.raw_residents_csv r
          WHERE TRIM(r.streetname) IS NOT NULL 
            AND TRIM(r.streetname) <> ''
            AND NOT EXISTS (
                SELECT 1 FROM public.street s 
                WHERE TRIM(s.streetname) = TRIM(r.streetname)
            );

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

        # 5. עדכון פונקציית העיבוד - כולל התיקון הקריטי למונה (Sequence)
        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."process_residents_csv"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        DECLARE
            rec RECORD;
            existing_person INT;
            target_id INT;
        BEGIN
            FOR rec IN SELECT * FROM temp_residents_csv LOOP
                target_id := rec.code;
                existing_person := NULL;

                -- זיהוי לפי ID
                IF target_id IS NOT NULL THEN
                    SELECT personid INTO existing_person FROM person WHERE personid = target_id;
                END IF;

                -- זיהוי לפי טלפון
                IF existing_person IS NULL THEN
                    SELECT personid INTO existing_person FROM person 
                    WHERE format_il_phone(phone) = format_il_phone(rec.phone) LIMIT 1;
                END IF;

                IF existing_person IS NOT NULL THEN
                    -- עדכון
                    UPDATE person SET 
                        lastname = COALESCE(rec.lastname, lastname),
                        email = COALESCE(rec.email, email),
                        standing_order = COALESCE(rec.standing_order, standing_order),
                        streetcode = COALESCE(rec.streetcode, streetcode)
                    WHERE personid = existing_person;
                    UPDATE temp_residents_csv SET status = 'עודכן' WHERE temp_id = rec.temp_id;
                ELSE
                    -- הכנסה חדשה
                    IF target_id IS NOT NULL THEN
                        -- הכנסה ידנית עם ID
                        INSERT INTO person(
                            personid, lastname, father_name, mother_name, streetcode, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order
                        ) VALUES (
                            target_id, rec.lastname, rec.father_name, rec.mother_name, rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber, rec.phone, rec.mobile, rec.mobile2, rec.email, rec.standing_order
                        ) 
                        ON CONFLICT (personid) DO UPDATE SET lastname = EXCLUDED.lastname;
                        
                        -- תיקון קריטי: עדכון המונה מיד אחרי שימוש ידני במספר
                        PERFORM setval(pg_get_serial_sequence('public.person', 'personid'), GREATEST(target_id, (SELECT last_value FROM public.person_personid_seq)));
                    ELSE
                        -- הכנסה אוטומטית
                        -- תיקון קריטי 2: וודא שהמונה מסונכרן עם המקסימום לפני בקשת מספר חדש
                        PERFORM setval(pg_get_serial_sequence('public.person', 'personid'), (SELECT COALESCE(MAX(personid), 0) FROM public.person));
                        
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
    total_families = cur.fetchone()['count']
    cur.execute('SELECT COUNT(*) as count FROM "Order"')
    total_orders = cur.fetchone()['count']
    cur.execute("SELECT COUNT(*) as count FROM outerapporder WHERE status='waiting'")
    pending_external = cur.fetchone()['count']
    cur.close()
    conn.close()
    return render_template('index.html', families=total_families, orders=total_orders, pending=pending_external)

@app.route('/reset_db', methods=['POST'])
def reset_db():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # ניקוי מלא
        cur.execute('TRUNCATE TABLE "payment_ledger", "Order", "outerapporder", "person_archive", "raw_residents_csv", "temp_residents_csv" CASCADE;')
        cur.execute('DELETE FROM "person";')
        
        # איפוס מונים
        cur.execute("ALTER SEQUENCE public.person_personid_seq RESTART WITH 1;")
        cur.execute("ALTER SEQUENCE public.order_id_seq RESTART WITH 1;")
        
        # החזרת נתוני בסיס
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")
        
        flash('המערכת אופסה בהצלחה! כל הנתונים נמחקו.', 'success')
    except Exception as e:
        flash(f'שגיאה באיפוס: {e}', 'danger')
    
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
                    try:
                        file.seek(0)
                        df = pd.read_excel(file, engine='openpyxl', header=None)
                    except: pass
                
                if df is None:
                    encodings = ['cp1255', 'utf-8', 'windows-1252']
                    for enc in encodings:
                        try:
                            file.seek(0)
                            df = pd.read_csv(file, encoding=enc, header=None)
                            break
                        except: continue
                
                if df is None:
                    flash('לא ניתן לקרוא את הקובץ.', 'danger')
                else:
                    # מציאת כותרת
                    header_index = -1
                    for i, row in df.head(20).iterrows():
                        row_text = " ".join([str(val) for val in row.values])
                        if 'משפחה' in row_text or 'last_name' in row_text or 'lastname' in row_text or 'order_code' in row_text:
                            header_index = i
                            break
                    
                    if header_index > -1:
                        df.columns = df.iloc[header_index]
                        df = df[header_index + 1:].reset_index(drop=True)
                    else:
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)

                    df.columns = [str(c).strip() for c in df.columns]
                    
                    column_mapping = {
                        # CODES.xlsx
                        'משפחה': 'lastname', 'שם אבא': 'father_name', 'שם אמא': 'mother_name',
                        'בנין': 'buildingnumber', 'נייד 1': 'mobile', 'הוראת קבע': 'standing_order',
                        
                        # 5785_3.xlsx / 5785_5.csv
                        'last_name': 'lastname', 'father_first_name': 'father_name', 'mother_first_name': 'mother_name',
                        'street': 'streetname', 'building_number': 'buildingnumber', 'apartment_number': 'apartmentnumber',
                        'home_phone': 'phone', 'mobile': 'mobile', 'order_code': 'code',
                        
                        # עברית כללי
                        'שם משפחה': 'lastname', 'רחוב': 'streetname', 'כתובת': 'streetname',
                        'מס בית': 'buildingnumber', 'מספר בית': 'buildingnumber',
                        'טלפון': 'phone', 'נייד': 'mobile', 'נייד 2': 'mobile2',
                        'קוד': 'code', 'דירה': 'apartmentnumber'
                    }
                    df.rename(columns=column_mapping, inplace=True)
                    
                    required = ['code', 'lastname', 'father_name', 'mother_name', 'streetname', 'buildingnumber', 'entrance', 'apartmentnumber', 'phone', 'mobile', 'mobile2', 'email', 'standing_order']
                    for col in required:
                        if col not in df.columns: df[col] = None
                    df = df.where(pd.notnull(df), None)

                    cur.execute("TRUNCATE TABLE raw_residents_csv RESTART IDENTITY")
                    data_values = []
                    for _, row in df.iterrows():
                        so_val = 0
                        try:
                            if row['standing_order']: so_val = int(float(str(row['standing_order'])))
                        except: so_val = 0
                        
                        code_val = str(row['code']) if row['code'] else None

                        val = (
                            code_val,
                            str(row['lastname']) if row['lastname'] else '',
                            str(row['father_name']) if row['father_name'] else '',
                            str(row['mother_name']) if row['mother_name'] else '',
                            str(row['streetname']) if row['streetname'] else '',
                            str(row['buildingnumber']) if row['buildingnumber'] else '',
                            str(row['entrance']) if row['entrance'] else '',
                            str(row['apartmentnumber']) if row['apartmentnumber'] else '',
                            str(row['phone']) if row['phone'] else '',
                            str(row['mobile']) if row['mobile'] else '',
                            str(row['mobile2']) if row['mobile2'] else '',
                            str(row['email']) if row['email'] else '',
                            so_val
                        )
                        data_values.append(val)

                    insert_query = """
                    INSERT INTO raw_residents_csv 
                    (code, lastname, father_name, mother_name, streetname, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    execute_batch(cur, insert_query, data_values)
                    
                    cur.execute("SELECT raw_to_temp_stage()")
                    cur.execute("SELECT process_residents_csv()")
                    
                    flash(f'קובץ {filename} נקלט בהצלחה!', 'success')
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
                        try: file.seek(0); df = pd.read_csv(file, encoding='cp1255')
                        except: file.seek(0); df = pd.read_csv(file, encoding='utf-8')
                    else: file.seek(0); df = pd.read_excel(file, engine='openpyxl')

                    df.columns = [str(c).strip() for c in df.columns]
                    data_values = []
                    for _, row in df.iterrows():
                        sender = ''
                        for col in ['sender_code', 'order_code', 'code', 'קוד', 'קוד מזמין']:
                            if col in df.columns and pd.notnull(row[col]): sender = str(row[col]); break
                        
                        invitees = ''
                        for col in ['invitees', 'guest_list', 'מוזמנים', 'רשימה']:
                            if col in df.columns and pd.notnull(row[col]): invitees = str(row[col]); break

                        phone = None
                        for col in ['mobile', 'phone', 'home_phone', 'נייד', 'טלפון', 'נייד 1']:
                            if col in df.columns and pd.notnull(row[col]): phone = str(row[col]); break
                        
                        pkg = str(row.get('package_size', 'סמלי'))
                        if sender or invitees: data_values.append((sender, invitees, pkg, 'upload', phone))

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