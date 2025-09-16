import psycopg2
from psycopg2 import sql
import time

DB_NAME = "DBTDatabase"
DB_HOST = "localhost"
DB_USER = "postgres"   # change to your postgres user
DB_PASS = "root"   # change to your postgres password
DB_PORT = 5432



def create_database():
    conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, port=DB_PORT, dbname="postgres")
    conn.autocommit = True  # needed to CREATE DATABASE outside a transaction
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DB_NAME,))
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
    cur.close()
    conn.close()
    time.sleep(1)

create_database()

a = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, port=DB_PORT, dbname=DB_NAME)
cur = a.cursor()


# Create tables (if not exist)


def create_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Students (
        student_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE,
        phone VARCHAR(15) UNIQUE,
        state VARCHAR(50) NOT NULL,
        college VARCHAR(100)
    )
    """)
    a.commit()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS BankAccounts (
        account_id SERIAL PRIMARY KEY,
        student_id INT NOT NULL,
        account_number VARCHAR(50) UNIQUE NOT NULL,
        bank_name VARCHAR(100) NOT NULL,
        CONSTRAINT fk_student FOREIGN KEY (student_id) REFERENCES Students(student_id) ON DELETE CASCADE
    )
    """)
    a.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS AccountStatus (
        status_id SERIAL PRIMARY KEY,
        account_id INT NOT NULL,
        aadhaar_linked BOOLEAN DEFAULT FALSE,
        dbt_enabled BOOLEAN DEFAULT FALSE,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_account FOREIGN KEY (account_id) REFERENCES BankAccounts(account_id) ON DELETE CASCADE
    )
    """)
    a.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Schemes (
        scheme_id SERIAL PRIMARY KEY,
        scheme_name VARCHAR(100) NOT NULL,
        department VARCHAR(100)
    )
    """)
    a.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Beneficiaries (
        ben_id SERIAL PRIMARY KEY,
        student_id INT NOT NULL,
        scheme_id INT NOT NULL,
        is_beneficiary BOOLEAN DEFAULT FALSE,
        date_registered DATE,
        CONSTRAINT fk_ben_student FOREIGN KEY (student_id) REFERENCES Students(student_id) ON DELETE CASCADE,
        CONSTRAINT fk_ben_scheme FOREIGN KEY (scheme_id) REFERENCES Schemes(scheme_id) ON DELETE CASCADE
    )
    """)
    a.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS AccountStatusHistory (
        history_id SERIAL PRIMARY KEY,
        account_id INT NOT NULL,
        aadhaar_linked BOOLEAN,
        dbt_enabled BOOLEAN,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_hist_account FOREIGN KEY (account_id) REFERENCES BankAccounts(account_id) ON DELETE CASCADE
    )
    """)
    a.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS AwarenessContent (
        content_id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    a.commit()
    print("Table AwarenessContent OK")

    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_accountstatus_aadhaar ON AccountStatus (aadhaar_linked)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_accountstatus_dbt ON AccountStatus (dbt_enabled)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bankaccounts_student ON BankAccounts (student_id)")
        a.commit()
    except Exception:
        a.rollback()

create_tables()
time.sleep(1)

#Functions

def insert_student():
    print("INSERT STUDENT")
    name = input("Name: ")
    email = input("Email (or leave blank): ") or None
    phone = input("Phone (or leave blank): ") or None
    state = input("State: ")
    college = input("College (or leave blank): ") or None
    try:
        cur.execute("INSERT INTO Students (name, email, phone, state, college) VALUES (%s,%s,%s,%s,%s) RETURNING student_id",
                    (name, email, phone, state, college))
        sid = cur.fetchone()[0]
        a.commit()
        print("Inserted student id:", sid)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def insert_bank_account():
    print("INSERT BANK ACCOUNT")
    student_id = input("Student ID (must exist): ")
    account_number = input("Account Number: ")
    bank_name = input("Bank Name: ")
    try:
        cur.execute("INSERT INTO BankAccounts (student_id, account_number, bank_name) VALUES (%s,%s,%s) RETURNING account_id",
                    (student_id, account_number, bank_name))
        acc_id = cur.fetchone()[0]
        # create default account status row
        try:
            cur.execute("INSERT INTO AccountStatus (account_id, aadhaar_linked, dbt_enabled) VALUES (%s, FALSE, FALSE)", (acc_id,))
        except Exception:
            pass
        a.commit()
        print("Inserted bank account id:", acc_id)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def insert_scheme():
    print("INSERT SCHEME")
    name = input("Scheme name: ")
    dept = input("Department (or leave blank): ") or None
    try:
        cur.execute("INSERT INTO Schemes (scheme_name, department) VALUES (%s,%s) RETURNING scheme_id", (name, dept))
        sid = cur.fetchone()[0]
        a.commit()
        print("Inserted scheme id:", sid)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def show_students():
    cur.execute("SELECT student_id, name, email, phone, state, college FROM Students ORDER BY student_id")
    rows = cur.fetchall()
    if not rows:
        print("No students found.")
    else:
        for r in rows:
            print("ID:", r[0], "Name:", r[1], "Email:", r[2], "Phone:", r[3], "State:", r[4], "College:", r[5])
    time.sleep(1)

def show_bank_accounts():
    cur.execute("""
    SELECT ba.account_id, ba.account_number, ba.bank_name, s.student_id, s.name,
           COALESCE(asu.aadhaar_linked, FALSE), COALESCE(asu.dbt_enabled, FALSE), asu.last_updated
    FROM BankAccounts ba
    LEFT JOIN Students s ON ba.student_id = s.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id = asu.account_id
    ORDER BY ba.account_id
    """)
    rows = cur.fetchall()
    if not rows:
        print("No bank accounts.")
    else:
        for r in rows:
            print("AccID:", r[0], "AccNo:", r[1], "Bank:", r[2], "StudentID:", r[3], "StudentName:", r[4],
                  "AadhaarLinked:", r[5], "DBT:", r[6], "Updated:", r[7])
    time.sleep(1)

def show_students_pending_dbt():
    print("STUDENTS PENDING DBT (no DBT enabled)")
    cur.execute("""
    SELECT s.student_id, s.name, ba.account_id, COALESCE(asu.dbt_enabled, FALSE)
    FROM Students s
    LEFT JOIN BankAccounts ba ON s.student_id = ba.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id = asu.account_id
    WHERE COALESCE(asu.dbt_enabled, FALSE) = FALSE
    ORDER BY s.student_id
    """)
    rows = cur.fetchall()
    if not rows:
        print("No pending students.")
    else:
        for r in rows:
            print("SID:", r[0], "Name:", r[1], "AccountID:", r[2], "DBT_enabled:", r[3])
    time.sleep(1)

def update_student():
    print("UPDATE STUDENT")
    sid = input("Student ID to update: ")
    cur.execute("SELECT student_id, name, email, phone, state, college FROM Students WHERE student_id=%s", (sid,))
    row = cur.fetchone()
    if not row:
        print("Student not found.")
        return
    print("Current:", row)
    name = input("New name (leave blank to keep): ") or row[1]
    email = input("New email (leave blank to keep): ") or row[2]
    phone = input("New phone (leave blank to keep): ") or row[3]
    state = input("New state (leave blank to keep): ") or row[4]
    college = input("New college (leave blank to keep): ") or row[5]
    try:
        cur.execute("UPDATE Students SET name=%s, email=%s, phone=%s, state=%s, college=%s WHERE student_id=%s",
                    (name, email, phone, state, college, sid))
        a.commit()
        print("Student updated.")
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def update_account_status():
    print("UPDATE ACCOUNT STATUS")
    acc = input("Account ID to update: ")
    # ensure row exists
    cur.execute("SELECT status_id, aadhaar_linked, dbt_enabled FROM AccountStatus WHERE account_id=%s", (acc,))
    st = cur.fetchone()
    if not st:
        # create new
        try:
            cur.execute("INSERT INTO AccountStatus (account_id, aadhaar_linked, dbt_enabled) VALUES (%s, FALSE, FALSE)", (acc,))
            a.commit()
            cur.execute("SELECT status_id, aadhaar_linked, dbt_enabled FROM AccountStatus WHERE account_id=%s", (acc,))
            st = cur.fetchone()
            print("Created AccountStatus row.")
        except Exception as e:
            a.rollback()
            print("Error creating status:", e)
            return
    print("Current status:", st)
    aad = input("Set Aadhaar linked? (y/n) leave blank keep current: ").lower()
    dbt = input("Set DBT enabled? (y/n) leave blank keep current: ").lower()
    def val(inp, curval):
        if inp == 'y': return True
        if inp == 'n': return False
        return curval
    new_aad = val(aad, st[1])
    new_dbt = val(dbt, st[2])
    try:
        # insert history
        cur.execute("INSERT INTO AccountStatusHistory (account_id, aadhaar_linked, dbt_enabled) VALUES (%s,%s,%s)",
                    (acc, new_aad, new_dbt))
        # update status and last_updated explicitly
        cur.execute("UPDATE AccountStatus SET aadhaar_linked=%s, dbt_enabled=%s, last_updated = CURRENT_TIMESTAMP WHERE account_id=%s",
                    (new_aad, new_dbt, acc))
        a.commit()
        print("Account status updated and history recorded.")
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def update_scheme():
    print("UPDATE SCHEME")
    sid = input("Scheme ID to update: ")
    cur.execute("SELECT scheme_id, scheme_name, department FROM Schemes WHERE scheme_id=%s", (sid,))
    row = cur.fetchone()
    if not row:
        print("Scheme not found.")
        return
    print("Current:", row)
    name = input("New name (leave blank keep): ") or row[1]
    dept = input("New dept (leave blank keep): ") or row[2]
    try:
        cur.execute("UPDATE Schemes SET scheme_name=%s, department=%s WHERE scheme_id=%s", (name, dept, sid))
        a.commit()
        print("Scheme updated.")
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def update_awareness_content():
    print("UPDATE/INSERT AWARENESS CONTENT")
    cid = input("Content ID to update (0 to create new): ")
    try:
        cid = int(cid)
    except:
        print("Invalid id.")
        return
    if cid == 0:
        title = input("Title: ")
        content = input("Content text (plain): ")
        try:
            cur.execute("INSERT INTO AwarenessContent (title, content) VALUES (%s,%s) RETURNING content_id", (title, content))
            newid = cur.fetchone()[0]
            a.commit()
            print("Inserted content id:", newid)
        except Exception as e:
            a.rollback()
            print("Error:", e)
    else:
        cur.execute("SELECT content_id, title, content FROM AwarenessContent WHERE content_id=%s", (cid,))
        row = cur.fetchone()
        if not row:
            print("Content not found.")
            return
        print("Current:", row)
        title = input("New title (leave blank keep): ") or row[1]
        content = input("New content (leave blank keep): ") or row[2]
        try:
            cur.execute("UPDATE AwarenessContent SET title=%s, content=%s WHERE content_id=%s", (title, content, cid))
            a.commit()
            print("Updated awareness content.")
        except Exception as e:
            a.rollback()
            print("Error:", e)
    time.sleep(1)

def delete_student():
    print("DELETE STUDENT")
    sid = input("Student ID to delete: ")
    conf = input("Type YES to confirm delete: ")
    if conf != "YES":
        print("Abort.")
        return
    try:
        cur.execute("DELETE FROM Students WHERE student_id=%s", (sid,))
        a.commit()
        print("Deleted. Rows affected:", cur.rowcount)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def delete_scheme():
    print("DELETE SCHEME")
    sid = input("Scheme ID to delete: ")
    conf = input("Type YES to confirm delete: ")
    if conf != "YES":
        print("Abort.")
        return
    try:
        cur.execute("DELETE FROM Schemes WHERE scheme_id=%s", (sid,))
        a.commit()
        print("Deleted. Rows affected:", cur.rowcount)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)

def delete_awareness_content():
    print("DELETE AWARENESS CONTENT")
    cid = input("Content ID to delete: ")
    conf = input("Type YES to confirm delete: ")
    if conf != "YES":
        print("Abort.")
        return
    try:
        cur.execute("DELETE FROM AwarenessContent WHERE content_id=%s", (cid,))
        a.commit()
        print("Deleted. Rows affected:", cur.rowcount)
    except Exception as e:
        a.rollback()
        print("Error:", e)
    time.sleep(1)



# Main loop (menu)


def main():
    while True:
        print("\n******************** DBT DATABASE MENU ********************")
        print("1. Insert Student")
        print("2. Insert Bank Account")
        print("3. Insert Scheme")
        print("4. Show Students")
        print("5. Show Bank Accounts")
        print("6. Show Students Pending DBT")
        print("7. Update Student")
        print("8. Update Account Status")
        print("9. Update Scheme")
        print("10. Update Awareness Content")
        print("11. Delete Student")
        print("12. Delete Scheme")
        print("13. Delete Awareness Content")
        print("0. Exit")
        choice = input("ENTER YOUR CHOICE: ")
        if choice == "1":
            insert_student()
        elif choice == "2":
            insert_bank_account()
        elif choice == "3":
            insert_scheme()
        elif choice == "4":
            show_students()
        elif choice == "5":
            show_bank_accounts()
        elif choice == "6":
            show_students_pending_dbt()
        elif choice == "7":
            update_student()
        elif choice == "8":
            update_account_status()
        elif choice == "9":
            update_scheme()
        elif choice == "10":
            update_awareness_content()
        elif choice == "11":
            delete_student()
        elif choice == "12":
            delete_scheme()
        elif choice == "13":
            delete_awareness_content()
        elif choice == "0":
            print("Exiting.")
            break
        else:
            print("Please enter a valid number.")

if __name__ == "__main__":
    main()
    cur.close()
    a.close()
