import sqlite3
from typing import Dict, List
from datetime import date
import os

def parse(path: str, d: date) -> List[Dict[str, str]]:
    with open(path, 'r') as f:
        lines = f.readlines()

    records = []
    # first line is the quarantine data for that day
    students_quarantined, staff_quarantined = lines[0].strip().split(',')
    records.append({'Date': d.isoformat(), 'Students_Quarantined': students_quarantined, 'Staff_Quarantined': staff_quarantined})

    # remaining lines are per-school case data
    for i in range(1, len(lines), 5):
        try:
            rec = {
                'Date': d.isoformat(),
                'Primary_Location': lines[i].strip(),
                'Active_Student': lines[i+1].strip(),
                'Total_Student': lines[i+2].strip(),
                'Active_Staff': lines[i+3].strip(),
                'Total_Staff': lines[i+4].strip()
            }
        except Exception as e:
            print('exception at {}:{}'.format(path, i+1))
            raise

        records.append(rec)
    return records

def create_tables(db_conn):
    c = db_conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantines (
            Datestamp TEXT    NOT NULL PRIMARY KEY,
            Students  INTEGER,
            Staff     INTEGER
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS cases (
            School         TEXT    NOT NULL,
            Datestamp      TEXT    NOT NULL,
            Active_Student INTEGER,
            Total_Student  INTEGER,
            Active_Staff   INTEGER,
            Total_Staff    INTEGER,
            PRIMARY KEY(School, Datestamp)
        );
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS school_level (
            School    TEXT                                                                                                                PRIMARY KEY,
            Level     TEXT CHECK ( Level IN ("High", "Middle", "Elementary", "Specialty Centers", "Charter/Contract", "Bus", "Unknown") ) NOT NULL,
            Students  INTEGER,
            Latitude  REAL,
            Longitude REAL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS dates (
            Datestamp TEXT NOT NULL
        )
        """
    )

    db_conn.commit()

def main():
    db_conn = sqlite3.connect('cases.db')
    create_tables(db_conn)

    dates = [date.fromisoformat(path[5:]) for path in os.listdir('.') if path.startswith('data_')]
    for d in dates:
        cur = db_conn.execute("SELECT COUNT(*) FROM dates WHERE Datestamp=(?)", (d.isoformat(), ))
        if cur.fetchone()[0] == 0:
            datafile_path = 'data_{}'.format(d.isoformat())
            parsed = parse(datafile_path, d)
            # After Jan 10, 2022 they stopped publishing the quarantine data
            if d < date(2022, 1, 11):
                db_conn.execute('INSERT INTO quarantines VALUES(:Date, :Students_Quarantined, :Staff_Quarantined)', parsed[0])
            db_conn.executemany('INSERT INTO cases VALUES(:Primary_Location, :Date, :Active_Student, :Total_Student, :Active_Staff, :Total_Staff)', parsed[1:])
            db_conn.execute('INSERT INTO dates VALUES(?)', (d.isoformat(), ))
    
    db_conn.commit()

    with open('school_list_geo', 'r') as f:
        schools_lines = f.readlines()
    schools = [[None if a == '' else a for a in x.strip().split(',')] for x in schools_lines]
    db_conn.executemany('INSERT OR IGNORE INTO school_level VALUES(?, ?, ?, ?, ?)', schools)

    db_conn.commit()

    curr = db_conn.execute("SELECT DISTINCT cases.School FROM cases LEFT JOIN school_level USING(School) WHERE school_level.Level IS NULL")
    unaccounted_schools = curr.fetchall()
    if (len(unaccounted_schools) > 0):
        print('Schools not in school list: ', ', '.join([x[0] for x in unaccounted_schools]))

    db_conn.close()

if __name__ == '__main__':
    main()
