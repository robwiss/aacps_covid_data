import pandas as pd
import sqlite3
import numpy as np
from datetime import date, timedelta

def summarize(db_conn: sqlite3.Connection, total_students: int):
    df_summary = pd.read_sql_query('''
    SELECT * FROM (
      SELECT Datestamp,
             SUM(Active_Student) AS "Active Student Cases",
             SUM(Total_Student) AS "Total Student Cases",
             SUM(Active_Staff) AS "Active Staff Cases",
             SUM(Total_Staff) AS "Total Staff Cases"
      FROM cases
      GROUP BY Datestamp
    ) AS case_summary
    LEFT JOIN (
      SELECT Datestamp,
             Students AS "Quarantined Students",
             Staff AS "Quarantined Staff"
      FROM quarantines
    ) USING(Datestamp);
    ''', db_conn, index_col='Datestamp', parse_dates='Datestamp',
    dtype={'Active Student Cases': np.int32, 'Total Student Cases': np.int32, 'Active Staff Cases': np.int32, 'Total Staff Cases': np.int32}
    )
    df_summary['Student Quarantine Factor'] = df_summary['Quarantined Students'] / df_summary['Active Student Cases']
    df_summary['Staff Quarantine Factor'] = df_summary['Quarantined Staff'] / df_summary['Active Staff Cases']
    df_summary['Change in Students Quarantined'] = df_summary['Quarantined Students'] - df_summary['Quarantined Students'].shift(1, fill_value=0)
    df_summary['% Change in Students Quarantined'] = (df_summary['Quarantined Students'] - df_summary['Quarantined Students'].shift(1, fill_value=0)) / df_summary['Quarantined Students'].shift(1, fill_value=0) * 100
    df_summary['% Students Quarantined'] = (df_summary['Quarantined Students'] / total_students) * 100
    df_summary['Change in Active Student Cases'] = df_summary['Active Student Cases'] - df_summary['Active Student Cases'].shift(1, fill_value=0)
    df_summary['% Change in Active Student Cases'] = (df_summary['Active Student Cases'] - df_summary['Active Student Cases'].shift(1, fill_value=0)) / df_summary['Active Student Cases'].shift(1, fill_value=0) * 100
    df_summary['% Students with Active Cases'] = df_summary['Active Student Cases'] / total_students * 100
    df_summary['Change in Total Student Cases'] = df_summary['Total Student Cases'] - df_summary['Total Student Cases'].shift(1, fill_value=0)
    df_summary['% Change in Total Student Cases'] = (df_summary['Total Student Cases'] - df_summary['Total Student Cases'].shift(1, fill_value=0)) / df_summary['Total Student Cases'].shift(1, fill_value=0) * 100
    df_summary['% of Students in Total Student Cases'] = df_summary['Total Student Cases'] / total_students * 100
    df_summary['Student Cases Resolved'] = df_summary['Total Student Cases'] - df_summary['Active Student Cases']
    df_summary['Student Cases Newly Resolved'] = df_summary['Student Cases Resolved'] - df_summary['Student Cases Resolved'].shift(1, fill_value=0)
    df_summary['Change in Staff Quarantined'] = df_summary['Quarantined Staff'] - df_summary['Quarantined Staff'].shift(1, fill_value=0)
    df_summary['% Change in Staff Quarantined'] = (df_summary['Quarantined Staff'] - df_summary['Quarantined Staff'].shift(1, fill_value=0)) / df_summary['Quarantined Staff'].shift(1, fill_value=0) * 100
    df_summary['Change in Active Staff Cases'] = df_summary['Active Staff Cases'] - df_summary['Active Staff Cases'].shift(1, fill_value=0)
    df_summary['% Change in Active Staff Cases'] = (df_summary['Active Staff Cases'] - df_summary['Active Staff Cases'].shift(1, fill_value=0)) / df_summary['Active Staff Cases'].shift(1, fill_value=0) * 100
    df_summary['Change in Total Staff Cases'] = df_summary['Total Staff Cases'] - df_summary['Total Staff Cases'].shift(1, fill_value=0)
    df_summary['% Change in Total Staff Cases'] = (df_summary['Total Staff Cases'] - df_summary['Total Staff Cases'].shift(1, fill_value=0)) / df_summary['Total Staff Cases'].shift(1, fill_value=0) * 100
    df_summary['Staff Cases Resolved'] = df_summary['Total Staff Cases'] - df_summary['Active Staff Cases']
    df_summary['Staff Cases Newly Resolved'] = df_summary['Staff Cases Resolved'] - df_summary['Staff Cases Resolved'].shift(1, fill_value=0)

    return df_summary

def model(df_summary: pd.DataFrame, total_students: int = 0, seroprevalence: int = 0, r0: int = 0, quarantine_factor: int = 0, pct_long_covid: float = 0, pct_severe: float = 0, pct_death: float = 0, quarantine_period: int = 0):
    pct_quarantine_resolved_in_period = 7 / quarantine_period # 7 days elapsed of 'quarantine_period' days
    
    df_proj = df_summary[:1].loc[:, ['Active Student Cases', 'Total Student Cases', 'Active Staff Cases', 'Total Staff Cases', 'Quarantined Students', 'Quarantined Staff']]
    df_proj.rename(columns = {'Active Student Cases': 'Active Cases', 'Total Student Cases': 'Total Cases', 'Quarantined Students': 'Quarantined'}, inplace = True)
    df_proj['Week'] = [1]
    df_proj['Susceptible Students Remaining'] = [
        total_students * (1 - seroprevalence) - 
          df_summary.loc[df_proj.index[0].strftime('%Y-%m-%d')]['Quarantined Students'] -
          df_summary.loc[df_proj.index[0].strftime('%Y-%m-%d')]['Student Cases Resolved']
    ]
    df_proj['% Susceptible Students Remaining'] = [df_proj['Susceptible Students Remaining'][0] / total_students]
    df_proj['Rel pop dens'] = [1]
    df_proj['R'] = [r0 * df_proj['Rel pop dens'][0]]
    df_proj['New Cases'] = df_summary['Change in Active Student Cases'][0]
    df_proj['New Quarantined'] = df_proj['Quarantined']
    df_proj = df_proj.reindex(columns=['Week', 'Susceptible Students Remaining', '% Susceptible Students Remaining', 'Rel pop dens', 'R', 'New Cases', 'Active Cases', 'Total Cases', 'New Quarantined', 'Quarantined'])
    
    susceptible_students_remaining =  total_students * (1 - seroprevalence) - \
        df_summary.loc['2021-09-21']['Quarantined Students'] - \
        df_summary.loc['2021-09-21']['Student Cases Resolved']
    pct_susceptible_students = susceptible_students_remaining / total_students
    r_rel = pct_susceptible_students / df_proj['% Susceptible Students Remaining'][-1]
    df_proj.loc[date(2021,9,21)] = [
        2,
        susceptible_students_remaining,
        pct_susceptible_students,
        r_rel,
        r_rel * df_proj['R'][0],
        df_summary['Active Student Cases'][1] - (df_summary['Active Student Cases'][0] - df_summary['Student Cases Newly Resolved'][1]),
        df_summary['Active Student Cases'][1],
        df_summary['Total Student Cases'][1],
        df_summary['Change in Students Quarantined'][1],
        df_summary['Quarantined Students'][1]
    ]
    
    def add_entry(d: date, df: pd.DataFrame):
        new_cases = df['R'][-1] * df['New Cases'][-1]
        active_cases = (1 - pct_quarantine_resolved_in_period) * df['New Cases'][-1] + new_cases
        total_cases = df['Total Cases'][-1] + new_cases
        new_quarantined = new_cases * quarantine_factor
        quarantined = active_cases * quarantine_factor
        susceptible_students_remaining = total_students * (1 - seroprevalence) - (total_cases - active_cases) - quarantined
        pct_susceptible_students = susceptible_students_remaining / total_students
        r_rel = pct_susceptible_students / df['% Susceptible Students Remaining'][0]
        df.loc[d] = [
            df['Week'][-1] + 1,
            susceptible_students_remaining,
            pct_susceptible_students,
            r_rel,
            r_rel * df['R'][0],
            new_cases,
            active_cases,
            total_cases,
            new_quarantined,
            quarantined
        ]

    for d in [date(2021,9,28) + timedelta(days=7*x) for x in range(0,13)]:
        add_entry(d, df_proj)
    
    return df_proj

