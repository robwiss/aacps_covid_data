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
    df_summary['New Student Cases'] = df_summary['Active Student Cases'] - (df_summary['Active Student Cases'].shift(1, fill_value=0) - df_summary['Student Cases Newly Resolved'])
    df_summary['Change in Staff Quarantined'] = df_summary['Quarantined Staff'] - df_summary['Quarantined Staff'].shift(1, fill_value=0)
    df_summary['% Change in Staff Quarantined'] = (df_summary['Quarantined Staff'] - df_summary['Quarantined Staff'].shift(1, fill_value=0)) / df_summary['Quarantined Staff'].shift(1, fill_value=0) * 100
    df_summary['Change in Active Staff Cases'] = df_summary['Active Staff Cases'] - df_summary['Active Staff Cases'].shift(1, fill_value=0)
    df_summary['% Change in Active Staff Cases'] = (df_summary['Active Staff Cases'] - df_summary['Active Staff Cases'].shift(1, fill_value=0)) / df_summary['Active Staff Cases'].shift(1, fill_value=0) * 100
    df_summary['Change in Total Staff Cases'] = df_summary['Total Staff Cases'] - df_summary['Total Staff Cases'].shift(1, fill_value=0)
    df_summary['% Change in Total Staff Cases'] = (df_summary['Total Staff Cases'] - df_summary['Total Staff Cases'].shift(1, fill_value=0)) / df_summary['Total Staff Cases'].shift(1, fill_value=0) * 100
    df_summary['Staff Cases Resolved'] = df_summary['Total Staff Cases'] - df_summary['Active Staff Cases']
    df_summary['Staff Cases Newly Resolved'] = df_summary['Staff Cases Resolved'] - df_summary['Staff Cases Resolved'].shift(1, fill_value=0)
    df_summary['New Staff Cases'] = df_summary['Active Staff Cases'] - (df_summary['Active Staff Cases'].shift(1, fill_value=0) - df_summary['Staff Cases Newly Resolved'])

    return df_summary

class Model3:
    def __init__(self,
                 df_summary: pd.DataFrame,
                 start_date: pd.Timestamp,
                 total_students: int = 0,
                 r0: int = 0,
                 quarantine_factor: float = 0,
                 quarantine_success: float = 0,
                 pct_long_covid: float = 0,
                 pct_severe: float = 0,
                 pct_death: float = 0,
                 quarantine_period: int = 0):
        if start_date not in df_summary.index:
            raise IndexError

        self.start_date = start_date
        self.total_students = total_students
        self.r0 = r0
        self.quarantine_factor = quarantine_factor
        self.pct_long_covid = pct_long_covid
        self.pct_severe = pct_severe
        self.pct_death = pct_death
        self.quarantine_period = quarantine_period
        self.quarantine_success = quarantine_success
        series = df_summary.loc[start_date, ['Active Student Cases', 'Total Student Cases', 'Active Staff Cases', 'Total Staff Cases', 'Quarantined Students', 'Quarantined Staff']]
        self.df = pd.DataFrame(series.T).T
        self.df.rename(columns = {'Active Student Cases': 'Active Cases', 'Total Student Cases': 'Total Cases', 'Quarantined Students': 'Quarantined'}, inplace = True)
        self.df['New Cases'] = df_summary.loc[start_date]['New Student Cases']
        self.df['New Quarantined'] = self.df['Quarantined']
        self.df = self.df.reindex(columns=['New Cases', 'Active Cases', 'Total Cases', 'New Quarantined', 'Quarantined'])

    def tick(self):
        last_day = self.df.index[-1]
        today = last_day + pd.Timedelta(1, 'day')

        # figure the new cases from spread in school
        # also figure the new cases among those quarantined
        new_cases_in_school = self.r0 * self.df['New Cases'][-1] * (1 - self.quarantine_success)
        new_cases_among_quarantined = self.r0 * self.df['New Cases'][-1] * self.quarantine_success
        # remove (1 / quarantine_period) * quarantined from the quarantine amt every day
        pct_quarantine_resolved_in_period = (1. / self.quarantine_period)
        active_cases = (1 - pct_quarantine_resolved_in_period) * self.df['Active Cases'][-1] + new_cases
        total_cases = self.df['Total Cases'][-1] + new_cases
        new_quarantined = new_cases * self.quarantine_factor
        quarantined = active_cases * self.quarantine_factor
        resolved_cases = total_cases - active_cases
        if today.day_of_week in [0, 6]:
            # on sundays and mondays no case growth because kids aren't in school the day before
            # this means the model's saturday case growth should match the reported data's monday case growth
            r_rel = 0
        else:
            r_rel = pct_susceptible_students / self.df['% Susceptible Students Remaining'][0]
        r = r_rel * self.df['R_tick'][0]
        self.df.loc[today] = [
            susceptible_students_remaining,
            pct_susceptible_students,
            r_rel,
            r,
            new_cases,
            active_cases,
            total_cases,
            new_quarantined,
            quarantined
        ]


class Model2:
    def __init__(self,
                 df_summary: pd.DataFrame,
                 start_date: pd.Timestamp,
                 total_students: int = 0,
                 seroprevalence: int = 0,
                 r0: float = 0,
                 quarantine_factor: int = 0,
                 pct_long_covid: float = 0,
                 pct_severe: float = 0,
                 pct_death: float = 0,
                 quarantine_period: int = 0):
        if start_date not in df_summary.index:
            raise IndexError

        self.start_date = start_date
        self.total_students = total_students
        self.seroprevalence = seroprevalence
        self.r0 = r0
        self.quarantine_factor = quarantine_factor
        self.pct_long_covid = pct_long_covid
        self.pct_severe = pct_severe
        self.pct_death = pct_death
        self.quarantine_period = quarantine_period
        series = df_summary.loc[start_date, ['Active Student Cases', 'Total Student Cases', 'Active Staff Cases', 'Total Staff Cases', 'Quarantined Students', 'Quarantined Staff']]
        self.df = pd.DataFrame(series.T).T
        self.df.rename(columns = {'Active Student Cases': 'Active Cases', 'Total Student Cases': 'Total Cases', 'Quarantined Students': 'Quarantined'}, inplace = True)
        self.df['Susceptible Students Remaining'] = [
            total_students * (1 - seroprevalence) - 
              df_summary.loc[start_date]['Quarantined Students'] -
              df_summary.loc[start_date]['Student Cases Resolved']
        ]
        self.df['% Susceptible Students Remaining'] = [self.df['Susceptible Students Remaining'][0] / total_students]
        self.df['Rel pop dens'] = self.df['% Susceptible Students Remaining'][0] / self.df['% Susceptible Students Remaining'].shift(1, fill_value=self.df['% Susceptible Students Remaining'][0])
        self.df['R_tick'] = [r0 * self.df['Rel pop dens'][-1]]
        self.df['New Cases'] = df_summary.loc[start_date]['New Student Cases']
        self.df['New Quarantined'] = self.df['Quarantined']
        self.df = self.df.reindex(columns=['Susceptible Students Remaining', '% Susceptible Students Remaining', 'Rel pop dens', 'R_tick', 'New Cases', 'Active Cases', 'Total Cases', 'New Quarantined', 'Quarantined'])

    def tick(self):
        last_day = self.df.index[-1]
        today = last_day + pd.Timedelta(1, 'day')

        new_cases = self.df['R_tick'][-1] * self.df['New Cases'][-5:].mean()
        # remove (1 / quarantine_period) * quarantined from the quarantine amt every day
        pct_quarantine_resolved_in_period = (1. / self.quarantine_period)
        active_cases = (1 - pct_quarantine_resolved_in_period) * self.df['Active Cases'][-1] + new_cases
        total_cases = self.df['Total Cases'][-1] + new_cases
        new_quarantined = new_cases * self.quarantine_factor
        quarantined = active_cases * self.quarantine_factor
        resolved_cases = total_cases - active_cases
        susceptible_students_remaining = self.total_students * (1 - self.seroprevalence) - resolved_cases - quarantined
        pct_susceptible_students = susceptible_students_remaining / self.total_students
        if today.day_of_week in [0, 6]:
            # on sundays and mondays no case growth because kids aren't in school the day before
            # this means the model's saturday case growth should match the reported data's monday case growth
            r_rel = 0
        else:
            r_rel = pct_susceptible_students / self.df['% Susceptible Students Remaining'][0]
        r = r_rel * self.df['R_tick'][0]
        self.df.loc[today] = [
            susceptible_students_remaining,
            pct_susceptible_students,
            r_rel,
            r,
            new_cases,
            active_cases,
            total_cases,
            new_quarantined,
            quarantined
        ]

def model1(df_summary: pd.DataFrame, total_students: int = 0, seroprevalence: int = 0, r0: int = 0, quarantine_factor: int = 0, pct_long_covid: float = 0, pct_severe: float = 0, pct_death: float = 0, quarantine_period: int = 0):
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

