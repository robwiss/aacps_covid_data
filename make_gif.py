import pandas as pd
pd.set_option('plotting.backend', 'pandas_bokeh')
import sqlite3
db_conn = sqlite3.connect('cases.db')
total_students = 85000
import techniques
from PIL import Image
import glob
from bokeh.io import export_png
import os

def plotday(d: pd.Timestamp):
    df = pd.read_sql_query('''
        SELECT
            Datestamp,
            cases.School,
            Active_Student,
            Total_Student,
            school_level.Students as "Student_Pop",
            Latitude,
            Longitude
        FROM
            cases
        LEFT JOIN school_level USING(School)
        WHERE Datestamp='{}';
        '''.format(d), db_conn, parse_dates='Datestamp'
    )
    df["size"] = df.Active_Student / df.Student_Pop * 500
    fig = df.dropna().plot_bokeh.map(
        title="Active Cases {}".format(d),
        x="Longitude",
        y="Latitude",
        hovertool_string='''<h2> @{School} </h2>
                            <h3> Active Student Cases: @{Active_Student} </h3>
                            <h3> Total Student Cases: @{Total_Student} </h3>''',
        size="size",
        figsize=(1024,800)
    )
    return fig

def main():
    df = techniques.summarize(db_conn, total_students)
    for d in df.index[1:]:
        d_str = d.strftime('%Y-%m-%d')
        fig = plotday(d_str)
        export_png(fig, filename='active_cases_{}.png'.format(d_str))
    
    fp_in = "active_cases_*.png"
    fp_out = "active_cases.gif"
    
    # https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#gif
    img, *imgs = [Image.open(f) for f in sorted(glob.glob(fp_in))]
    img.save(fp=fp_out, format='GIF', append_images=imgs,
             save_all=True, duration=500, loop=0)
    for f in glob.glob(fp_in):
        os.unlink(f)
    print("finished")

if __name__ == '__main__':
    main()
