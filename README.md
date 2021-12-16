# AACPS covid data

Data is collected from aacps.org/covid19dashboard

summary.csv and summary_T.csv offer the summary data in csv format.

The summary data is displayed in Summary.ipynb, use https://nbviewer.jupyter.org/github/robwiss/aacps_covid_data/blob/main/Summary.ipynb to see the graph rendered.

A gif showing active cases at schools across the county plotted on a map, across time:

[Active Cases Plotted on Map over Time](active_cases.gif)

To setup the python environment:
```
virtualenv .venv
. .venv/bin/activate
pip3 install -r requirements.txt
```

To build the database `cases.db`:
```
python3 parse.py
```

To create the gif:
```
# on ubuntu need to install `firefox` and `firefox-geckodriver`
python3 make_gif.py
```