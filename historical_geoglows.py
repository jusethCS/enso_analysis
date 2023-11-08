# Import libraries and dependencies
import os
import math
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np

import warnings
warnings.filterwarnings('ignore')


# Change the work directory
user = os.getlogin()
user_dir = os.path.expanduser('~{}'.format(user))
os.chdir(user_dir)
os.chdir("tethys_apps_ecuador/geoglows_database_ecuador")

# Import enviromental variables
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

# Generate the conection token
token = "postgresql+psycopg2://{0}:{1}@localhost:5432/{2}".format(DB_USER, DB_PASS, DB_NAME)




###############################################################################################################
#                                 Function to get and format the data from DB                                 #
###############################################################################################################
def get_format_data(sql_statement, conn):
    # Retrieve data from database
    data =  pd.read_sql(sql_statement, conn)
    # Datetime column as dataframe index
    data.index = data.datetime
    data = data.drop(columns=['datetime'])
    # Format the index values
    data.index = pd.to_datetime(data.index)
    data.index = data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    data.index = pd.to_datetime(data.index)
    # Return result
    return(data)



###############################################################################################################
#                                   Getting return periods from data series                                   #
###############################################################################################################
def gumbel_1(std: float, xbar: float, rp: int or float) -> float:
  return -math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std)

def get_return_periods(comid, data):
    # Stats
    max_annual_flow = data.groupby(data.index.strftime("%Y")).max()
    mean_value = np.mean(max_annual_flow.iloc[:,0].values)
    std_value = np.std(max_annual_flow.iloc[:,0].values)
    # Return periods
    return_periods = [100, 50, 25, 10, 5, 2]
    return_periods_values = []
    # Compute the corrected return periods
    for rp in return_periods:
      return_periods_values.append(gumbel_1(std_value, mean_value, rp))
    # Parse to list
    d = {'rivid': [comid], 
         'return_period_100': [return_periods_values[0]], 
         'return_period_50': [return_periods_values[1]], 
         'return_period_25': [return_periods_values[2]], 
         'return_period_10': [return_periods_values[3]], 
         'return_period_5': [return_periods_values[4]], 
         'return_period_2': [return_periods_values[5]]}
    # Parse to dataframe
    corrected_rperiods_df = pd.DataFrame(data=d)
    corrected_rperiods_df.set_index('rivid', inplace=True)
    return(corrected_rperiods_df)


# Función para contar los máximos locales que superan un umbral
def get_events(series, umbral):
    count = 0
    for i in range(1, len(series) - 1):
        actual = series.iloc[i][0]
        anterior = series.iloc[i-1][0]
        siguiente = series.iloc[i+1][0]
        if actual > anterior and actual > siguiente and actual > umbral:
            count += 1
    return count


def get_enso_index(qquery, conn, rp, mean_streamflow):
  enso = get_format_data(qquery, conn)
  events = get_events(enso, rp)
  enso = enso.sort_values(by='streamflow_m^3/s', ascending=False)
  frec_enso = ((enso > rp)['streamflow_m^3/s'] == True).sum()
  max_enso = enso['streamflow_m^3/s'][0]
  max_enso_anomality = max_enso/mean_streamflow
  date_max_enso = enso.index[0]
  #return([events, frec_enso, date_max_enso, max_enso, max_enso_anomality])
  out = events*frec_enso*max_enso_anomality + 1
  #print(out)
  return(math.log(out))


###############################################################################################################
#                                              Main routine                                                   #
###############################################################################################################

# Setting the connetion to db
db = create_engine(token)

# Establish connection
conn = db.connect()

# Getting stations
drainage = pd.read_sql("select * from drainage_network;", conn)

# Number of stations
n = len(drainage)
out = pd.DataFrame()

for i in range(0,n):
    # State variable
    comid = drainage.comid[i]
    # Progress
    prog = round(100 * i/n, 3)
    print(prog)
    # Query to database
    qquery = "select * from r_{0} where datetime < '2022-06-01' and datetime > '1979-01-01';".format(comid)
    simulated_data = get_format_data(qquery, conn)
    # Return period
    return_periods = get_return_periods(comid, simulated_data)
    rp = float(return_periods["return_period_10"])
    # Statics streamflow
    max_streamflow = simulated_data.max()[0]
    mean_streamflow = simulated_data.mean()[0]
    # years
    yy = range(1997, 2023)
    index_out = list()
    index_out.append(comid)
    for y in yy:
      qquery = "select * from r_{0} where datetime >= '{1}-01-01' and datetime < '{2}-01-01';".format(comid, y, y+1)
      enso_yy = get_enso_index(qquery, conn, rp, mean_streamflow)
      index_out.append(enso_yy)
    out = out.append(pd.Series(index_out), ignore_index=True)


# Change the work directory
user = os.getlogin()
user_dir = os.path.expanduser('~{}'.format(user))
os.chdir(user_dir)
out.to_excel("out_years.xlsx", index=False)


