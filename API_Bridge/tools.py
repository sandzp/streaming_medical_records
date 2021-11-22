import pandas as pd
from random import randint
import sys

df1 = pd.read_csv("Data_Admissions.csv")
df2 = pd.read_csv("Data_Patients.csv", sep='\t')
df3 = pd.read_csv("Data_Labs.csv", sep='\t')

# ==============================================================
#
# ==============================================================
def get_sleep():
    return 0.01
# ==============================================================
#
# ==============================================================
def get_user():
    random_number = randint(1,10)
    if random_number == 1:
        return ("hacker","password")
    else:
        return ("user1","password1")
# ==============================================================
#
# ==============================================================
def get_base_url():
	return 'http://0.0.0.0:5000/api_twitter'
# ==============================================================
#
# ==============================================================
def get_http_headers():
    http_headers = {'Content-Type': 'application/json'}
    return http_headers
# ==============================================================
#
# ==============================================================
def get_json_1():
    json_load = (df1.iloc[randint(1, len(df1) -1)]).to_json()
    return json_load , 'http://flask:5000/api_admissions' 
# ==============================================================
#
# ==============================================================
def get_json_2():
    json_load = (df2.iloc[randint(1, len(df2) -1 )]).to_json()
    return json_load , 'http://flask:5000/api_patient'
# ==============================================================
#
# ==============================================================
def get_json_3():
    json_load = (df3.iloc[randint(1, len(df3)-1)]).to_json()
    return json_load , 'http://flask:5000/api_lab_results'