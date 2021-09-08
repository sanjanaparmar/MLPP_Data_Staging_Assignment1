# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import pandas as pd
import psycopg2
import csv


def ACS5_api(variables, year):
    host="https://api.census.gov/data"
    dataset="acs/acs5?"
    base_url="/".join([host, year, dataset])
    #print(base_url)

    predicates = {}
    #variables=["NAME","B01001_002E", "B01003_001E", "B02001_002E", "B29001_002E"]
    predicates["get"]=",".join(variables)
    predicates["for"]="block group:*"
    predicates["in"]="state:15%20county:*"
    r=requests.get(base_url, params=predicates)
    #print(r.text)
    return r
 
    
def dataframe(r, col_names):
    #col_names=["Name", "Sex_by_Age_male", "Total_Population", "Race","Population_voting", "State", "County", "Tract", "Block_Group"]
    #print(r.json()[0])
    #print(col_names)
    acs_df=pd.DataFrame(columns=col_names, data=r.json()[1:])
    #print(acs_df.head())
    return acs_df

def postgres_sql(filename):

    conn=psycopg2.connect("host=acs-db.mlpolicylab.dssg.io dbname=acs_data_loading user=mlpp_student password=CARE-horse-most port=5432")                        
                        
    cur = conn.cursor()

    cur.execute("""
                CREATE TABLE ACS.SPARMAR2_acs_data(
                    Row_number integer,
                    Name text PRIMARY KEY,
                    Sex_by_Age_Male integer,
                    Total_Population integer,
                    Race integer,
                    Population_Voting integer,
                    State integer,
                    County integer,
                    Tract integer,
                    Block_Group integer
                    )
                """)
    conn.commit()

    with open(r'Hawaii.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            cur.execute(
                "INSERT INTO ACS.SPARMAR2_acs_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                row
                )
    conn.commit()

def main():
    year_main='2019'
    variables_main=["NAME","B01001_001E", "B01003_001E", "B02001_002E", "B29001_002E"]
    col_names_main=["Name", "Sex_by_Age_male", "Total_Population", "Race","Population_voting", "State", "County", "Tract", "Block_Group"]
    r_main=ACS5_api(variables_main,year_main)
    df_main=dataframe(r_main,col_names_main)
    filename_main=r'Hawaii.csv'
    dataframe(r_main,col_names_main).to_csv(filename_main,mode='w')
    # acs_df.to_csv(r'Hawaii.csv',mode='w')
    postgres_sql(filename_main)
    
if__name__=='__main__':
    main()