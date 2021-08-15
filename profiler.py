import numpy as np
from pandas import DataFrame
from pandas_profiling import ProfileReport
import snowflake.connector
import csv
from prettytable import PrettyTable


try:
    ctx = snowflake.connector.connect(
        user='RJAYRAMAN',
        password='Ushsho0519*ra',
        account='tm74354.east-us-2.azure',
        warehouse='RR_DEV_ETL_VW'
        )
except:
    print("Error connect to Database")
    raise

DB='RR_DEV'
SCHEMA='RR_LND'
sqlQuery = "select * from "+DB+"."+SCHEMA+".SI_STORES limit 1"

def fields(cursor):

    results = {}
    column = 0
    for d in cursor.description:
        results[d[0]] = column
        column = column + 1

    return results

Tbl=ctx.cursor()
Tbl.execute(sqlQuery)

field_map = fields(Tbl)
countd =list()
maxV=list()
minV = list()
countN=list()

for i in field_map:
    sqlq = "Select count(Distinct "  + str(i) + ") Countd, max(cast("  + str(i)   + " as varchar)) MaxV, Min(cast("  + str(i)   + " as varchar)) MinV, sum(Case when " + str(i)   + " is Null then 1 else 0  end) NullCount from "  +DB+"."+SCHEMA+".SI_STORES"
    #print(sqlq)
    Tbl.execute(sqlq)
    row = Tbl.fetchone()
    countd.append(row[0])
    maxV.append(row[1])
    minV.append(row[2])
    countN.append(row[3])
    #print(i, row[0],row[1],row[2],row[3])

t=PrettyTable(['Column Name', 'Disctinct Count','Max Value', 'Min Value','Null Count'])
ctr=0

for i in field_map:
    trow=list()
    trow.append(i)
    trow.append(countd[ctr])
    trow.append(maxV[ctr])
    trow.append(minV[ctr])
    trow.append(countN[ctr])
    t.add_row(trow)
    ctr+=1

print(t)

