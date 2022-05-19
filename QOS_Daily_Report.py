
import pandas as pd
from datetime import timedelta
import datetime
import shutil
import time
from datetime import datetime

import glob
import teradata
import os
import time
from zipfile import ZipFile

# CurrentDate = datetime.now().strftime('%d %B %Y')
SetDate = datetime.now().date()
SetDate = SetDate -timedelta(days=7)
CurrentDate = SetDate.strftime('%d %B %Y')
start_time = time.time()


print(CurrentDate)
net_path = "\\\\FS002\MKT-Analytics\QOS_Daily_Dashboard_MTD_Data\\"
dest_path = "D:\\TD Queries\\QOS Daily Dashboard\\Daily Extract\\"

host,username,password = 'pam.zong.com.pk','usman_altaf', 'Usm@n2112'
#Make a connection
udaExec = teradata.UdaExec (appName="test", version="1.0", logConsole=False)


with udaExec.connect(method="odbc",system=host, username=username,
                            password=password, driver="Teradata Database ODBC Driver 16.20") as connect:

    query = """SELECT
    BB.SBSCRPN_EFF_DT AS SBSCRPN_EFF_DT,
    CC.BVS_DEVICE_ID,
    --CC.LATITUDE,
    --CC.LONGITUDE,
    DD.RET_ID AS Retailer_ID,
    DD.CHNL_NUM AS Franchise_ID,
    EE.GRID_ID,
    BB.PORT_IN_IND,
    DD.CHNL_RGN_NAME AS Sales_Region
        ,CASE WHEN CHNL_RGN_NAME	IN ('Central I','Central IV','Central V') THEN 'CENTRAL-A'
                    WHEN CHNL_RGN_NAME	IN ('Central II','Central III') THEN 'CENTRAL-B'
                    WHEN CHNL_RGN_NAME	IN ('North I','North II','North III') THEN 'NORTH'
                    WHEN CHNL_RGN_NAME	IN ('South I','SOUTH II','SOUTH III','SOUTH IV') THEN 'SOUTH'
                    ELSE 'NULL' END SALES_REGION_GROUP
    ,Count(DISTINCT CASE WHEN FLAG= 1 THEN AA.sbscrpn_key ELSE NULL END ) LOW_Q_SUBS
        ,Count(DISTINCT CASE WHEN FLAG= 0 THEN AA.sbscrpn_key ELSE NULL END ) HIGH_Q_SUBS    
        ,Count(DISTINCT AA.sbscrpn_key) TOTAL_SALES

    FROM DT_SPSS.QOS_1STMONTH_PRED AA -- QOS predictions against SBSCRPN_KEY
    INNER JOIN DT_SDMVW.DIM_SBSCRPN BB -- SBSCRPN_EFF_DT
    ON AA.SBSCRPN_KEY = BB.SBSCRPN_KEY

    LEFT JOIN DT_SDMVW.FCT_SALES CC
    ON AA.SBSCRPN_KEY = CC.SBSCRPN_KEY

    LEFT JOIN DT_SDMVW.DIM_CHNL_SND DD -- Franchise_ID, Sales_Region, Retailer_ID
    ON CC.SL_CHNL_KEY = DD.CHNL_KEY

    LEFT JOIN (SEL * FROM DT_SDMVW.DIM_GRID QUALIFY Row_Number() Over(PARTITION BY Franchise_Id ORDER BY EnD_Date DESC)=1) EE -- GRID_ID
    ON EE.Franchise_id = DD.CHNL_NUM
    AND Start_date <= BB.SBSCRPN_EFF_DT
    AND End_date > BB.SBSCRPN_EFF_DT
    WHERE BB.SBSCRPN_BIL_TYPE = 2 AND BB.SBSCRPN_EFF_DT BETWEEN   '2021-12-01' AND DATE-7
    GROUP BY 1,2,3,4,5,6,7;"""

    #Reading query to df
    df = pd.read_sql(query,connect)
    # do something with df,e.g.
    print(df.head()) #to see the first 5 rows

print("--- %s seconds ---" % (time.time() - start_time))



df.to_csv(dest_path + "MTD "+CurrentDate+" Data.txt",index=False,sep='|')
time.sleep(10)
os.chdir('D:/TD Queries/QOS Daily Dashboard/Daily Extract')
print(os.getcwd())
zipObj = ZipFile("MTD "+CurrentDate+" Data.zip", 'w')
zipObj.write("MTD "+CurrentDate+" Data.txt")
zipObj.close()

shutil.copyfile(dest_path+"MTD "+CurrentDate+" Data.zip",net_path + "MTD "+CurrentDate+" Data.zip")

os.remove(dest_path + "MTD "+CurrentDate+" Data.txt")


