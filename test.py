
import pandas as pd
from datetime import timedelta
import datetime
import shutil
import time
from datetime import datetime
import numpy as np
import glob
import teradata
import os
import time
from zipfile import ZipFile

SetDate = datetime.now().date()
SetDate = SetDate -timedelta(days=7)
CurrentDate = SetDate.strftime('%d %B %Y')
start_time = time.time()

print(CurrentDate)

net_path = "\\\\FS002\MKT-Analytics\QOS_Daily_Dashboard_MTD_Data\\"
dest_path = "D:\\TD Queries\\QOS Daily Dashboard\\Daily Extract\\"

host,username,password = 'pam.zong.com.pk','usman_altaf', 'Zong@321'
#Make a connection
udaExec = teradata.UdaExec (appName="test", version="1.0", logConsole=False)


with udaExec.connect(method="odbc",system=host, username=username,
                            password=password, driver="Teradata Database ODBC Driver 16.20") as connect:

    query = """SEL TOP 10 * FROM dt_sdmvw.DIM_SBSCRPN;"""

    #Reading query to df
    df = pd.read_sql(query,connect)
    # do something with df,e.g.
    print(df.head()) #to see the first 5 rows

print("--- %s seconds ---" % (time.time() - start_time))



df.to_csv(dest_path + "test"+CurrentDate+".txt",index=False,sep='|')

os.chdir('D:/TD Queries/QOS Daily Dashboard/Daily Extract')
print(os.getcwd())
zipObj = ZipFile('sample '+CurrentDate+'.zip', 'w')
zipObj.write("test"+CurrentDate+".txt")
zipObj.close()

shutil.copyfile(dest_path+'sample '+CurrentDate+'.zip',net_path + 'sample '+CurrentDate+'.zip')
os.remove(dest_path + "test"+CurrentDate+".txt")