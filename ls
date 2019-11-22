# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 13:11:55 2019
@author: s367859
"""
#Library
import sys
import os
from datetime import datetime
import pandas as pd
import pyodbc
import logging

os.chdir('D:/LeftShift_Python_Job')
#Create and configure logger 
logging.basicConfig(filename=str(datetime.now().strftime('LeftShift_Index_%Y%m%d.log')), format='%(asctime)s %(message)s') 
#Creating an object
logger=logging.getLogger()   
#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG)
logger.info("LeftShift_Index Batch job started")

#APPLENS DATABASE CONNECTION     
conn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                     #"Server=10.142.143.47,1433;"
                     "Server=CTSC01165026301;"
                     "Database=LeftShift;"
                     "uid=LeftShift;pwd=ls@123")
logger.info("Established the DB connection")

#RUN DATE LOGIC
'''
today = datetime.today()
ls_month = today.month
ls_year = today.year

if (ls_month == 1):
    ls_year = ls_year-1
    ls_month = 12
else:
    ls_month = ls_month -1'''
ls_month = 1
ls_year = 2019


#INDEX TABLE COMPUTATION
All_query = 'EXEC [dbo].[GetIndex_AllProjects]'
all_projdf =  pd.read_sql_query(All_query,conn)
all_projlist = all_projdf['EsaProjectID'].unique().tolist()

type(onboarded_prjlist)
type(all_projlist)
c=list(set(all_projlist)-set(onboarded_prjlist))
c
# On boarded APPLENS projects
onboarded_query = 'EXEC [dbo].[GetIndex_OnboardedProjects] @ls_month = '+str(ls_month)+',@ls_year = '+str(ls_year)
onboarded_prjdf =  pd.read_sql_query(onboarded_query,conn)
onboarded_prjlist = onboarded_prjdf['EsaProjectID'].unique().tolist()

# not onboarded to APPLENS
Not_Applens_prj = list(set(all_projlist)-set(onboarded_prjlist))

params = tuple(onboarded_prjlist)  
    
MPS_query = " ".join(["select distinct ADP.EsaProjectID",
                      "from [AppVisionLens].[dbo].[Adp_Input_Excel] ADP ",
                      "INNER JOIN [AppVisionLens].[AVL].MAS_ProjectMaster PM ON ADP.ESAProjectID = PM.EsaProjectID ",
                      "inner join [AppVisionLens].[AVL].[TK_PRJ_ProjectServiceActivityMapping] SP on SP.ProjectID = PM.ProjectID ",
                      "inner join [AppVisionLens].[AVL].[TK_MAS_ServiceActivityMapping] SM on SM.ServiceMappingID = SP.ServiceMapID",
                      "where SM.ServiceTypeID=4  and SM.IsDeleted='0' and SP.IsDeleted='0' and isNULL(SP.IsHidden,0)=0 ",
                      "and ADP.EsaProjectID IN {}".format(tuple(params))])
MPS_df = pd.read_sql_query(MPS_query, conn)
MPS_list = MPS_df['EsaProjectID'].unique().tolist()

# Not MPS project list
NotMPS_list = list(set(onboarded_prjlist)-set(MPS_list))

Activeprj_Param = tuple(MPS_list)

Eligible_Query = " ".join(["Select TK.[ProjectID],TK.EsaProjectID,Count(TK.TicketID) as Ticket_Count,ST.ServiceTypeName",
                           "from [AppVisionLens].[dbo].[vw_TK_TRN_TicketDetail] TK ",
                           "INNER JOIN [AppVisionLens].[AVL].[TK_MAS_Service] SR ON TK.ServiceID = SR.ServiceID ",
                           "INNER JOIN [AppVisionLens].[AVL].[TK_MAS_ServiceType] ST ON SR.ServiceType = ST.ServiceTypeID ",
                           "where TK.ticketstatus = 'closed' and month(TK.[Closeddate]) = "+str(ls_month)+" and year(TK.[Closeddate]) = "+str(ls_year),
                           "and ST.ServiceTypeName =  'MPS' and TK.EsaProjectID IN  {}".format(tuple(Activeprj_Param)),
                           "group by TK.[ProjectID],TK.EsaProjectID,month(TK.[Closeddate]),year(TK.[Closeddate]),SR.ServiceType,ST.ServiceTypeName",
                           "having Count(TK.TicketID) >= 50"])
MPS_Projects_df = pd.read_sql_query(Eligible_Query, conn)
MPS_Project_List = MPS_Projects_df['EsaProjectID'].unique().tolist()
MPS_l50Project_List = list( set(MPS_list) - set(MPS_Project_List))

#Dataframe Operations
Left_shit_Index = pd.DataFrame()
Left_shit_Index['EsaProjectID'] = MPS_Project_List
Left_shit_Index['Month'] = str(ls_month)+'-'+str(ls_year)
Left_shit_Index['Eligibility'] = 'Eligible'
Left_shit_Index['Remarks'] = ''
Left_shit_Index['Solution_Applied'] = 'cause code - resolution code cluster'

Left_shit_Index1 = pd.DataFrame()
Left_shit_Index1['EsaProjectID'] = MPS_l50Project_List
Left_shit_Index1['Month'] = str(ls_month)+'-'+str(ls_year)
Left_shit_Index1['Eligibility'] = 'Not Eligible'
Left_shit_Index1['Remarks'] = '< 50 Ticket Volume'
Left_shit_Index1['Solution_Applied'] = 'cause code - resolution code cluster'

Left_shit_Index2 = pd.DataFrame()
Left_shit_Index2['EsaProjectID'] = NotMPS_list
Left_shit_Index2['Month'] = str(ls_month)+'-'+str(ls_year)
Left_shit_Index2['Eligibility'] = 'Not Eligible'
Left_shit_Index2['Remarks'] = 'Not a MPS Project'
Left_shit_Index2['Solution_Applied'] = 'cause code - resolution code cluster'


Left_shit_Index3 = pd.DataFrame()
Left_shit_Index3['EsaProjectID'] = Not_Applens_prj
Left_shit_Index3['Month'] = str(ls_month)+'-'+str(ls_year)
Left_shit_Index3['Eligibility'] = 'Not Eligible'
Left_shit_Index3['Remarks'] = 'Not Onboarded to APPLENS'
Left_shit_Index3['Solution_Applied'] = 'cause code - resolution code cluster'


Left_Shift_df_final = pd.concat([Left_shit_Index, Left_shit_Index1, Left_shit_Index2,Left_shit_Index3], axis=0)
Left_Shift_df_final['Month'] = pd.to_datetime(Left_Shift_df_final['Month'], format="%m-%Y")
Left_Shift_df_final = Left_Shift_df_final.sort_values(['EsaProjectID'], ascending=[True])

logger.info("project eligibility check completed and initiating the cursor for loading")
cursor_ls = conn.cursor()
for index, row in Left_Shift_df_final.iterrows():
    cursor_ls.execute("INSERT INTO dbo.LeftShift_Index([ESA_ProjectID],[LS_Month],[Eligibility],[Remarks],[Solution_Applied]) values (?,?,?,?,?)", row['EsaProjectID'], row['Month'], row['Eligibility'], row['Remarks'], row['Solution_Applied'])
#conn.commit()
#cursor_ls.close()
#conn.close()
logger.info("Job Completed & Successfully loaded into the Index table")
        




