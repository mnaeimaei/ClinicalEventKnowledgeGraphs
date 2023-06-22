import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func9_Link_ClassSNOMEDCT as cl3f

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
DK5dataSet = 'SNOMED_CT_MIMIC'
DK5inputPath = './Test_Input/'

DK5_Activity_DK5_FileName = 'L3_ClassSNOMEDCT'

DK5_Extension = '.csv'

DK5_Input_Activity_DK5_FileName = DK5_Activity_DK5_FileName + DK5_Extension

DK5_Neo4JImport_Activity_DK5_FileName = DK5_Activity_DK5_FileName + '_Neo4j' + DK5_Extension

DK5_Perf_FileName = DK5dataSet + '_Performance' + DK5_Extension

DK5_Activity = "Activity"
DK5_Activity_Synonym = "Activity_Synonym"
DK5_Activity_Origin = "Activity_Origin"
DK5_OTC = "OTC"
DK5_SCTCode = "SCTCode"








print("************************** input from cl1: Activity_OCPS ****************************************************************************")
Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"


Activity_OCT_MappingRelation= [['aaa', 'a', 'O1', 41, 1], ['bbb', 'b', 'O2', 42, 1], ['ccc', 'c', 'O3', 43, 1]]

###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

    relationTypesDK = [":MAPPED_TO", ":Form_OCT", ":BOND"]

    relationTypes = relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3f.deleteRelation, relationTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK5dataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end





print("-------------------------------------------------------------------------------------------------------------------------")


step_link_Activity_OCPS=True
if step_link_Activity_OCPS:
    print('                      ')
    print('Step7 - Creating Relationship between Activity to OCPS......')

    for item in Activity_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.Activity_OCPS, item[0], item[1], item[2], item[3], item[4])




    end = time.time()
    row={'name':DK5dataSet+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print(':HAS relationships done: took '+str(end - last)+' seconds')
    last = end


print("-------------------------------------------------------------------------------------------------------------------------")

step_useful_Query=True
if step_useful_Query:
    print('                      ')
    print('Step8 - Useful Query......')
    cl3f.usefulQuery()




print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': DK5dataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK5_Perf_FileName)
perf.to_csv(fullname)
driver.close()



