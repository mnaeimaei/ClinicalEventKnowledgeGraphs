import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase



import Func10_Link_formSNOMEDCT as cl3f

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


DK6dataSet = 'MIMIC_ML'
DK6inputPath = './Test_Input/'

DK6_Form_DK6_FileName = 'L4_formSNOMEDCT'

DK6_Extension = '.csv'

DK6_Input_Form_DK6_FileName = DK6_Form_DK6_FileName + DK6_Extension

DK6_Neo4JImport_Form_DK6_FileName = DK6_Form_DK6_FileName + '_Neo4j' + DK6_Extension

DK6_Perf_FileName = DK6dataSet + '_Performance' + DK6_Extension

DK6_Activity_Origin = "Activity_Origin"
DK6_OTC = "OTC"
DK6_SCTCode = "SCTCode"





print("************************** input from cl1: Activity_OCPS ****************************************************************************")
Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"
Form_OCT_MappingRelation= [['O1', 80, 1], ['O2', 81, 1], ['O3', 82, 1]]


###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

    relationTypesDK = [":Form_OCT", ":BOND"]

    relationTypes = relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3f.deleteRelation, relationTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK6inputPath+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end





print("-------------------------------------------------------------------------------------------------------------------------")


step_link_Activity_OCPS=True
if step_link_Activity_OCPS:
    print('                      ')
    print('Step7 - Creating Relationship between Activity to OCPS......')

    for item in Form_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3f.Activity_OCPS, item[0], item[1], item[2])


    end = time.time()
    row={'name':DK6inputPath+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
row = {'name': DK6inputPath + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK6_Perf_FileName)
perf.to_csv(fullname)
driver.close()



