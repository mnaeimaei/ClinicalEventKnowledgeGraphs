import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func6_Link_PaitentPatient as cl3g

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
DK2dataSet = 'MIMIC'
DK2inputPath = './Test_Input/'

DK2_Extension = '.csv'

DK2_Perf_FileName = DK2dataSet + '_Performance' + DK2_Extension


Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"


###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')


    relationTypesDK = [":Patient_Patient", ":LINKED_TO", ":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]

    relationTypes = relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3g.deleteRelation, relationTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK2dataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end





print("-------------------------------------------------------------------------------------------------------------------------")


step_link_Patient_Patient=True
if step_link_Patient_Patient:
    print('                      ')
    print('Step7 - Creating Relationship between Patient to Patient......')


    with driver.session() as session:
        session.execute_write(cl3g.Patient_Patient)




    end = time.time()
    row={'name':DK2dataSet+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print(':HAS relationships done: took '+str(end - last)+' seconds')
    last = end


print("-------------------------------------------------------------------------------------------------------------------------")

step_useful_Query=True
if step_useful_Query:
    print('                      ')
    print('Step8 - Useful Query......')
    cl3g.usefulQuery()




print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': DK2dataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK2_Perf_FileName)
perf.to_csv(fullname)
driver.close()



