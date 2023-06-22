import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func11_Link_eventDiagnosis as cl3g

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))



DK7dataSet = 'MIMIC_ML'
DK7inputPath = './Test_Input/'

DK7_Activity_DK7_FileName = 'L5_eventDiagnosis'

DK7_Extension = '.csv'

DK7_Input_Activity_DK7_FileName = DK7_Activity_DK7_FileName + DK7_Extension

DK7_Neo4JImport_Activity_DK7_FileName = DK7_Activity_DK7_FileName + '_Neo4j' + DK7_Extension

DK7_Perf_FileName = DK7dataSet + '_Performance' + DK7_Extension

DK7_DK_ID = "dkID"
DK7_Activity = "Activity"
DK7_Activity_Synonym = "Activity_Synonym"
DK7_Activity_Origin = "Activity_Origin"
DK7_Activity_Value_ID = "Activity_Value_ID"
DK7_Icd9_Code_Short_list = "Icd9_Code_Short_list"






print("************************** input from cl1: Activity_OCPS ****************************************************************************")
Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"
Event_Diagnose_MappingRelation_split= [['dk1', 'aaa', 'a', 'O1', 1009, '1'], ['dk1', 'aaa', 'a', 'O1', 1009, '3'], ['dk1', 'aaa', 'a', 'O1', 1009, '5'], ['dk2', 'aaa', 'a', 'O1', 1014, '1'], ['dk2', 'aaa', 'a', 'O1', 1014, '3'], ['dk2', 'aaa', 'a', 'O1', 1014, '5'], ['dk3', 'aaa', 'a', 'O1', 1001, '1'], ['dk3', 'aaa', 'a', 'O1', 1001, '3'], ['dk3', 'aaa', 'a', 'O1', 1001, '5'], ['dk4', 'aaa', 'a', 'O1', 1006, '10'], ['dk4', 'aaa', 'a', 'O1', 1006, '12'], ['dk4', 'aaa', 'a', 'O1', 1006, '13'], ['dk4', 'aaa', 'a', 'O1', 1006, '16'], ['dk5', 'aaa', 'a', 'O1', 1010, '23'], ['dk5', 'aaa', 'a', 'O1', 1010, '25'], ['dk5', 'aaa', 'a', 'O1', 1010, '27'], ['dk6', 'aaa', 'a', 'O1', 1012, '23'], ['dk6', 'aaa', 'a', 'O1', 1012, '25'], ['dk6', 'aaa', 'a', 'O1', 1012, '27'], ['dk7', 'aaa', 'a', 'O1', 1018, '47'], ['dk8', 'bbb', 'b', 'O2', 1019, '48'], ['dk8', 'bbb', 'b', 'O2', 1019, '49'], ['dk9', 'bbb', 'b', 'O2', 1005, '35'], ['dk9', 'bbb', 'b', 'O2', 1005, '36'], ['dk9', 'bbb', 'b', 'O2', 1005, '29'], ['dk10', 'bbb', 'b', 'O2', 1013, '35'], ['dk10', 'bbb', 'b', 'O2', 1013, '36'], ['dk10', 'bbb', 'b', 'O2', 1013, '29'], ['dk11', 'bbb', 'b', 'O2', 1002, '17'], ['dk11', 'bbb', 'b', 'O2', 1002, '18'], ['dk11', 'bbb', 'b', 'O2', 1002, '22'], ['dk12', 'bbb', 'b', 'O2', 1011, '17'], ['dk12', 'bbb', 'b', 'O2', 1011, '18'], ['dk12', 'bbb', 'b', 'O2', 1011, '22'], ['dk13', 'bbb', 'b', 'O2', 1015, '43'], ['dk13', 'bbb', 'b', 'O2', 1015, '44'], ['dk14', 'bbb', 'b', 'O2', 1016, '43'], ['dk14', 'bbb', 'b', 'O2', 1016, '44'], ['dk15', 'ccc', 'c', 'O3', 1008, '8'], ['dk15', 'ccc', 'c', 'O3', 1008, '9'], ['dk15', 'ccc', 'c', 'O3', 1008, '10'], ['dk16', 'ccc', 'c', 'O3', 1020, '30'], ['dk16', 'ccc', 'c', 'O3', 1020, '31'], ['dk16', 'ccc', 'c', 'O3', 1020, '32'], ['dk17', 'ccc', 'c', 'O3', 1003, '34'], ['dk17', 'ccc', 'c', 'O3', 1003, '40'], ['dk18', 'ccc', 'c', 'O3', 1004, '34'], ['dk18', 'ccc', 'c', 'O3', 1004, '40'], ['dk19', 'ccc', 'c', 'O3', 1007, '45'], ['dk19', 'ccc', 'c', 'O3', 1007, '47'], ['dk20', 'ccc', 'c', 'O3', 1017, '45'], ['dk20', 'ccc', 'c', 'O3', 1017, '47']]


###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')


    relationTypesDK = [":BOND"]

    relationTypes = relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3g.deleteRelation, relationTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK7dataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end





print("-------------------------------------------------------------------------------------------------------------------------")


step_link_Activity_OCPS=True
if step_link_Activity_OCPS:
    print('                      ')
    print('Step7 - Creating Relationship between Activity to OCPS......')

    for item in Event_Diagnose_MappingRelation_split:
        with driver.session() as session:
            session.execute_write(cl3g.Event_Potential, item[0], item[1], item[2], item[3], item[4], item[5])




    end = time.time()
    row={'name':DK7dataSet+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
row = {'name': DK7dataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK7_Perf_FileName)
perf.to_csv(fullname)
driver.close()



