import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func4_ICD as cl3b

from tqdm import tqdm



driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
CEdataSet = 'MIMIC'
CEinputPath = './Test_Input/'

CE_PoNode_FileName = '4ICD'

CE_Extension = '.csv'

CE_Input_PoNode_FileName = CE_PoNode_FileName + CE_Extension

CE_Neo4JImport_PoNode_FileName = CE_PoNode_FileName + '_Neo4j' + CE_Extension

CE_Perf_FileName = CEdataSet + '_Performance' + CE_Extension

CE_PoNode_id = "potentialID"
CE_PoNode_code = "icd_code"
CE_PoNode_Version = "icd_version"
CE_PoNode_code_details = "icd_code_title"
CE_PoNode_syn = "icd_code_syn"





Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"
csv_CE = cl3b.ImportCSV(CEinputPath, CE_Input_PoNode_FileName)
cl3b.Create_CSV_in_Neo4J_import(csv_CE, Neo4JImport, CE_Neo4JImport_PoNode_FileName)
header_CE, csvLog_header_CE = cl3b.LoadLog(Neo4JImport+CE_Neo4JImport_PoNode_FileName)







###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')


    ############PART G ##########################################
    nodeTypesG = [":ICD"]

    ############PART H ##########################################
    nodeTypesH = [":Concept"]
    relationTypesH = [":ANCESTOR_OF"]

    ############PART DK ##########################################
    relationTypesDK = [":Activity_Class", ":Patient_Patient", ":LINKED_TO", ":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]

    nodeTypes = nodeTypesG + nodeTypesH
    relationTypes = relationTypesH + relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3b.deleteRelation, relationTypes)
        session.execute_write(cl3b.DeleteNodes, nodeTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':CEdataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end

print("-------------------------------------------------------------------------------------------------------------------------")


step_Clear_OCT_Constraints=True
if step_Clear_OCT_Constraints:
    print('                      ')
    print('Step2 - Dropping Constraint...')

    with driver.session() as session:
        session.execute_write(cl3b.clearConstraint, None, driver)



    end = time.time()
    row={'name':CEdataSet+'_clearConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint clearing done: took '+str(end - last)+' seconds')
    last = end



print("-------------------------------------------------------------------------------------------------------------------------")


step_createConstraint=True
if step_createConstraint:
    print('                      ')
    print('Step3 - Creating Constraint...')

    with driver.session() as session:
        session.execute_write(cl3b.createConstraint)


    end = time.time()
    row={'name':CEdataSet+'_createConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint creating done: took '+str(end - last)+' seconds')
    last = end




print("-------------------------------------------------------------------------------------------------------------------------")


step_loadPotential_Nodes=True
if step_loadPotential_Nodes :
    print('                      ')
    print('Step4 - Creating OCT Concepts Nodes...')
    # convert each record in the CSV table into an Event node
    query, testingQ = cl3b.loadPotential_Nodes(header_CE, CE_Neo4JImport_PoNode_FileName, CEdataSet)  # generate query to create all events with all log columns as properties
    print(query)
    print(testingQ)
    cl3b.runQuery(driver, query)

    end = time.time()
    row={'name':CEdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('SCT nodes done: took '+str(end - last)+' seconds')
    last = end



print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': CEdataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, CE_Perf_FileName)
perf.to_csv(fullname)
driver.close()



