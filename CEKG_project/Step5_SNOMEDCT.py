import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func5_SNOMEDCT as cl3c

from tqdm import tqdm

print("******************************************************************************************************")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
OCTdataSet = 'SNOMED_CT'
OCTinputPath = './Test_Input/'

OCT_OCT_Node_FileName = '5SNOMEDCT_NODE'
OCT_OCT_REL_FileName = '5SNOMEDCT_REL'

OCT_Extension = '.csv'

OCT_Input_OCT_Node_FileName = OCT_OCT_Node_FileName + OCT_Extension
OCT_Input_OCT_REL_FileName = OCT_OCT_REL_FileName + OCT_Extension

OCT_Neo4JImport_OCT_Node_FileName = OCT_OCT_Node_FileName + '_Neo4j' + OCT_Extension
OCT_Neo4JImport_OCT_REL_FileName = OCT_OCT_REL_FileName + '_Neo4j' + OCT_Extension

OCT_Perf_FileName = OCTdataSet + '_Performance' + OCT_Extension

OCT_OCT_Node_conceptId = "conceptId"
OCT_OCT_Node_ConceptCode = "conceptCode"
OCT_OCT_Node_termA = "termA"
OCT_OCT_Node_termB = "termB"
OCT_OCT_Node_semanticTags = "semanticTags"
OCT_OCT_Node_ConceptType = "ConceptType"
OCT_OCT_Node_Levels = "level"

OCT_OCT_REL_s0 = "s0"
OCT_OCT_REL_s0_code = "s0_code"
OCT_OCT_REL_s1 = "s1"
OCT_OCT_REL_s1_code = "s1_code"



print("******************************************************************************************************")

Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"

csv_OCT_Node = cl3c.ImportCSV(OCTinputPath, OCT_Input_OCT_Node_FileName)
cl3c.Create_CSV_in_Neo4J_import(csv_OCT_Node, Neo4JImport, OCT_Neo4JImport_OCT_Node_FileName)

header_OCT_Node, csvLog_header_OCT_Node = cl3c.LoadLog(Neo4JImport+OCT_Neo4JImport_OCT_Node_FileName)




OCPS_REL_MappingRelation= [[0, 1, 34, 1], [1, 1, 35, 1], [2, 1, 36, 1], [3, 1, 37, 1], [4, 1, 38, 1], [5, 1, 39, 1], [6, 1, 40, 1], [7, 1, 41, 1], [8, 1, 42, 1], [9, 1, 43, 1], [10, 1, 44, 1], [11, 1, 45, 1], [12, 1, 46, 1], [13, 1, 47, 1], [14, 1, 48, 1], [15, 1, 49, 1], [16, 1, 50, 1], [17, 1, 51, 1], [18, 1, 52, 1], [19, 1, 53, 1], [20, 1, 54, 1], [21, 1, 55, 1], [22, 1, 56, 1], [23, 1, 57, 1], [24, 1, 58, 1], [25, 1, 59, 1], [26, 1, 60, 1], [27, 1, 61, 1], [28, 1, 62, 1], [29, 1, 63, 1], [30, 1, 64, 1], [31, 1, 65, 1], [32, 1, 66, 1], [33, 1, 67, 1], [41, 1, 44, 1], [42, 1, 45, 1], [43, 1, 46, 1], [34, 1, 99, 1], [35, 1, 99, 1], [36, 1, 99, 1], [37, 1, 99, 1], [38, 1, 99, 1], [39, 1, 99, 1], [40, 1, 99, 1], [41, 1, 99, 1], [42, 1, 99, 1], [43, 1, 99, 1], [44, 1, 99, 1], [45, 1, 99, 1], [46, 1, 99, 1], [47, 1, 99, 1], [48, 1, 99, 1], [49, 1, 99, 1], [50, 1, 99, 1], [51, 1, 99, 1], [52, 1, 99, 1], [53, 1, 99, 1], [54, 1, 99, 1], [55, 1, 99, 1], [56, 1, 99, 1], [57, 1, 99, 1], [58, 1, 99, 1], [59, 1, 99, 1], [60, 1, 99, 1], [61, 1, 99, 1], [62, 1, 99, 1], [63, 1, 99, 1], [64, 1, 99, 1], [65, 1, 99, 1], [66, 1, 99, 1], [67, 1, 99, 1], [80, 1, 83, 1], [81, 1, 84, 1], [82, 1, 85, 1], [83, 1, 99, 1], [84, 1, 99, 1], [85, 1, 99, 1]]




###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

    ############PART H ##########################################
    nodeTypesH = [":Concept"]
    relationTypesH = [":ANCESTOR_OF"]

    ############PART DK ##########################################
    relationTypesDK = [":Activity_Class", ":Patient_Patient", ":LINKED_TO", ":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]

    nodeTypes = nodeTypesH
    relationTypes = relationTypesH + relationTypesDK



    with driver.session() as session:
        session.execute_write(cl3c.deleteRelation, relationTypes)
        session.execute_write(cl3c.DeleteNodes, nodeTypes)
        session.execute_write(cl3c.DeleteAllNodesRels, nodeTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':OCTdataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
        session.execute_write(cl3c.clearConstraint, None, driver)



    end = time.time()
    row={'name':OCTdataSet+'_clearConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
        session.execute_write(cl3c.createConstraint)


    end = time.time()
    row={'name':OCTdataSet+'_createConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint creating done: took '+str(end - last)+' seconds')
    last = end




print("-------------------------------------------------------------------------------------------------------------------------")


step_load_OCPS_Concepts=True
if step_load_OCPS_Concepts :
    print('                      ')
    print('Step4 - Creating OCPS Concepts Nodes...')
    # convert each record in the CSV table into an Event node
    query, testingQ = cl3c.loadOCPSConcepts(header_OCT_Node, OCT_Neo4JImport_OCT_Node_FileName, OCTdataSet)  # generate query to create all events with all log columns as properties
    print(query)
    print(testingQ)
    cl3c.runQuery(driver, query)

    end = time.time()
    row={'name':OCTdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('SCT nodes done: took '+str(end - last)+' seconds')
    last = end



print("-------------------------------------------------------------------------------------------------------------------------")




step_link_Concepts=True
if step_link_Concepts:
    print('                      ')
    print('Step5 - Creating Relationship between all Concepts......')
    print("Inputs:")
    print("OCPS_REL_MappingRelation=", OCPS_REL_MappingRelation)

    for item in OCPS_REL_MappingRelation:
        with driver.session() as session:
            session.execute_write(cl3c.link_Concepts, item[0], item[1], item[2], item[3])


    end = time.time()
    row={'name':OCTdataSet+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print(':HAS relationships done: took '+str(end - last)+' seconds')
    last = end



print("-------------------------------------------------------------------------------------------------------------------------")

step_useful_Query=True
if step_useful_Query:
    print('                      ')
    print('Step8 - Useful Query......')
    cl3c.usefulQuery()




print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': OCTdataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, OCT_Perf_FileName)
perf.to_csv(fullname)
driver.close()



