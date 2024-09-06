import os
import os
from neo4j import GraphDatabase
import pandas as pd
import time, csv
from neo4j import GraphDatabase

import Script05_actDomain_funcs as funcs





print("")
print("**************************  From Entry cl1 ****************************************************************************")

medDataDirectory = f'Data'
dataPath = os.path.realpath(medDataDirectory)
Data_Extension = '.csv'


print("")
print("**************************  Performance File ****************************************************************************")


perf_Name= 'all_Performance'
Perf_FileName = dataPath + "/" + perf_Name + Data_Extension
Perf_file_path = os.path.realpath(Perf_FileName)
print("Perf_file_path=", Perf_file_path)



print("")
print("**************************  From Entry cl2 ****************************************************************************")


uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

Neo4JImport = funcs.Neo4j_import_dir(driver)
print("Neo4JImport=", Neo4JImport)



print("")
print("************************** From Nodes ****************************************************************************")


DP_dataSet = "EventLogActivitiesDomain"
print("DP_dataSet=", DP_dataSet)

DP_FileName= "F_ActivitiesDomain"
print("DP_FileName=", DP_FileName)

DP_FileName_com = dataPath + "/" + DP_FileName + Data_Extension
DP_Input_PoNode_FileName = os.path.realpath(DP_FileName_com)
print("DP_Input_PoNode_FileName=", DP_Input_PoNode_FileName)

DP_Neo4JImport_PoNode_FileName = DP_FileName + "_Neo4j" + Data_Extension
print("DP_Neo4JImport_PoNode_FileName=", DP_Neo4JImport_PoNode_FileName)


ACT_Domain = "Domain"

print("ACT_Domain=", ACT_Domain)


print("")
print("************************** DF ****************************************************************************")


csv_ACT = funcs.ImportCSV(DP_Input_PoNode_FileName)
print("csv_ACT",csv_ACT)


logSamples21 = funcs.Create_CSV_in_Neo4J_import4(csv_ACT)
print("logSamples21=", logSamples21)

header_ACT, csvLog_header_ACT = funcs.header_csv(logSamples21)
print("header_ACT=", header_ACT)
print("")
print("csvLog_header_ACT=\n", csvLog_header_ACT)
print("")



print("************************** input from cl1 ****************************************************************************")

domainNode=funcs.domainNode(csvLog_header_ACT)
print("domainNode=",domainNode)

#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################

driver=driver
Perf_file_path = Perf_file_path





print("-------------------------------------------------------------------------------------------------------------------------")




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepE1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART F ##########################################
    nodeTypesF = ["Domain"]

    ############PART G ##########################################
    nodeTypesG = []
    relationTypesG = []

    ############PART H ##########################################
    nodeTypesH = ["Clinical"]

    ############PART I ##########################################
    nodeTypesI = ["Concept"]
    relationTypesI = ["ANCESTOR_OF"]

    ############PART DK2 ##########################################

    relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]


    nodeTypes = nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2





    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.DeleteNodes, nodeTypes,)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print("-------------------------------------------------------------------------------------------------------------------------")


step_Clear_OCT_Constraints=True
if step_Clear_OCT_Constraints:
    stepName='StepD2 - Dropping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.clearConstraint, None, driver, nodeTypes)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")


step_createConstraint=True
if step_createConstraint:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.createConstraint, nodeTypesF)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print("-------------------------------------------------------------------------------------------------------------------------")


domainNode=domainNode


step_Domain_Nodes=True
if step_Domain_Nodes :
    stepName='StepE4 - Creating Domain Nodes...'
    print('                      ')
    print(stepName)
    start = time.time()




    print("domainNode=", domainNode)
    # convert each record in the CSV table into an Event node

    for item in domainNode:

        with driver.session() as session:
            session.execute_write(funcs.Domain_Nodes, item)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print("-------------------------------------------------------------------------------------------------------------------------")



driver.close()




