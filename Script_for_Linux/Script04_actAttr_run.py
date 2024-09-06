import os
import csv
import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os

from neo4j import GraphDatabase


import Script04_actAttr_funcs as funcs



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

AcP_dataSet = "EventLogActivitiesProperties"
print("AcP_dataSet=", AcP_dataSet)


AcP_FileName= "E_ActivityAttributes"
print("AcP_FileName=", AcP_FileName)


AcP_FileName_com = dataPath + "/" + AcP_FileName + Data_Extension
AcP_Input_PoNode_FileName = os.path.realpath(AcP_FileName_com)
print("AcP_Input_PoNode_FileName=", AcP_Input_PoNode_FileName)


AcP_Neo4JImport_PoNode_FileName = AcP_FileName + "_Neo4j" + Data_Extension
print("AcP_Neo4JImport_PoNode_FileName=", AcP_Neo4JImport_PoNode_FileName)





AcP_acID ="Activity_Attributes_ID"
AcP_activityName="Activity"
AcP_activitySynonym="Activity_Synonym"
AcP_label="Attribute"
AcP_Value="Attribute_Value"
print("AcP_acID=", AcP_acID)
print("AcP_activityName=", AcP_activityName)
print("AcP_activitySynonym=", AcP_activitySynonym)
print("AcP_label=", AcP_label)
print("AcP_Value=", AcP_Value)



print("")
print("************************** DF ****************************************************************************")


EnP_Node_csv = funcs.ImportCSV(AcP_Input_PoNode_FileName)
# print("EnP_Node_csv=\n",EnP_Node_csv)
print("")

logSamples21 = funcs.Create_CSV_in_Neo4J_import3(EnP_Node_csv)
print("logSamples21=", logSamples21)

header_EnP, csvLog_EnP = funcs.header_csv(logSamples21)
print("header_EnP=", header_EnP)
print("")
print("csvLog_EnP=\n", csvLog_EnP)
print("")



print("************************** input from cl1 ****************************************************************************")

acProp_1=funcs.CreatePro1(csvLog_EnP, AcP_acID, AcP_activityName, AcP_activitySynonym, AcP_label, AcP_Value)
print("acProp_1=",acProp_1)

'''
acProp_2=funcs.CreatePro2(csvLog_EnP, AcP_acID)
print("acProp_2=",acProp_2)
'''

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
dataSet=AcP_dataSet
Perf_file_path = Perf_file_path



print(" ")
print("---------------------------------------- Step D1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    ############PART E ##########################################
    nodeTypesE = ["Feature"]
    relationTypesE = ["Assign"]


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


    nodeTypes = nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes,)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step D2 -----------------------------------------------------------------------------------")



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



print(" ")
print("---------------------------------------- Step D3 -----------------------------------------------------------------------------------")


step_createConstraint=True
if step_createConstraint:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.createConstraint, nodeTypesE)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D4 -----------------------------------------------------------------------------------")

acProp_1=acProp_1


step_createProperty=True
if step_createProperty:
    stepName='StepD4 - Creating Property Node...'
    print('                      ')
    print(stepName)
    start = time.time()
    print("")
    print("Inputs:")
    print("acProp_1=",acProp_1)
    print("")



    for name in acProp_1:
        with driver.session() as session:
            session.execute_write(funcs.createProperty, name[0], name[1] , name[2], name[3] , name[4])




    # table to measure performance
    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D5 -----------------------------------------------------------------------------------")


acProp_1=acProp_1


step_createDomains=True
if step_createDomains:
    stepName='StepD5 - Creating Property Entity Rel...'
    print('                      ')
    print(stepName)
    start = time.time()
    print("")
    print("Inputs:")
    print("acProp_1=",acProp_1)
    print("")


    for name in acProp_1:
        with driver.session() as session:
            session.execute_write(funcs.createEnProperty, name[0])




    # table to measure performance
    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


driver.close()