import os
import time
import Script02_EntAttr_funcs as funcs
from neo4j import GraphDatabase




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

EnP_dataSet = "EventLogEntities"
print("EnP_dataSet=", EnP_dataSet)


EA_FileName= "C_EntitiesAttributes"
print("EA_FileName=", EA_FileName)

EA_FileName_com = dataPath + "/" + EA_FileName + Data_Extension
EnP_Input_PoNode_FileName_1 = os.path.realpath(EA_FileName_com)
print("EnP_Input_PoNode_FileName_1=", EnP_Input_PoNode_FileName_1)


EnP_Neo4JImport_PoNode_FileName_1 = EA_FileName + "_Neo4j" + Data_Extension
print("EnP_Neo4JImport_PoNode_FileName_1=", EnP_Neo4JImport_PoNode_FileName_1)


EnP_Origin= "Origin"
EnP_ID= "ID"
EnP_Name= "Name"
EnP_Value= "Value"
EnP_Category= "Category"
print("EnP_Origin=", EnP_Origin)
print("EnP_ID=", EnP_ID)
print("EnP_Name=", EnP_Name)
print("EnP_Value=", EnP_Value)
print("EnP_Category=", EnP_Category)

print("")
print("************************** DF ****************************************************************************")

EnP_Node_csv = funcs.ImportCSV(EnP_Input_PoNode_FileName_1)
# print("EnP_Node_csv=\n",EnP_Node_csv)
print("")

logSamples21 = funcs.Create_CSV_in_Neo4J_import21(EnP_Node_csv)
print("logSamples21=", logSamples21)

header_EnP_Node, csvLog_EnP_Node = funcs.header_csv(logSamples21)
print("header_EnP_Node=", header_EnP_Node)
print("")
print("csvLog_EnP_Node=\n", csvLog_EnP_Node)
print("")






print("************************** input from cl1 ****************************************************************************")

NodeList=funcs.CreateLoL(csvLog_EnP_Node)
print("NodeList=",NodeList)


NodeEntity=funcs.CreateEntity(NodeList)
print("NodeEntity=",NodeEntity)



print("************************** Queries ****************************************************************************")




driver=driver
dataSet=EnP_dataSet
Perf_file_path = Perf_file_path
NodeEntity=NodeEntity
print(NodeEntity)


print(" ")
print("---------------------------------------- Step D1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    ############PART D ##########################################
    nodeTypesD = NodeEntity
    #['gender', 'newMorbids', 'age', 'treatedMorbids', 'untreatedMorbids', 'Disorder', 'Multimorbidity']
    relationTypesD = ["INCLUDED"]
    relationTypesD2 = [f'''INCLUDED {{Type:"last"}}''']

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


    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]


    nodeTypes = nodeTypesD + nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesD + relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2


    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
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
        session.execute_write(funcs.createConstraint, NodeEntity)


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D4 -----------------------------------------------------------------------------------")

NodeList=NodeList


step_createProperty=True
if step_createProperty:
    stepName='StepD4 - Creating Other Entities Node...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("NodeList=",NodeList)
    print("")

    for item in NodeList:
        with driver.session() as session:
            session.execute_write(funcs.OtherEntities_Nodes, item[0], item[1], item[2], item[3], item[4])

    print("NodeEntity=", NodeEntity)



    # table to measure performance
    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################





driver.close()