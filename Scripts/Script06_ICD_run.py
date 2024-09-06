import os
import os



import time, csv
from neo4j import GraphDatabase

from tqdm import tqdm


import Script06_ICD_funcs as funcs


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


CL_dataSet = "ClinicalICD"
print("CL_dataSet=", CL_dataSet)


CL_FileName= "G_ICD"
print("CL_FileName=", CL_FileName)

CL_FileName_com = dataPath + "/" + CL_FileName + Data_Extension
CE_Input_PoNode_FileName = os.path.realpath(CL_FileName_com)
print("CE_Input_PoNode_FileName=", CE_Input_PoNode_FileName)

CE_Neo4JImport_PoNode_FileName = CL_FileName + "_Neo4j" + Data_Extension
print("CE_Neo4JImport_PoNode_FileName=", CE_Neo4JImport_PoNode_FileName)

CE_ClinicalEntity ="icd_Origin"
CE_icd_code="icd_code"
CE_icd_version="icd_version"
CE_icd_code_title="icd_code_title"
print("CE_ClinicalEntity=", CE_ClinicalEntity)
print("CE_icd_code=", CE_icd_code)
print("CE_icd_version=", CE_icd_version)
print("CE_icd_code_title=", CE_icd_code_title)




print("")
print("************************** DF ****************************************************************************")


csv_CE = funcs.ImportCSV(CE_Input_PoNode_FileName)
print("csv_CE=",csv_CE)


logSamples21 = funcs.Create_CSV_in_Neo4J_import5(csv_CE)
print("logSamples21=", logSamples21)

header_CE, csvLog_CE = funcs.header_csv(logSamples21)
print("header_CE=", header_CE)
print("")
print("csvLog_CE=\n", csvLog_CE)
print("")



print("************************** input from cl1 ****************************************************************************")

caseICD=funcs.CreateCase(csvLog_CE, CE_ClinicalEntity, CE_icd_code, CE_icd_version, CE_icd_code_title)
print("caseICD=",caseICD)


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


driver = driver
CEdataSet=CL_dataSet


Perf_file_path = Perf_file_path







print(" ")
print("---------------------------------------- Step G1 -----------------------------------------------------------------------------------")




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepG1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()




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

    nodeTypes = nodeTypesH + nodeTypesI
    relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step G2 -----------------------------------------------------------------------------------")


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
print("---------------------------------------- Step G3 -----------------------------------------------------------------------------------")




step_createConstraint=True
if step_createConstraint:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()


    with driver.session() as session:
        session.execute_write(funcs.createConstraint, nodeTypesH)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step G4 -----------------------------------------------------------------------------------")



caseICD=caseICD

step_icd_Nodes=True
if step_icd_Nodes :
    stepName='StepF4 - Creating ICD Nodes....'
    print('                      ')
    print(stepName)
    start = time.time()



    for item in caseICD:
        with driver.session() as session:
            session.execute_write(funcs.icd_Nodes, item[0], item[1], item[2], item[3])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- -----------------------------------------------------------------------------------")


driver.close()