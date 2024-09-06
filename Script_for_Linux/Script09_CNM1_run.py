import os
import time
from neo4j import GraphDatabase

import Script09_CNM1_funcs  as funcs




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


DK3_dataSet = "DomainKnowledge3"
print("DK3_dataSet=", DK3_dataSet)

DK3_FileName= "I_CNM1"
print("DK3_FileName=", DK3_FileName)


DK3_FileName_com = dataPath + "/" + DK3_FileName + Data_Extension
DK3_Input_Potential_DK3_FileName = os.path.realpath(DK3_FileName_com)
print("DK3_Input_Potential_DK3_FileName=", DK3_Input_Potential_DK3_FileName)

DK3_Neo4JImport_Potential_OCPS_FileName = DK3_FileName + "_Neo4j" + Data_Extension
print("DK3_Neo4JImport_Potential_OCPS_FileName=", DK3_Neo4JImport_Potential_OCPS_FileName)

DK3_Disorders ="Disorders_ID"
DK3_icd_code="icd_code"
print("DK3_Disorders=",DK3_Disorders)
print("DK3_icd_code=",DK3_icd_code)




print("")
print("************************** DF ****************************************************************************")


csv_DP = funcs.ImportCSV(DK3_Input_Potential_DK3_FileName)
print("csv_DP=\n",csv_DP)


logSamples9 = funcs.Create_CSV_in_Neo4J_import9(csv_DP)
print("logSamples9=", logSamples9)

header_Diagnose_Potential, csvLog_Diagnose_Potential = funcs.header_csv(logSamples9)
print("header_Diagnose_Potential=", header_Diagnose_Potential)
print("")
print("csvLog_Diagnose_Potential=\n", csvLog_Diagnose_Potential)
print("")




print("************************** DK 3 ****************************************************************************")




DiagClinRel=funcs.CreateRel(csvLog_Diagnose_Potential, DK3_Disorders, DK3_icd_code)
print("DiagClinRel=",DiagClinRel)


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




print("************************** From cl1: ****************************************************************************")


driver = driver
Perf_file_path = Perf_file_path



print(" ")
print("---------------------------------------- Step K1 -----------------------------------------------------------------------------------")




step_Clear_DK1_DB=True
if step_Clear_DK1_DB:
    stepName='StepK1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()


    ############PART DK ##########################################
    relationTypesDK = ["LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step K2 -----------------------------------------------------------------------------------")


DiagClinRel=DiagClinRel



step_link_Entity1_Potential=True
if step_link_Entity1_Potential:
    stepName='StepK2 - Creating Relationship between Disorders and Clinical ....'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("DiagClinRel=",DiagClinRel)
    print("")


    for item in DiagClinRel:

        with driver.session() as session:
            session.execute_write(funcs.Entity1_Potential_Entities, item[0], item[1])





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()


