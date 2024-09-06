import os
import time
from neo4j import GraphDatabase

import Script10_CNM2_funcs as funcs
from tqdm import tqdm

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

DK4_dataSet = "DomainKnowledge4"
print("DK4_dataSet=", DK4_dataSet)

DK4_FileName= "J_CNM2"
print("DK4_FileName=", DK4_FileName)


DK4_FileName_com = dataPath + "/" + DK4_FileName + Data_Extension
DK4_Input_ICD_OCT_FileName = os.path.realpath(DK4_FileName_com)
print("DK4_Input_ICD_OCT_FileName=", DK4_Input_ICD_OCT_FileName)

DK4_Neo4JImport_ICD_OCT_FileName = DK4_FileName + "_Neo4j" + Data_Extension
print("DK4_Neo4JImport_ICD_OCT_FileName=", DK4_Neo4JImport_ICD_OCT_FileName)

DK4_icd_code ="icd_code"
DK4_OTC="SCT_ID"
print("DK4_icd_code=", DK4_icd_code)
print("DK4_OTC=", DK4_OTC)


print("")
print("************************** DF ****************************************************************************")


csv_ICD_OCT = funcs.ImportCSV(DK4_Input_ICD_OCT_FileName)
print("csv_ICD_OCT=\n",csv_ICD_OCT)


logSamples10 = funcs.Create_CSV_in_Neo4J_import10(csv_ICD_OCT)
print("logSamples10=", logSamples10)

header_ICD_OCT, csvLog_ICD_OCT = funcs.header_csv(logSamples10)
print("header_ICD_OCT=", header_ICD_OCT)
print("")
print("csvLog_ICD_OCT=\n", csvLog_ICD_OCT)
print("")





print("************************** DK 4 ****************************************************************************")

icdOCT=funcs.CreateRel(csvLog_ICD_OCT, DK4_icd_code, DK4_OTC)
print("icdOCT=",icdOCT)


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
print("---------------------------------------- Step L1 -----------------------------------------------------------------------------------")



step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepL1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART DK ##########################################
    relationTypesDK = ["CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV

    fileName = "Q1"


    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step L2 -----------------------------------------------------------------------------------")

icdOCT=icdOCT

step_link_potential_OCPS=True
if step_link_potential_OCPS:
    stepName='StepL2 - Creating Relationship between Clinical and Concepts ....'
    print('                      ')
    print(stepName)
    start = time.time()




    for item in icdOCT:
        with driver.session() as session:
            session.execute_write(funcs.Potential_OCPS, item[0], item[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)







print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()



