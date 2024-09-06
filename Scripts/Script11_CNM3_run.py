import os
import time
from neo4j import GraphDatabase
import Script11_CNM3_funcs as funcs




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


DK5_dataSet = "DomainKnowledge5"
print("DK5_dataSet=", DK5_dataSet)

DK5_FileName= "K_CNM3"
print("DK5_FileName=", DK5_FileName)


DK5_FileName_com = dataPath + "/" + DK5_FileName + Data_Extension
DK5_Input_Activity_DK5_FileName = os.path.realpath(DK5_FileName_com)
print("DK5_Input_Activity_DK5_FileName=", DK5_Input_Activity_DK5_FileName)

DK5_Neo4JImport_Activity_DK5_FileName = DK5_FileName + "_Neo4j" + Data_Extension
print("DK5_Neo4JImport_Activity_DK5_FileName=", DK5_Neo4JImport_Activity_DK5_FileName)

DK5_Activity ="Activity"
DK5_Activity_Synonym="Activity_Synonym"
DK5_OTC="SCT_ID"
DK5_SCTCode="SCT_Code"


print("DK5_Activity=",DK5_Activity)
print("DK5_Activity_Synonym=",DK5_Activity_Synonym)
print("DK5_OTC=",DK5_OTC)
print("DK5_SCTCode=",DK5_SCTCode)





print("")
print("************************** DF ****************************************************************************")


csv_Activity_OCT = funcs.ImportCSV(DK5_Input_Activity_DK5_FileName)
print("csv_Activity_OCT=\n",csv_Activity_OCT)


logSamples11 = funcs.Create_CSV_in_Neo4J_import11(csv_Activity_OCT)
print("logSamples11=", logSamples11)

header_Activity_OCT, csvLog_Activity_OCT = funcs.header_csv(logSamples11)
print("header_Activity_OCT=", header_Activity_OCT)
print("")
print("csvLog_Activity_OCT=\n", csvLog_Activity_OCT)
print("")




print("")
print("************************** Analysis ****************************************************************************")



Activity_OCT_MappingRelation=funcs.CreateMappingRelation3(csvLog_Activity_OCT, DK5_Activity,DK5_Activity_Synonym,DK5_OTC,DK5_SCTCode)
print("Activity_OCPS_MappingRelation=",Activity_OCT_MappingRelation)
print("")

print("************************** input from cl1: Final ****************************************************************************")

final_Activity_OCT_MappingRelation = Activity_OCT_MappingRelation
print("final_Activity_OCT_MappingRelation=", final_Activity_OCT_MappingRelation)
print(len(final_Activity_OCT_MappingRelation))

'''

ActivityNode = ["Activity"]
colTitle = "Syn"

or

ActivityNode = ["Concept"]
colTitle = "termA"

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



print("************************** From cl1: ****************************************************************************")


driver = driver
Perf_file_path = Perf_file_path



print(" ")
print("---------------------------------------- Step M1 -----------------------------------------------------------------------------------")




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepM1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()


    ############PART DK ##########################################
    relationTypesDK = ["MAPPED_TO", "TIED","TYPE_OF"]
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
print("---------------------------------------- Step M2 -----------------------------------------------------------------------------------")

Activity_OCT_MappingRelation=final_Activity_OCT_MappingRelation


step_link_Activity_OCPS=True
if step_link_Activity_OCPS:
    stepName='StepM2 - Creating Relationship between Activity and Concepts  ....'
    print('                      ')
    print(stepName)
    start = time.time()



    for item in Activity_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(funcs.Activity_OCPS, item[0], item[1], item[2], item[3])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print("-------------------------------------------------------------------------------------------------------------------------")

print(" ")
print("---------------------------------------- Step M3 -----------------------------------------------------------------------------------")

Activity_OCT_MappingRelation=final_Activity_OCT_MappingRelation


step_link_ActivityProperty_OCPS=True
if step_link_ActivityProperty_OCPS:
    stepName='StepM2 - Creating Relationship between Activity Property and Concepts  ....'
    print('                      ')
    print(stepName)
    start = time.time()



    for item in Activity_OCT_MappingRelation:
        with driver.session() as session:
            session.execute_write(funcs.ActivityProperty_OCPS, item[0], item[1], item[2], item[3])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")

driver.close()




