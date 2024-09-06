import os
import time
from neo4j import GraphDatabase

import Script08_SctRel_funcs as funcs




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


SCTR_dataSet = "SCTIDNodesRel"
print("SCTR_dataSet=", SCTR_dataSet)


SCTR_FileName= "H_SCT_REL"
print("SCTR_FileName=", SCTR_FileName)

SCTR_FileName_com = dataPath + "/" + SCTR_FileName + Data_Extension
OCT_Input_OCT_REL_FileName = os.path.realpath(SCTR_FileName_com)
print("OCT_Input_OCT_REL_FileName=", OCT_Input_OCT_REL_FileName)

OCT_Neo4JImport_OCT_REL_FileName = SCTR_FileName + "_Neo4j" + Data_Extension
print("OCT_Neo4JImport_OCT_REL_FileName=", OCT_Neo4JImport_OCT_REL_FileName)




OCT_OCT_REL_s0 ="sct_id_1"
OCT_OCT_REL_s0_code="sct_code_1"
OCT_OCT_REL_s1="sct_id_2"
OCT_OCT_REL_s1_code="sct_code_2"
print("OCT_OCT_REL_s0=",OCT_OCT_REL_s0)
print("OCT_OCT_REL_s0_code=",OCT_OCT_REL_s0_code)
print("OCT_OCT_REL_s1=",OCT_OCT_REL_s1)
print("OCT_OCT_REL_s1_code=",OCT_OCT_REL_s1_code)







print("")
print("************************** DF ****************************************************************************")


csv_OCT_REL = funcs.ImportCSV(OCT_Input_OCT_REL_FileName)
print("csv_OCT_REL=\n",csv_OCT_REL)

csv_OCT_REL[OCT_OCT_REL_s0] = csv_OCT_REL[OCT_OCT_REL_s0].astype(int)
csv_OCT_REL[OCT_OCT_REL_s1] = csv_OCT_REL[OCT_OCT_REL_s1].astype(int)

print("csv_OCT_REL=\n",csv_OCT_REL)

logSamples62 = funcs.Create_CSV_in_Neo4J_import62(csv_OCT_REL)
print("logSamples62=", logSamples62)




header_OCT_REL, csvLog_OCT_REL = funcs.header_csv(logSamples62)
print("header_OCT_REL=", header_OCT_REL)
print("")
print("csvLog_OCT_REL=\n", csvLog_OCT_REL)
print("")





print("")
print("************************** Analysis ****************************************************************************")

print("csvLog_OCT_REL=\n", type(csv_OCT_REL))


OCT_REL_MappingRelation=funcs.CreateMappingRelation1(csvLog_OCT_REL,OCT_OCT_REL_s0, OCT_OCT_REL_s0_code,OCT_OCT_REL_s1,OCT_OCT_REL_s1_code)
print("OCT_REL_MappingRelation=",OCT_REL_MappingRelation)


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


Perf_file_path = Perf_file_path


print(" ")
print("---------------------------------------- Step H1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=1
if step_Clear_OCT_DB==1:
    stepName='StepH1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART I ##########################################

    relationTypesI = ["ANCESTOR_OF"]

    ############PART DK2 ##########################################

    relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

    ############PART DK ##########################################
    relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]


    relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step H5 -----------------------------------------------------------------------------------")

step_link_Concepts=1
if step_link_Concepts==1:
    stepName='StepH5 - Creating Relationship between all Concepts ....'
    print('                      ')
    print(stepName)

    OCPS_REL_MappingRelation = OCT_REL_MappingRelation
    #print("OCPS_REL_MappingRelation=", OCPS_REL_MappingRelation)
    length = len(OCPS_REL_MappingRelation)

    start = time.time()



    print("OCPS_REL_MappingRelation=", OCPS_REL_MappingRelation)

    for item, k in zip(OCPS_REL_MappingRelation, range(length)) :
        with driver.session() as session:
            session.execute_write(funcs.link_Concepts, item[0], item[1], item[2], item[3])
        formatted_number = "{:.2f}".format(100 * k / length)
        print("Completed: ", formatted_number, "%")




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H6 -----------------------------------------------------------------------------------")

step_modify_concepts=1
if step_modify_concepts==1:
    stepName='StepH6 - Modifying Concept ....'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.modify_concept)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)






print("-------------------------------------------------------------------------------------------------------------------------")




driver.close()
