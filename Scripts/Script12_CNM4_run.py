import os
import time
from neo4j import GraphDatabase
import Script12_CNM4_funcs as funcs



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


DK61_dataSet = "DomainKnowledge61"
print("DK61_dataSet=", DK61_dataSet)

DK61_FileName= "L_CNM4_1"
print("DK61_FileName=", DK61_FileName)


DK61_FileName_com = dataPath + "/" + DK61_FileName + Data_Extension
Domain_Input_FileName_1 = os.path.realpath(DK61_FileName_com)
print("Domain_Input_FileName_1=", Domain_Input_FileName_1)

Domain_Neo4JImport_FileName_1 = DK61_FileName + "_Neo4j" + Data_Extension
print("Domain_Neo4JImport_FileName_1=", Domain_Neo4JImport_FileName_1)

Domain1_Activity ="Activity"
Domain1_Activity_Synonym="Activity_Synonym"
Domain1_Domain="Activity_Domain"
print("Domain1_Activity=", Domain1_Activity)
print("Domain1_Activity_Synonym=", Domain1_Activity_Synonym)
print("Domain1_Domain=", Domain1_Domain)




print("")
print("************************** DF ****************************************************************************")


csv_DK5_1 = funcs.ImportCSV(Domain_Input_FileName_1)
print("csv_DK5_1=\n",csv_DK5_1)


logSamples121 = funcs.Create_CSV_in_Neo4J_import12(csv_DK5_1)
print("logSamples121=", logSamples121)

header_DK5_1, csvLog_DK5_1 = funcs.header_csv(logSamples121)
print("header_DK5_1=", header_DK5_1)
print("")
print("csvLog_DK5_1=\n", csvLog_DK5_1)
print("")




print("")
print("************************** Analysis ****************************************************************************")




DK5_1_Rel=funcs.CreateMappingRelation3(csvLog_DK5_1, Domain1_Activity,Domain1_Activity_Synonym,Domain1_Domain)
print("DK5_1_Rel2=",DK5_1_Rel)
print("")

print("************************** input from DK5_2 ****************************************************************************")

DK62_dataSet = "DomainKnowledge62"
print("DK62_dataSet=", DK62_dataSet)

DK62_FileName= "L_CNM4_2"
print("DK62_FileName=", DK62_FileName)


DK62_FileName_com = dataPath + "/" + DK62_FileName + Data_Extension
Domain_Input_FileName_2 = os.path.realpath(DK62_FileName_com)
print("Domain_Input_FileName_2=", Domain_Input_FileName_2)

Domain_Neo4JImport_FileName_2 = DK62_FileName + "_Neo4j" + Data_Extension
print("Domain_Neo4JImport_FileName_2=", Domain_Neo4JImport_FileName_2)

Domain2_Domain ="Activity_Domain"
Domain2_OTC="SCT_ID"
Domain2_SCTCode="SCT_Code"
print("Domain2_Domain=", Domain2_Domain)
print("Domain2_OTC=", Domain2_OTC)
print("Domain2_SCTCode=", Domain2_SCTCode)



print("")
print("************************** DF ****************************************************************************")


csv_DK5_2 = funcs.ImportCSV(Domain_Input_FileName_2)
print("csv_DK5_2=\n",csv_DK5_2)


logSamples122 = funcs.Create_CSV_in_Neo4J_import12(csv_DK5_2)
print("logSamples122=", logSamples122)

header_DK5_2, csvLog_DK5_2 = funcs.header_csv(logSamples122)
print("header_DK5_2=", header_DK5_2)
print("")
print("csvLog_DK5_2=\n", csvLog_DK5_2)
print("")


DK5_2_Rel=funcs.CreateMappingRelation3(csvLog_DK5_2, Domain2_Domain,Domain2_OTC,Domain2_SCTCode)
print("DK5_2_Rel=",DK5_2_Rel)
print("")

print("************************** input for: 2:	DomainConcept ****************************************************************************")
# ICD
sc2_list =funcs.sc2(driver, DK5_1_Rel, DK5_2_Rel)
print("sc2_list=",sc2_list)
print(len(sc2_list))
print("")


print("************************** input for: 2:	Config ****************************************************************************")

myInput="main_Entities_plus_SCT"


#1
if myInput == "main_Entities" :

    entityList = ['Patient', 'Admission']
    entityListIDproperty = ['ID', 'ID']
    conditionProperty = ['Category', 'Category']
    conditionPropertyValue = ['Absolute', 'Absolute']

#2
if myInput == "main_Entities_plus_Disorder" :
    entityList = ["Disorder", 'Patient', 'Admission']
    entityListIDproperty = ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#3
if myInput == "main_Entities_plus_ICD" :
    entityList = ["Clinical", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#5
if myInput == "main_Entities_plus_SCT" :
    entityList = ["Concept", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#7
if myInput == "main_Entities_plus_SCT_Level_One":
    entityList = ["Concept", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']


#8
if myInput == "main_Entities_plus_ICD_one":
    entityList = ["Clinical", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']



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
print("---------------------------------------- Step N1 -----------------------------------------------------------------------------------")



EntityLists=entityList
print(EntityLists)




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepN1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()



    ############PART DK ##########################################
    relationTypesDK = ["TIED", "TYPE_OF"]
    relTypePartially = ["CORR", "Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesDK + relationTypesV



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])
        session.execute_write(funcs.deletePartRel)
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deletePartNode,EntityLists[i])
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deleteProperty,EntityLists[i])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step N2 -----------------------------------------------------------------------------------")


start = time.time()


stepName='StepN2 - Creating Relationship between Domain to Concepts  ....'
print('                      ')
print(stepName)
Form_OCT_MappingRelation = DK5_2_Rel
for item in Form_OCT_MappingRelation:
    with driver.session() as session:
        session.execute_write(funcs.Activity_OCPS, item[0], item[1], item[2])

stepName = 'StepN3 - Creating Relationship between Activities and Concept ....'
print('                      ')
print(stepName)
relList = sc2_list
for item in relList:
    with driver.session() as session:
        session.execute_write(funcs.Domain_Scenario_2, item[0], item[1],item[2],item[3])


stepName = 'StepN4 - Creating Relationship between Activities Properties and Concept ....'
print('                      ')
print(stepName)
relList = sc2_list
for item in relList:
    with driver.session() as session:
        session.execute_write(funcs.Domain_Scenario_2_Proprty, item[0], item[1],item[2],item[3])



end = time.time()
funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print("-------------------------------------------------------------------------------------------------------------------------")



driver.close()



