import os
import time
import Script13_CNM5_funcs as funcs
from neo4j import GraphDatabase



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


DK7_dataSet = "DomainKnowledge7"
print(DK7_dataSet, DK7_dataSet)

DK7_FileName= "M_CNM5"
print("DK7_FileName=", DK7_FileName)


DK7_FileName_com = dataPath + "/" + DK7_FileName + Data_Extension
DK7_Input_Activity_DK7_FileName = os.path.realpath(DK7_FileName_com)
print("DK7_Input_Activity_DK7_FileName=", DK7_Input_Activity_DK7_FileName)

DK7_Neo4JImport_Activity_DK7_FileName = DK7_FileName + "_Neo4j" + Data_Extension
print("DK7_Neo4JImport_Activity_DK7_FileName=", DK7_Neo4JImport_Activity_DK7_FileName)

DK7_Activity_Value_ID ="Activity_Instance_ID"
DK7_Disorders="Disorders_ID"
print("DK7_Activity_Value_ID=", DK7_Activity_Value_ID)
print("DK7_Disorders=", DK7_Disorders)





print("")
print("************************** DF ****************************************************************************")


csv_Event_Diagnoses = funcs.ImportCSV(DK7_Input_Activity_DK7_FileName)
print("csv_Event_Diagnoses=\n",csv_Event_Diagnoses)


logSamples13 = funcs.Create_CSV_in_Neo4J_import13(csv_Event_Diagnoses)
print("logSamples13=", logSamples13)

Event_Diagnoses_OCPS, csvLog_Event_Diagnoses = funcs.header_csv(logSamples13)
print("Event_Diagnoses_OCPS=", Event_Diagnoses_OCPS)
print("")
print("csvLog_Event_Diagnoses=\n", csvLog_Event_Diagnoses)
print("")




print("")
print("************************** Analysis ****************************************************************************")




Event_Diagnoses_MappingRelation=funcs.CreateMappingRelation2(csvLog_Event_Diagnoses,  DK7_Activity_Value_ID , DK7_Disorders)
print("Event_Diagnoses_MappingRelation=",Event_Diagnoses_MappingRelation)
print("")


print("************************** input for: Split ****************************************************************************")

Event_Diagnose_MappingRelation_split =funcs.dkSplit(Event_Diagnoses_MappingRelation)
print("Event_Diagnose_MappingRelation_split=",Event_Diagnose_MappingRelation_split)
print(len(Event_Diagnose_MappingRelation_split))
print("")

print("************************** input for: ScenarioEntity ****************************************************************************")


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





if myInput == 'main_Entities': #1
    print("************************** input for: Scenario 1 ****************************************************************************")
    # Main Disorder
    print("This scenario doesn't use DK7")


if myInput == 'main_Entities_plus_Disorder': #2
    print("************************** input for: Scenario 1 ****************************************************************************")
    # Main Disorder
    sc2_list =funcs.sc2(driver, Event_Diagnose_MappingRelation_split)
    print("sc2_list=",sc2_list)
    print(len(sc2_list))
    print("")

if myInput == 'main_Entities_plus_ICD': #3
    print("************************** input for: Scenario 2 ****************************************************************************")
    # ICD
    sc3_list =funcs.sc3(driver, Event_Diagnose_MappingRelation_split)
    print("sc3_list=",sc3_list)
    print(len(sc3_list))
    print("")


if myInput == 'main_Entities_plus_SCT': #5
    print("************************** input for: Scenario 4 ****************************************************************************")
    # SNOMED
    sc5_list =funcs.sc5(driver, Event_Diagnose_MappingRelation_split)
    print("sc5_list=",sc5_list)
    print(len(sc5_list))
    print("")



if myInput == 'main_Entities_plus_SCT_Level_One': #7
    print("************************** input for: Scenario 6 ****************************************************************************")


    distanceFromTLC = 1
    Semanti_tags = "disorder"
    ConceptType = "Concept"
    TLC_Semanti_tags = "finding"


    sc7_list =funcs.sc7(driver, Event_Diagnose_MappingRelation_split,distanceFromTLC,Semanti_tags,ConceptType,TLC_Semanti_tags)
    print("sc7_list=",sc7_list)
    print(len(sc7_list))
    print("")

if myInput == 'main_Entities_plus_ICD_one': #8
    print("************************** input for: Scenario 7 ****************************************************************************")
    # ICD Specific

    icdCode = 12346

    if icdCode!=0:
        icdCode = icdCode
    else:
        icdCode = funcs.sc8_icdFinder(driver)


    print("icdCode=", icdCode)
    sc8_list =funcs.sc8(driver, Event_Diagnose_MappingRelation_split,icdCode)
    print("sc8_list=",sc8_list)
    print(len(sc8_list))
    print("")


if myInput == 'main_Entities_plus_SCT_one': #9
    print("************************** input for: Scenario 8 ****************************************************************************")
    # SNOMED Specific
    conceptId = 12346

    if conceptId!=0:
        conceptId = conceptId
    else:
        conceptId = funcs.sc8_icdFinder(driver)

    print("conceptId=", conceptId)
    sc9_list =funcs.sc9(driver, Event_Diagnose_MappingRelation_split,conceptId)
    print("sc9_list=",sc9_list)
    print(len(sc9_list))
    print("")




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


print("************************** From cl1: ****************************************************************************")



print(" ")
print("---------------------------------------- Step O1 -----------------------------------------------------------------------------------")

EntityLists=entityList
print(EntityLists)


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepO1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()




    ############PART DK ##########################################
    relTypePartially = ["CORR","Scenario", "2"]

    ############PART V ##########################################
    relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

    relationTypes = relationTypesV



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0],relTypePartially[1],relTypePartially[2])
        session.execute_write(funcs.deletePartRel)
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deletePartNode,EntityLists[i])
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deleteProperty,EntityLists[i])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step O2 -----------------------------------------------------------------------------------")

stepName = 'StepO3 - Creating Relationship between Events and Disorders ....'
print('                      ')
print(stepName)
start = time.time()
myInput=myInput
print("myInput=",myInput)



if myInput == 'main_Entities': #1
    print("This scenario doesn't use DK7")


if myInput == 'main_Entities_plus_Disorder': #2
    relList = sc2_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(funcs.Event_Scenario_1, item[0], item[1],"2")



if myInput == 'main_Entities_plus_ICD': #3
    relList = sc3_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(funcs.Event_Scenario_2, item[0], item[1],"2")



if myInput == 'main_Entities_plus_SCT': #5
    relList = sc5_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(funcs.Event_Scenario_4, item[0], item[1],"2")



if myInput == 'main_Entities_plus_SCT_Level_One': #7
    relList = sc7_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(funcs.Event_Scenario_6, item[0], item[1],"2")



if myInput == 'main_Entities_plus_ICD_one': #8
    relList = sc8_list
    for item in relList:
        with driver.session() as session:
            session.execute_write(funcs.Event_Scenario_7, item[0], item[1],"2")





end = time.time()
funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print("-------------------------------------------------------------------------------------------------------------------------")


driver.close()



