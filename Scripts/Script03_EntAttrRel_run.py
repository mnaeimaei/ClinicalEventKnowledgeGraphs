import os
import time
import Script03_EntAttrRel_funcs as funcs
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


EnP_FileName= "D_EntitiesAttributeRel"
print("EnP_FileName=", EnP_FileName)

EnPR_FileName_com = dataPath + "/" + EnP_FileName + Data_Extension
EnP_Input_PoNode_FileName_2 = os.path.realpath(EnPR_FileName_com)
print("EnP_Input_PoNode_FileName_2=", EnP_Input_PoNode_FileName_2)

EnP_Neo4JImport_PoNode_FileName_2 = EnP_FileName + "_Neo4j" + Data_Extension
print("EnP_Neo4JImport_PoNode_FileName_2=", EnP_Neo4JImport_PoNode_FileName_2)

EnP_Entity_Origin1 ="Origin1"
EnP_Entity_ID1="ID1"
EnP_Entity_Origin2="Origin2"
EnP_Entity_ID2="ID2"
print("EnP_Entity_Origin1=", EnP_Entity_Origin1)
print("EnP_Entity_ID1=", EnP_Entity_ID1)
print("EnP_Entity_Origin2=", EnP_Entity_Origin2)
print("EnP_Entity_ID2=", EnP_Entity_ID2)





print("")
print("************************** DF ****************************************************************************")



EnP_Node_csv = funcs.ImportCSV(EnP_Input_PoNode_FileName_2)
# print("EnP_Node_csv=\n",EnP_Node_csv)
print("")

logSamples21 = funcs.Create_CSV_in_Neo4J_import22(EnP_Node_csv)
print("logSamples21=", logSamples21)

header_EnP_REL, csvLog_EnP_REL = funcs.header_csv(logSamples21)
print("header_EnP_REL=", header_EnP_REL)
print("")
print("csvLog_EnP_REL=\n", csvLog_EnP_REL)
print("")


print("************************** input from cl1 ****************************************************************************")



RelList=funcs.CreateLoL(csvLog_EnP_REL)
print("RelList=",RelList)

RelFinal=funcs.ListMaker(RelList)
print("RelFinal=",RelFinal)


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
dataSet=EnP_dataSet
Perf_file_path = Perf_file_path


print(" ")
print("---------------------------------------- Step D1 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()


    ############PART D ##########################################

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


    nodeTypes = nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesD + relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2],)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D2 -----------------------------------------------------------------------------------")

print(nodeTypes)

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
print("---------------------------------------- Step D5 -----------------------------------------------------------------------------------")


RelFinal=RelFinal


step_createDomains=True
if step_createDomains:
    stepName='StepD5 - Creating Rel between Entities ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("RelFinal=",RelFinal)
    print("")

    for item in RelFinal:
        with driver.session() as session:
            session.execute_write(funcs.intraEntitiesRel,  item[0], item[1], item[2], item[3])





    # table to measure performance
    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


driver.close()


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








print("************************** DF 1 ****************************************************************************")

df1=funcs.creatingDfFromGraph(driver)
print("\ndf1=\n",df1)

dfWithRankingAdmission=funcs.rankingAdm(df1)
print("\ndfWithRankingAdmission=\n",dfWithRankingAdmission)

df1Disorders=funcs.groupingDisorder(df1)
print("\ndf1Disorders=\n",df1Disorders)

finalTable=funcs.lefJoinTable(df1Disorders, dfWithRankingAdmission)
print("\nfinalTable=\n",finalTable)

dfComparable=funcs.comparing(finalTable)
print("\ndfComparable=\n",dfComparable)

print("************************** DF 2 ****************************************************************************")

dfComparable=funcs.comparing(finalTable)
print("\ndfComparable=\n",dfComparable)

dfFinal=funcs.createFinal(dfComparable)
print("\ndfFinal=\n",dfFinal)

Treated=funcs.createTreated(dfFinal,"Treated")
print("\nTreated=\n",Treated)


NotTreated=funcs.createTreated(dfFinal,"New")
print("\nNotTreated=\n",NotTreated)


New=funcs.createTreated(dfFinal,"notTreated")
print("\nNew=\n",New)

print("************************** DF 3 ****************************************************************************")

multiMorbidityValue=funcs.multiDis(finalTable)
print("multiMorbidityValue=",multiMorbidityValue)

treatedValue=funcs.treatVal(dfFinal)
print("treatedValue=",treatedValue)


notTreatedValue=funcs.notVal(dfFinal)
print("notTreatedValue=",notTreatedValue)

newValue=funcs.newVal(dfFinal)
print("newValue=",newValue)

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
dataSet=EnP_dataSet
Perf_file_path = Perf_file_path



print(" ")
print("---------------------------------------- Step D4 -----------------------------------------------------------------------------------")


step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    stepName='StepD1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()

    ############PART D ##########################################

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


    nodeTypes = nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH + nodeTypesI
    relationTypes = relationTypesE + relationTypesG + relationTypesI + relationTypesDK + relationTypesV + relationTypesD2



    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])

    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step D5 -----------------------------------------------------------------------------------")



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
print("---------------------------------------- Step D6 -----------------------------------------------------------------------------------")


Treated=Treated


step_treated=True
if step_treated:
    stepName='StepJ2 - Adding Disorder to Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("Treated=",Treated)
    print("")

    for item in Treated:
        with driver.session() as session:
            session.execute_write(funcs.admTreated_Fun, item[0], item[1])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D7 -----------------------------------------------------------------------------------")


NotTreated=NotTreated


step_NotTreated=True
if step_NotTreated:
    stepName='StepJ2 - Adding Disorder to Not Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("NotTreated=",NotTreated)
    print("")

    for item in NotTreated:
        with driver.session() as session:
            session.execute_write(funcs.admNotTreated_Fun, item[0], item[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D8 -----------------------------------------------------------------------------------")


New=New


step_New=True
if step_New:
    stepName='StepJ2 - Adding Disorder to new Treated ....'
    print('                      ')
    print(stepName)
    start = time.time()
    print("")
    print("Inputs:")
    print("New=",New)
    print("")



    for item in New:
        with driver.session() as session:
            session.execute_write(funcs.admNew_Fun, item[0], item[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step D9 -----------------------------------------------------------------------------------")


multiMorbidityValue=multiMorbidityValue


step_multiMorbidityValue=True
if step_multiMorbidityValue:
    stepName='StepJ5 - Adding Values to Multimorbidity ....'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("multiMorbidityValue=",multiMorbidityValue)
    print("")

    for item in multiMorbidityValue:
        with driver.session() as session:
            session.execute_write(funcs.admMulti_Value, item[0], item[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step D10 -----------------------------------------------------------------------------------")


treatedValue=treatedValue


step_treatedValue=True
if step_treatedValue:
    stepName='StepJ6 - Adding Values  to treatedMorbids  ....'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("treatedValue=",treatedValue)
    print("")

    for item in treatedValue:
        with driver.session() as session:
            session.execute_write(funcs.admTreated_Value, item[0], item[1])





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D11 -----------------------------------------------------------------------------------")


notTreatedValue=notTreatedValue


step_notTreatedValue=True
if step_notTreatedValue:
    stepName='StepJ7 - Adding Values to untreatedMorbids'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("notTreatedValue=",notTreatedValue)
    print("")

    for item in notTreatedValue:
        with driver.session() as session:
            session.execute_write(funcs.admNotTreated_Value, item[0], item[1])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step D12 -----------------------------------------------------------------------------------")


newValue=newValue


step_newValue=True
if step_newValue:
    stepName='StepJ8 - Adding Values to newMorbids ....'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("newValue=",newValue)
    print("")

    for item in newValue:
        with driver.session() as session:
            session.execute_write(funcs.admNew_Value, item[0], item[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print("-------------------------------------------------------------------------------------------------------------------------")





driver.close()

