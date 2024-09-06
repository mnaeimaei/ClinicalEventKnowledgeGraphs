
import os
import time
from neo4j import GraphDatabase


import Script01_EventLog_funcs as funcs


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


funcs.create_csv_with_row(Perf_file_path)


print("")
print("**************************  From Entry cl2 ****************************************************************************")

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

Neo4JImport = funcs.Neo4j_import_dir(driver)
print("Neo4JImport=", Neo4JImport)


print("")
print("************************** From Event Log Entry ****************************************************************************")



ED_dataSet= "EventLog"
print("ED_dataSet=", ED_dataSet)

ED_FileName= "B_EventLog"
print("ED_FileName=", ED_FileName)

Event_FileName = dataPath + "/" + ED_FileName + Data_Extension
ED_Input_Event_FileName = os.path.realpath(Event_FileName)
print("ED_Input_Event_FileName=", ED_Input_Event_FileName)



ED_Neo4JImport_Event_FileName = ED_FileName + "_Neo4j" + Data_Extension
print("ED_Neo4JImport_Event_FileName=", ED_Neo4JImport_Event_FileName)



ED_eventIdTitle= "Event_ID"
ED_Activity= "Activity"
ED_ActivitySynonym= "Activity_Synonym"
ED_Activity_Value_ID= "Activity_Instance_ID"
ED_Activity_Properties_ID= "Activity_Attributes_ID"
Timestamp= "Timestamp"
ED_EnNum= 2
dicEntOrigin= {'Entity1Origin': 'Entity1_Origin', 'Entity2Origin': 'Entity2_Origin'}
dicEntID= {'Entity1ID': 'Entity1_ID', 'Entity2ID': 'Entity2_ID'}
EntityIDColumnList = ['Entity1_ID', 'Entity2_ID']
EntityOrgColumnList = ['Entity1_Origin', 'Entity2_Origin']


print("ED_eventIdTitle=", ED_eventIdTitle)
print("ED_Activity=", ED_Activity)
print("ED_ActivitySynonym=", ED_ActivitySynonym)
print("ED_Activity_Value_ID=", ED_Activity_Value_ID)
print("ED_Activity_Properties_ID=", ED_Activity_Properties_ID)
print("Timestamp=", Timestamp)
print("ED_EnNum=", ED_EnNum)
print("Entities Origin Columns (dicEntOrigin)=", dicEntOrigin)
print("Entities ID Column (dicEntID)=", dicEntID)
print("EntityIDColumnList =", EntityIDColumnList)
print("EntityOrgColumnList =", EntityOrgColumnList)

print("")
print("************************** DF ****************************************************************************")

#Importing Input CSV File


df = funcs.ImportCSV(ED_Input_Event_FileName)
print(df)



df2 = funcs.removeDecimalInIDs(df, dicEntID, ED_EnNum)
# print(df2.to_string())



df22 = funcs.removeDecimalInAct(df2, ED_Activity_Properties_ID)





logSamples1 = funcs.Create_CSV_in_Neo4J_import1(df22, EntityIDColumnList, ED_Activity_Properties_ID, Timestamp)
print("logSamples1=", logSamples1)





header_ED, csvLog_ED = funcs.header_csv(logSamples1)
print("header_ED=", header_ED)
print("")
print("csvLog_ED=\n", csvLog_ED)
print("")



print("")
print("************************** Step1:  Clearing,Droping,Creating*******************************************************************")

EntityOrgValue=funcs.EntityOriginValue(csvLog_ED, dicEntOrigin,  ED_EnNum)
print("Entities Origin Value (EntityOrgValue) =", EntityOrgValue)

dicEnt=funcs.Entities_Alias_values (ED_EnNum, EntityOrgValue)
#print(dicEnt)
locals().update(dicEnt)
print("Entities Alias Values (dicEnt)=",dicEnt)

EntityLists=funcs.flat_list(EntityOrgValue)
print("EntityLists =", EntityLists)



print("")
print("************************** StepC5:  Creating Events ****************************************************************************")

actList, actProIdList, actSynList, actValdList, entityIdList, entityOriginList, eventList, timeList=funcs.CreateActivityList(csvLog_ED,ED_Activity,ED_Activity_Properties_ID,ED_ActivitySynonym,ED_Activity_Value_ID,EntityIDColumnList,EntityOrgColumnList,ED_eventIdTitle,Timestamp)
print("actList =", actList)
print("actProIdList =", actProIdList)
print("actSynList =", actSynList)
print("actValdList =", actValdList)
print("entityIdList =", entityIdList)
print("entityOriginList =", entityOriginList)
print("eventList =", eventList)
print("timeList =", timeList)



print("")
print("************************** StepC5:  Creating Entitirs ****************************************************************************")




EntityOriginIDValue=funcs.EntityOriginIDValue2(csvLog_ED,dicEntID,  ED_EnNum)
print("EntityOriginIDValue =", EntityOriginIDValue)

model_entities_Temp=funcs.EntityAndEntityOrg_Creater_Final(dicEntOrigin,EntityOriginIDValue, dicEntID, ED_EnNum, dicEnt)
print("model_entities_Temp=",model_entities_Temp)

model_entities=funcs.model_entities(model_entities_Temp,ED_EnNum)
print("model_entities =", model_entities)


print("")
print("************************** StepC6:  Creating Activities ****************************************************************************")


actNode=funcs.activityNode(csvLog_ED,ED_Activity,ED_ActivitySynonym)
print("actNode=", actNode)


actNodeWithID=funcs.activityNodewithID(csvLog_ED,ED_Activity,ED_ActivitySynonym,ED_Activity_Properties_ID)
print("actNodeWithID=", actNodeWithID)

print("")
print("************************** StepC7: For Log-Event ****************************************************************************")



print("")
print("************************** StepC8: For Event-Entity ****************************************************************************")



print("")
print("************************** StepC9: For Event-Entity ****************************************************************************")

eventAct_Rel=funcs.eventAct_Rel(actNode,ED_Activity,ED_ActivitySynonym)
print("eventAct_Rel=", eventAct_Rel)

print("")
print("************************** StepC10: For Event-Domain ****************************************************************************")




print("")
print("************************** Run Queries ****************************************************************************")



driver=driver
dataSet=ED_dataSet
Perf_file_path = Perf_file_path




print(" ")
print("---------------------------------------- Pre Step C1,2,3 -----------------------------------------------------------------------------------")

EntityLists=EntityLists

############PART C ##########################################
relationTypesC = ["HAS", "CORR", "OBSERVED", "MONITORED"]
nodeTypesC = ["Log", "Event", "Activity", "ActivityPropery"]
nodeTypesC.extend(EntityLists)

############PART D ##########################################
nodeTypesD = []
relationTypesD = ["ATTRIBUTES"]


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
relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED","TYPE_OF"]
relTypePartially = ["CORR", "Scenario", "2"]

############PART V ##########################################
relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

nodeTypes=nodeTypesC+nodeTypesD+nodeTypesE+nodeTypesF+nodeTypesG+nodeTypesH + nodeTypesI
relationTypes=relationTypesC+relationTypesD+relationTypesE+relationTypesG+relationTypesI+relationTypesDK+relationTypesV+relationTypesDK2

print(nodeTypes)
print(relationTypes)

print(" ")
print("---------------------------------------- Step C1 -----------------------------------------------------------------------------------")




step_ClearDB=True
if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    stepName='StepC1 - Clearing DB...'
    print('                      ')
    print(stepName)



    start = time.time()

    with driver.session() as session:
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deleteAllRelations)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.deleteAllNodes)
        session.execute_write(funcs.deleteAllNodesandRel)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)




print(" ")
print("---------------------------------------- Step C2 -----------------------------------------------------------------------------------")

step_ClearConstraints=True
if step_ClearConstraints:
    stepName='StepC2 - Droping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()


    with driver.session() as session:
        session.execute_write(funcs.clearConstraint)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)


print(" ")
print("---------------------------------------- Step C3 -----------------------------------------------------------------------------------")

step_createConstraint=True
if step_createConstraint:
    stepName='StepC3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()


    with driver.session() as session:
        session.execute_write(funcs.createConstraint,nodeTypesC)


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)

print(" ")
print("---------------------------------------- Step C4 -----------------------------------------------------------------------------------")


step_createLog=True
if step_createLog:
    stepName='StepC4 - Creating Log Node...'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.createLogNode, dataSet)





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)

print(" ")
print("---------------------------------------- Step C5 New -----------------------------------------------------------------------------------")


actList=actList
actProIdList=actProIdList
actSynList=actSynList
actValdList=actValdList
entityIdList=entityIdList
entityOriginList=entityOriginList
eventList=eventList
timeList=timeList


step_LoadEventsFromCSV=True
if step_LoadEventsFromCSV:
    stepName='StepC5 - Creating Event Nodes ...'
    print('                      ')
    print(stepName)
    start = time.time()



    for item1, item2, item3, item4, item5, item6, item7, item8 in zip(actList, actProIdList, actSynList, actValdList, entityIdList, entityOriginList, eventList, timeList):
        with driver.session() as session:
            session.execute_write(funcs.CreateEventNodeNew, item1, item2, item3, item4, item5, item6, item7, item8)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)

print(" ")
print("---------------------------------------- Step C6 -----------------------------------------------------------------------------------")
model_entities=model_entities

step_createEntities=True
if step_createEntities:
    stepName='StepC6 - Creating Entities Node...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")



    for entity in model_entities:
        with driver.session() as session:
            session.execute_write(funcs.createEntitiesNode, entity[0], entity[1], entity[2], entity[3])
            print(f'\n     *{entity[3] + str(entity[0])} entity nodes done')


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path,start,end,stepName)


print(" ")
print("---------------------------------------- Step C7 -----------------------------------------------------------------------------------")


actNode=actNode

step_createActivityClasses=True
if step_createActivityClasses:
    stepName='StepC7 - Creating activities Nodes  ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("actNode=",actNode)
    print("")
    for item in actNode:
        with driver.session() as session:
            session.execute_write(funcs.createActivityNode, item[0], item[1])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step C8 -----------------------------------------------------------------------------------")


actNodeWithID=actNodeWithID

step_createActivityPropertiesClasses=True
if step_createActivityPropertiesClasses:
    stepName='StepC7 - Creating activities properties Nodes  ...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("actNodeWithID=",actNodeWithID)
    print("")
    for item in actNodeWithID:
        with driver.session() as session:
            session.execute_write(funcs.createActivityPropertiesNode, item[0], item[1],item[2], item[3])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step C9 -----------------------------------------------------------------------------------")


step_link_log_evnts=True
if step_link_log_evnts:
    stepName='StepC9 - Linking log to events...'
    print('                      ')
    print(stepName)
    start = time.time()


    with driver.session() as session:
        session.execute_write(funcs.link_log_events, dataSet)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step C10 -----------------------------------------------------------------------------------")


model_entities=model_entities

step_correlate_Events_to_Entities=True
if step_correlate_Events_to_Entities:
    stepName='StepC10 - Linking Events to Entities......'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")

    for entity in model_entities:  # per entity

        # if entity[0] in include_entities:
        with driver.session() as session:
            session.execute_write(funcs.link_events_Entities, entity[0], entity[1], entity[2],entity[3])
            print(f'\n     *{entity[3]+ str(entity[0])} E_EN relationships done')


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step C11 -----------------------------------------------------------------------------------")


eventAct_Rel=eventAct_Rel


step_linkingActivityClassToEvent=True
if step_linkingActivityClassToEvent:
    stepName='StepC11 - Linking Events to activities ...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("eventAct_Rel=", eventAct_Rel)
    print("...")
    print("")

    for item in eventAct_Rel:
        with driver.session() as session:
            session.execute_write(funcs.link_events_Activity, item[0], item[1], item[2], item[3])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step C12 -----------------------------------------------------------------------------------")




step_linkingActivityPropertyClassToEvent=True
if step_linkingActivityPropertyClassToEvent:
    stepName='StepC11 - Linking Events to activities ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("...")
    print("")


    with driver.session() as session:
        session.execute_write(funcs.link_events_ActivityProperty)





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


driver.close()










