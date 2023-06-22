import pandas as pd
import time, csv
from neo4j import GraphDatabase
import os
import Func1_EventLog as step1func

#EventLogFile
dataSet = 'MIMIC'
inputPath = './'
eventLogInput = './Test_Input/'
ED_FileName = '1EventLog'
ED_Extension = '.csv'
inputFileName = ED_FileName + ED_Extension
neo4jImportDirectoryFileName = ED_FileName + '_Neo4j' + ED_Extension
outputPerfFileName = ED_FileName + '_Performance' + ED_Extension
eventIdTitle = "Event"
activityTitle = "Activity"
domainColTitle="Domain"
Timestamp = "Timestamp"
EnNum = 2
Entity1Origin = "Entity1_Origin"
Entity2Origin = "Entity2_Origin"
Entity1ID = "Entity1_ID"
Entity2ID = "Entity2_ID"
Output_Graph_File_Name = 'DFG'
fileName = neo4jImportDirectoryFileName
perfFileName = outputPerfFileName
#import
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

#Neo4JImportDirectory
path_to_neo4j_import_directory="/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"
outdirPerf = './Test_Perfomances_Files'

#Domain
dicDomain= {'Domain': "nan"}
DomainValue = []
model_domain_Temp= []


step_ClearDB = True                # Step1  entire graph shall be cleared before starting a new import
step_ClearConstraints = True     # Step2
step_LoadEventsFromCSV = True      # Step3  import all (new) events from CSV file
step_createConstraint = True       # Step4
step_createLog = True              # Step6  create log nodes and relate events to log node
step_link_log_evnts = True
step_createEntities = True         # Step7  create entities from identifiers in the data as specified in this script
step_createReifiedEntities=True
step_correlate_Events_to_Entities = True
step_entities_with_diff_ID_relationships= True
step_RelatingReifiedEntitiesAndEntities = True
step_correlate_ReifiedEntities_to_Event = True
step_createDF = True               # Step10 compute directly-follows relation for all entities in the data
step_deleteExtra_Reified_DF = True       # Step11 remove directly-follows relations for derived entities that run in parallel with DF-relations for base entities



############################################################################################################

model_entities_derived =  ['P_1_2', 'P_2_1', 'M_11_12', 'M_11_21', 'M_11_22', 'M_12_11', 'M_12_21', 'M_12_22', 'M_21_11', 'M_21_12', 'M_21_22', 'M_22_11', 'M_22_12', 'M_22_21']
model_entities_derived_Temp =  [['P_1_2', 'P_2_1'], ['M_11_12', 'M_11_21', 'M_11_22', 'M_12_11', 'M_12_21', 'M_12_22', 'M_21_11', 'M_21_12', 'M_21_22', 'M_22_11', 'M_22_12', 'M_22_21']]
model_domain_Temp= []
model_entities = [['1', 'Entity1_ID', 'WHERE e.Entity1_Origin ="P"', 'P'], ['2', 'Entity1_ID', 'WHERE e.Entity1_Origin ="P"', 'P'], ['12', 'Entity2_ID', 'WHERE e.Entity2_Origin ="M"', 'M'], ['21', 'Entity2_ID', 'WHERE e.Entity2_Origin ="M"', 'M'], ['11', 'Entity2_ID', 'WHERE e.Entity2_Origin ="M"', 'M'], ['22', 'Entity2_ID', 'WHERE e.Entity2_Origin ="M"', 'M']]
model_relations =  [['P_1_2', '1', '2', 'Entity1_ID', 'P'], ['P_2_1', '2', '1', 'Entity1_ID', 'P'], ['M_11_12', '11', '12', 'Entity2_ID', 'M'], ['M_11_21', '11', '21', 'Entity2_ID', 'M'], ['M_11_22', '11', '22', 'Entity2_ID', 'M'], ['M_12_11', '12', '11', 'Entity2_ID', 'M'], ['M_12_21', '12', '21', 'Entity2_ID', 'M'], ['M_12_22', '12', '22', 'Entity2_ID', 'M'], ['M_21_11', '21', '11', 'Entity2_ID', 'M'], ['M_21_12', '21', '12', 'Entity2_ID', 'M'], ['M_21_22', '21', '22', 'Entity2_ID', 'M'], ['M_22_11', '22', '11', 'Entity2_ID', 'M'], ['M_22_12', '22', '12', 'Entity2_ID', 'M'], ['M_22_21', '22', '21', 'Entity2_ID', 'M']]
include_entities = ['P', 'M', 'P_1_2', 'P_2_1', 'M_11_12', 'M_11_21', 'M_11_22', 'M_12_11', 'M_12_21', 'M_12_22', 'M_21_11', 'M_21_12', 'M_21_22', 'M_22_11', 'M_22_12', 'M_22_21']
EntityIDColumnList = ['Entity1_ID', 'Entity2_ID']
Final_AG= [['P', 'M', 'Entity2_Origin', 'Entity2_ID'], ['M', 'P', 'Entity1_Origin', 'Entity1_ID']]
Final_AG_New= [['P', 'M', 'Entity2_Origin', 'Entity2_ID', ['12', '21', '11', '22']], ['M', 'P', 'Entity1_Origin', 'Entity1_ID', ['1', '2']]]
Final_AG_All= [['P', 'P_1_2', 'P_2_1'], ['M', 'M_11_12', 'M_11_21', 'M_11_22', 'M_12_11', 'M_12_21', 'M_12_22', 'M_21_11', 'M_21_12', 'M_21_22', 'M_22_11', 'M_22_12', 'M_22_21']]


############################################################################################################
############# DEFAULT METHODS AND QUERIES ############
######################################################

df=step1func.ImportCSV(eventLogInput, inputFileName)
dicEntID= {'Entity1ID': 'Entity1_ID', 'Entity2ID': 'Entity2_ID'}
#print(df.to_string())
df2=step1func.removeDecimalInIDs(df, dicEntID, EnNum)

df3, activityTitle, Timestamp = step1func.ImportCSVRename(df2, activityTitle, Timestamp)


step1func.CreateM23(df3, path_to_neo4j_import_directory, neo4jImportDirectoryFileName, Entity1Origin,  eventIdTitle, EntityIDColumnList)


header, csvLog = step1func.LoadLog(path_to_neo4j_import_directory + fileName)

############PART C ##########################################
relationTypesC = [":HAS", ":INTER", ":CORR", ":REL", ":DF", ":OBSERVED", ":DF_C"]
nodeTypesC = [":Log", ":Event", ":Domain", ":Entity", ":Class", ":Domain"]

############PART E ##########################################
nodeTypesE = [":Act", ":Form"]
relationTypesE = [":INSIDE"]

############PART F ##########################################
nodeTypesF = [":Patient", ":Admission", ":Disorder"]
relationTypesF = [":poses", ":owns"]

############PART G ##########################################
nodeTypesG = [":ICD"]

############PART H ##########################################
nodeTypesH = [":Concept"]
relationTypesH = [":ANCESTOR_OF"]

############PART DK ##########################################
relationTypesDK = [":Activity_Class", ":Patient_Patient", ":LINKED_TO", ":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]



nodeTypes=nodeTypesC+nodeTypesE+nodeTypesF+nodeTypesG+nodeTypesH
relationTypes=relationTypesC+relationTypesE+relationTypesF+relationTypesH+relationTypesDK







###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################


print(" ")
print("---------------------------------------- Step 1 -----------------------------------------------------------------------------------")



if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    print('                      ')
    print('Step1 - Clearing DB...')

    with driver.session() as session:
        session.execute_write(step1func.deleteRelation, relationTypes)
        session.execute_write(step1func.deleteAllRelations)
        session.execute_write(step1func.DeleteNodes, nodeTypes)
        session.execute_write(step1func.deleteAllNodes)
        session.execute_write(step1func.deleteAllNodesandRel)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row = {'name': dataSet + '_clearDB', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 2 -----------------------------------------------------------------------------------")


if step_ClearConstraints:
    print('                      ')
    print('Step2 - Droping Constraint...')

    with driver.session() as session:
        session.execute_write(step1func.clearConstraint, None, driver)

    end = time.time()
    row = {'name': dataSet + '_clearConstraint', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint clearing done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 3 -----------------------------------------------------------------------------------")


if step_createConstraint:
    print('                      ')
    print('Step4 - Creating Constraint...')

    with driver.session() as session:
        session.execute_write(step1func.createConstraint)

    end = time.time()
    row = {'name': dataSet + '_createConstraint', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint creating done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 4 -----------------------------------------------------------------------------------")



if step_LoadEventsFromCSV:
    print('                      ')
    print('Step3 - Creating Event Nodes from CSV...')
    # convert each record in the CSV table into an Event node
    qCreateEvents, testingQ = step1func.CreateEventQuery(header, fileName,EntityIDColumnList, dataSet )  # generate query to create all events with all log columns as properties
    print(qCreateEvents)
    step1func.runQuery(driver, qCreateEvents)
    print(testingQ)

    end = time.time()
    row = {'name': dataSet + '_event_import', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Event nodes done: took ' + str(end - last) + ' seconds')
    last = end







print(" ")
print("---------------------------------------- Step 6 -----------------------------------------------------------------------------------")



if step_createLog:
    print('                      ')
    print('Step6 - Creating Log...')
    with driver.session() as session:
        session.execute_write(step1func.createLog, dataSet)

    end = time.time()
    row = {'name': dataSet + '_create_log', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Log relationships done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 7 -----------------------------------------------------------------------------------")



if step_link_log_evnts:
    print('                      ')
    print('Step7 - Liking events to Log...')
    with driver.session() as session:
        session.execute_write(step1func.link_log_evnts, dataSet)

    end = time.time()
    row = {'name': dataSet + '_Link_log', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print(':HAS relationships done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 8 -----------------------------------------------------------------------------------")



step_createDomains=True
if step_createDomains:
    print('                      ')
    print('Step8 - Creating Domains...')
    print("")
    print("Inputs:")
    print("model_domain_Temp=",model_domain_Temp)
    print("")

    for domain in model_domain_Temp:

        with driver.session() as session:
            session.execute_write(step1func.createDomains, domain[0], domain[1])
            print(f'\n     * entity nodes done')

        end = time.time()
        row = {'name': dataSet + '_create_entity', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity ' +  ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 9 -----------------------------------------------------------------------------------")



step_createEntities=True
if step_createEntities:
    print('                      ')
    print('Step9 - Creating Entities...')
    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")

    for entity in model_entities:

        with driver.session() as session:
            session.execute_write(step1func.createEntities, entity[0], entity[1], entity[2], entity[3])
            print(f'\n     *{entity[3]+ str(entity[0])} entity nodes done')

        end = time.time()
        row = {'name': dataSet + '_create_entity', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity ' + entity[3]+ str(entity[0]) + ' done: took ' + str(end - last) + ' seconds')
        last = end


print(" ")
print("---------------------------------------- Step 10 -----------------------------------------------------------------------------------")



step_createReifiedEntities=True
if step_createReifiedEntities:
    print('                      ')
    print('Step10 - Creating Reified Entity Nodes')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(step1func.createReifiedEntities, relation[0], relation[1], relation[2], relation[4])
            print(f'\n     *{relation[0]} relationships reified')

        end = time.time()
        row = {'name': dataSet + '_create_reified_en', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity  ' + relation[0] + ' done: took ' + str(end - last) + ' seconds')
        last = end




print(" ")
print("---------------------------------------- Step 11 -----------------------------------------------------------------------------------")



step_correlate_Domain_to_Events=True
if step_correlate_Domain_to_Events:
    print('                      ')
    print('Step11 - Correlating Domain Nodes to Events Node...')
    print("")
    print("Inputs:")
    print("DomainValue=",DomainValue)
    print("")

    for Value in DomainValue:  # per entity

        # if entity[0] in include_entities:
        with driver.session() as session:
            session.execute_write(step1func.correlate_Domain_to_Events, Value,domainColTitle )
            print(f'\n     *  E_EN relationships done')

        end = time.time()
        row = {'name': dataSet + '_corrolate_entity', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity ' + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 12 -----------------------------------------------------------------------------------")



step_correlate_Events_to_Entities=True
if step_correlate_Events_to_Entities:
    print('                      ')
    print('Step12 - Correlating Entities Nodes to Events Node...')
    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")

    for entity in model_entities:  # per entity

        # if entity[0] in include_entities:
        with driver.session() as session:
            session.execute_write(step1func.correlate_Events_to_Entities, entity[0], entity[1], entity[2],entity[3] )
            print(f'\n     *{entity[3]+ str(entity[0])} E_EN relationships done')

        end = time.time()
        row = {'name': dataSet + '_corrolate_entity', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity ' + entity[3]+ str(entity[0]) + ' done: took ' + str(end - last) + ' seconds')
        last = end





print(" ")
print("---------------------------------------- Step 13 -----------------------------------------------------------------------------------")




step_entities_with_diff_ID_relationships=True

if step_entities_with_diff_ID_relationships:
    print('                      ')
    print('Step13 - Creating Relationship between Entities with different ID...')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(step1func.entities_with_diff_ID_relationships, relation[0], relation[1], relation[2], relation[3], relation[4])
            print(f'\n     *{relation[0]} relationships created')

        end = time.time()
        row = {'name': dataSet + '_create_entity_relationships', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity  ' + relation[0] + ' done: took ' + str(end - last) + ' seconds')
        last = end


print(" ")
print("---------------------------------------- Step 14 -----------------------------------------------------------------------------------")



step_RelatingReifiedEntitiesAndEntities=True
if step_RelatingReifiedEntitiesAndEntities:
    print('                      ')
    print('Step14 - Relating Reified Entity Nodes to Non-Reified Entity Node')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(step1func.RelatingReifiedEntitiesAndEntities, relation[0])
            print(f'\n     *{relation[0]} relationships reified')

        end = time.time()
        row = {'name': dataSet + '_create_reified_en', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity  ' + relation[0] + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 15 -----------------------------------------------------------------------------------")


step_correlate_ReifiedEntities_to_Event=True
if step_correlate_ReifiedEntities_to_Event:
    print('                      ')
    print('Step15 - Correlate Reified Entities Nodes to Events Node')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(step1func.correlate_ReifiedEntities_to_Event, relation[0], relation[1], relation[2], relation[3], relation[4])
            print(f'\n     *{relation[0]} E_EN relationships created')

        end = time.time()
        row = {'name': dataSet + 'correlate_reified_en', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity  ' + relation[0] + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 16 -----------------------------------------------------------------------------------")



step_createDF=True
if step_createDF:
    print('                      ')
    print('Step16 - Creating DF Relationship ...')
    mylist = include_entities.copy()
    print("")
    print("Inputs:")
    print("mylist=",mylist)
    print("")
    for entity in mylist:  # per entity
        with driver.session() as session:
            session.execute_write(step1func.createDF, entity)

        end = time.time()
        row = {'name': dataSet + '_create_df', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     DF for Entity ' + entity + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 17 -----------------------------------------------------------------------------------")


step_delete_Polluted_Reified_DF=True
if step_delete_Polluted_Reified_DF:
    print('                      ')
    print('Step17 - Deleting Pulled Reified Relationship  ...')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        derived_entity = relation[0]
        parent_entity = relation[4]
        child_entity = relation[4]

        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(step1func.deletePuluted_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4])

        end = time.time()
        row = {'name': dataSet + '_delete_Puluted_Reified_df', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('Deleting Pulled Reified Relationship ' + derived_entity + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 18 -----------------------------------------------------------------------------------")


step_deleteExtra_Reified_DF=True
if step_deleteExtra_Reified_DF:
    print('                      ')
    print('Step18 - Deleting Extra Reified Relationship  ...')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        derived_entity = relation[0]
        ID_A = relation[1]
        ID_B = relation[2]
        ID_Column = relation[3]


        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(step1func.deleteExtra_Reified_DF, derived_entity, ID_A, ID_B, ID_Column)

        end = time.time()
        row = {'name': dataSet + '_delete_Extra_Reified_df', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('Deleting Extra Reified Relationship ' + derived_entity + ' done: took ' + str(end - last) + ' seconds')
        last = end


print(" ")
print("---------------------------------------- Step 19 -----------------------------------------------------------------------------------")



step_deletePolluted_CoRR_Reified_Events=True
if step_deletePolluted_CoRR_Reified_Events:
    print('                      ')
    print('Step19 - Deleting Reified CORR Relationship  ...')
    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        derived_entity = relation[0]
        ID_A = relation[1]
        ID_B = relation[2]
        ID_Column = relation[3]
        Value = relation[4]


        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(step1func.deletePolluted_CoRR_Reified_Events, derived_entity, ID_A, ID_B, ID_Column, Value)

        end = time.time()
        row = {'name': dataSet + '_delete_Polluted_Reified_CORR', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('Deleting Reified CORR Relationship  ' + derived_entity + ' done: took ' + str(end - last) + ' seconds')
        last = end




print(" ")
print("---------------------------------------- Step 20 -----------------------------------------------------------------------------------")


step_createActivityClasses=True
if step_createActivityClasses:
    print('                      ')
    print('Step20 - Creating Classifier for activities ...')
    print("")
    print("Inputs:")
    print("DomainValue=",DomainValue)
    print("")


    with driver.session() as session:
        session.execute_write(step1func.createActivityClasses, DomainValue, domainColTitle)



    end = time.time()
    row = {'name': dataSet + '_create_classes', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Event classes done: took ' + str(end - last) + ' seconds')
    last = end


print(" ")
print("---------------------------------------- Step 21 -----------------------------------------------------------------------------------")


step_linkingActivityClassToEvent=True
if step_linkingActivityClassToEvent:
    print('                      ')
    print('Step21 - Linking activity classifiers to Events ...')
    print("")
    print("Inputs:")
    print("...")
    print("")

    with driver.session() as session:
        session.execute_write(step1func.linkingActivityClassToEvent)




    end = time.time()
    row = {'name': dataSet + '_create_classes', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Event classes done: took ' + str(end - last) + ' seconds')
    last = end

##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################
##############################################################################




print(" ")
print("---------------------------------------- Step 22 -----------------------------------------------------------------------------------")



step_aggregateDF_Absolute=True
if step_aggregateDF_Absolute:
    print('                      ')
    print('Step22 - Aggregating Absoulte ...')
    print("")
    print("Inputs:")
    print("Final_AG_All=",Final_AG_All)
    print("")

    for entity in Final_AG_All:  # per entity
        Ent=entity[0]
        for i in range(len(entity)):
            EntIDbased=entity[i]
            with driver.session() as session:
                session.execute_write(step1func.aggregateDF_Absolute, Ent, EntIDbased)





    end = time.time()
    row = {'name': dataSet + '_corrolate_entity', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('     Entity ' +  ' done: took ' + str(end - last) + ' seconds')
    last = end



print(" ")
print("---------------------------------------- Step 23 -----------------------------------------------------------------------------------")


step_aggregateDF_Relative=True
if step_aggregateDF_Relative:
    print('                      ')
    print('Ste23 - Aggregating Relatievly...')
    print("")
    print("Inputs:")
    print("Final_AG_New=",Final_AG_New)
    print("")

    for entity in Final_AG_New:  # per entity
        En1=entity[0]
        En2=entity[1]
        En2_OriginColumn=entity[2]
        En2_IDColumn=entity[3]
        ID_Value_List=entity[4]

        for i in range(len(ID_Value_List)):
            eID=ID_Value_List[i]
            print(eID)
            with driver.session() as session:
                session.execute_write(step1func.aggregateDF_Relative, En1, En2, En2_OriginColumn, En2_IDColumn, ID_Value_List[i])






        end = time.time()
        row = {'name': dataSet + '_corrolate_entity', 'start': last, 'end': end, 'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('     Entity ' + entity[3]+ str(entity[0]) + ' done: took ' + str(end - last) + ' seconds')
        last = end



print(" ")
print("---------------------------------------- Step 24 -----------------------------------------------------------------------------------")


step_aggregateDF_All=True
if step_aggregateDF_All:
    print('                      ')
    print('Ste24 - Agregating All...')
    print("")
    print("Inputs:")
    print("Final_AG_All=",Final_AG_All)
    print("")

    for entity in Final_AG_All:  # per entity
        with driver.session() as session:
            session.execute_write(step1func.aggregateDF_All,entity, entity[0])



    end = time.time()
    row = {'name': dataSet + '_corrolate_entity', 'start': last, 'end': end, 'duration': (end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('     Entity ' +  ' done: took ' + str(end - last) + ' seconds')
    last = end



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################



if not os.path.exists(outdirPerf):
    os.mkdir(outdirPerf)


fullname = os.path.join(outdirPerf, perfFileName)
perf.to_csv(fullname)
driver.close()