

import time, csv
from datetime import datetime
from neo4j import GraphDatabase
import os


import Script14_DF_funcs as funcs



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






print("************************** Entry from clO ****************************************************************************")



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







entityList=entityList
print("entityList=", entityList)

entityListIDproperty=entityListIDproperty
print("entityListIDproperty=", entityListIDproperty)

conditionProperty=conditionProperty
print("conditionProperty=", conditionProperty)

conditionPropertyValue=conditionPropertyValue
print("conditionPropertyValue=", conditionPropertyValue)


ED_EnNum=len(entityList)
print("ED_EnNum=", ED_EnNum)





print("************************** Entry from cl6 ****************************************************************************")



EntityOriginIDValue=funcs.Finading_Entities_ID(driver,entityList,entityListIDproperty, conditionProperty, conditionPropertyValue)
print("EntityOriginIDValue=",EntityOriginIDValue)

EntityOrgValue=funcs.convert_to_list_of_lists(entityList)
print("EntityOrgValue=",EntityOrgValue)

dicEnt=funcs.Entities_Alias_values (ED_EnNum, EntityOrgValue)
#print(dicEnt)
locals().update(dicEnt)
print("dicEnt=",dicEnt)

EntityOriginValueTemp=funcs.EntityOriginValue_Temp(EntityOrgValue,  ED_EnNum)
print("EntityOriginValueTemp =", EntityOriginValueTemp)


dicNumEntOrgAbr=funcs.NumberEntityOriginAbr(EntityOriginValueTemp, ED_EnNum)
# print(dicNumEntTypeAbr)
locals().update(dicNumEntOrgAbr)
print("dicNumEntOrgAbr=", dicNumEntOrgAbr)



dicEntAliasAbr=funcs.EntityAliasAbr(dicEnt, dicNumEntOrgAbr, ED_EnNum)
# print(dicNumEntTypeAbr)
locals().update(dicEntAliasAbr)
print("dicEntAliasAbr=", dicEntAliasAbr)

model_entities_Temp=funcs.EntityAndEntityOrg_Creater_Final(EntityOriginIDValue, ED_EnNum, dicEnt,dicEntAliasAbr)
print("model_entities_Temp=",model_entities_Temp)

model_entities=funcs.model_entities(model_entities_Temp,ED_EnNum)
print("model_entities =", model_entities)

EntityIDValueOrdered=funcs.Finading_ID_List(driver,entityList,entityListIDproperty, conditionProperty, conditionPropertyValue)
print("EntityIDValueOrdered=",EntityIDValueOrdered)

EntityIDValueOrdered_pair=funcs.create_adjacent_pairs(EntityIDValueOrdered,ED_EnNum)
print("EntityIDValueOrdered_pair=",EntityIDValueOrdered_pair)

model_entities_derived, model_entities_derived_Temp = funcs.pair_memebr2(EntityIDValueOrdered_pair,ED_EnNum,dicEntAliasAbr)
print("model_entities_derived = " , model_entities_derived)
print("model_entities_derived_Temp = " , model_entities_derived_Temp)


model_relations=funcs.model_relations(model_entities_derived_Temp, EntityOrgValue,  ED_EnNum, dicEnt, dicEntAliasAbr)
print("model_relations = ",model_relations)

print("")
print("************************** Changing Dataset ****************************************************************************")



idColumnAndValue=funcs.idColumnAndValueMaler(entityList, EntityOriginIDValue, ED_EnNum)
print("idColumnAndValue =", idColumnAndValue)



print("")
print("**************************  ****************************************************************************")

EntityIDValueOrdered = funcs.Finading_ID_List(driver, entityList, entityListIDproperty, conditionProperty,
                                                 conditionPropertyValue)
# print("EntityIDValueOrdered=", EntityIDValueOrdered)

EntityIDValueOrdered_pair = funcs.create_adjacent_pairs(EntityIDValueOrdered, ED_EnNum)
# print("EntityIDValueOrdered_pair=", EntityIDValueOrdered_pair)

model_entities_derived, model_entities_derived_Temp = funcs.pair_memebr2(EntityIDValueOrdered_pair, ED_EnNum,
                                                                            dicEntAliasAbr)
# print("model_entities_derived = ", model_entities_derived)
# print("model_entities_derived_Temp = ", model_entities_derived_Temp)
include_entities = funcs.include_entities(EntityOrgValue, model_entities_derived_Temp, ED_EnNum)
# print("include_entities =", include_entities)


include_DF1 = funcs.include_DF1(EntityOrgValue, model_entities_derived_Temp, ED_EnNum)
# print("include_DF1 =", include_DF1)
include_DF2 = funcs.include_DF2(EntityOrgValue, model_entities_derived_Temp, ED_EnNum)
# print("include_DF2 =", include_DF2)
include_DF = include_DF1 + include_DF2
# print("include_DF =", include_DF)



print("")
print("************************** DFG_3: Defualt ****************************************************************************")





Dfg3_existance=False



if Dfg3_existance==False:
    status=11
    Final_AG_All = EntityOrgValue
    print("Final_AG_All=",Final_AG_All)

    Final_AG_All_ID = []
    print("Final_AG_All_ID=", Final_AG_All_ID)


else:

    Type3 = 1
    Type3_non_Relative_selection=False
    Type3_non_Relative_selection_ID_instances=[]

    '''
    Example:


    Type3 = 1 # 1=RelateveAndAbsolute  2=OnlyAbsolute (non Relative)
    if Type3 = 1:
        Type3_non_Relative_selection=False
        Type3_non_Relative_selection_ID_instances=[] 
    if Type3 = 2:
        Type3_non_Relative_selection=False
        Type3_non_Relative_selection_ID_instances=[] 

    if Type3 = 2:
        Type3_non_Relative_selection=True
        Type3_non_Relative_selection_ID_instances=[["1"],["13"],["44054006"]] 
    '''

    print("Type3=", Type3)
    print("Type3_non_Relative_selection=", Type3_non_Relative_selection)
    print("Type3_non_Relative_selection_ID_instances=", Type3_non_Relative_selection_ID_instances)

    if Type3==1:
        status = 22
        Final_AG_All = EntityOrgValue
        print("Final_AG_All=", Final_AG_All)

        Final_AG_All_ID = []
        print("Final_AG_All_ID=", Final_AG_All_ID)


    if Type3==2 and Type3_non_Relative_selection==False:
        status = 33
        Final_AG_All = funcs.entity_basedon_column(ED_EnNum, EntityOrgValue, model_entities_derived_Temp)
        print("Final_AG_All=", Final_AG_All)
        Final_AG_All_ID = Type3_non_Relative_selection_ID_instances
        print("Final_AG_All_ID=", Final_AG_All_ID)

    if Type3==2 and Type3_non_Relative_selection==True:
        status = 44
        Final_AG_All = EntityOrgValue
        print("Final_AG_All=", Final_AG_All)
        Final_AG_All_ID = Type3_non_Relative_selection_ID_instances
        print("Final_AG_All_ID=", Final_AG_All_ID)




print("")
print("************************** DFG5_Relative: Default ****************************************************************************")

Dfg5_existance=False

if Dfg5_existance==False:
    #status=11
    random_entity = funcs.select_two_random_items(entityList)
    # print("random_entity:", random_entity)

    Type5_Rel_1_DF_Show_Random = random_entity[0]
    Type5_Rel_2_DF_Show_Random = random_entity[1]

    print("Type5_Rel_1_DF_Show_Random=", Type5_Rel_1_DF_Show_Random)
    print("Type5_Rel_2_DF_Show_Random=", Type5_Rel_2_DF_Show_Random)

else:
    #status = 12
    Type5_Rel_1_DF_Show_Random = "Patient"  # Q02
    Type5_Rel_2_DF_Show_Random = "Concept"  # Q02

    print("Type5_Rel_1_DF_Show_Random=", Type5_Rel_1_DF_Show_Random)

    print("Type5_Rel_2_DF_Show_Random=", Type5_Rel_2_DF_Show_Random)


Final_AG_New=funcs.Final_AG_forListID(Type5_Rel_1_DF_Show_Random, Type5_Rel_2_DF_Show_Random,idColumnAndValue)
print("Final_AG_New=",Final_AG_New)


print("")
print("************************** DFG_6: ****************************************************************************")

node1=funcs.node1Finder(driver)
print("node1=",node1)

nodeLast=funcs.nodeLastFinder(driver)
print("nodeLast=",nodeLast)

otherNodes=funcs.otherLastFinder(driver)
print("otherNodes=",otherNodes)

maxLength=funcs.maxLengthFinder(driver)
print("maxLength=",maxLength)


nodeOther=funcs.node3Finder(driver,node1, nodeLast,maxLength)
print("nodeOther=",nodeOther)

converted_list=funcs.converted_lister(nodeOther)
print("converted_list=",converted_list)


pathNode=funcs.pathFinder(converted_list)
print("pathNode=",pathNode)

excludedNode=funcs.nodeExcluder(driver)
print("excludedNode=",excludedNode)

nodeReal=funcs.creatingDfFromGraph(driver,pathNode,excludedNode)
print("nodeReal=",nodeReal)



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
print("---------------------------------------- Pre Step 1,2 -----------------------------------------------------------------------------------")



EntityLists=entityList
print(EntityLists)

############PART D ##########################################
relationTypesD = [":REL", ":DF", ":DF_C", ":DF_E"]

relationTypes=relationTypesD



print(" ")
print("---------------------------------------- Step V1 -----------------------------------------------------------------------------------")



step_ClearDB=True
if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    stepName='StepV1 - Clearing DB...'
    print('                      ')
    print(stepName)
    start = time.time()


    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.deletePartRel)
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deletePartNode,EntityLists[i])
        for i in range(len(EntityLists)):
            session.execute_write(funcs.deleteProperty,EntityLists[i])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)







print(" ")
print("---------------------------------------- Step V2 -----------------------------------------------------------------------------------")


model_entities=model_entities

step_createReifiedEntities=True
if step_createReifiedEntities:
    stepName='StepV2 - Modifying Entities Properties'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_entities=",model_entities)
    print("")



    for relation in model_entities:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.modifyEntities, relation[0], relation[1], relation[2])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V3 -----------------------------------------------------------------------------------")

model_relations=model_relations

step_createReifiedEntities=True
if step_createReifiedEntities:
    stepName='StepV3 - Creating Reified Entity Nodes'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.createReifiedEntities, relation[0], relation[1], relation[2], relation[3], relation[4])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V4 -----------------------------------------------------------------------------------")



step_entities_with_diff_ID_relationships=True
if step_entities_with_diff_ID_relationships:
    stepName='StepV4 - Creating Relationship between Entities with different ID...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.entities_with_diff_ID_relationships, relation[0], relation[1], relation[2], relation[3], relation[4])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V5 -----------------------------------------------------------------------------------")


step_RelatingReifiedEntitiesAndEntities=True
if step_RelatingReifiedEntitiesAndEntities:
    stepName='StepV5 - Relating Reified Entity Nodes to Non-Reified Entity Node'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.RelatingReifiedEntitiesAndEntities, relation[0], relation[1], relation[2], relation[3], relation[4])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V6 -----------------------------------------------------------------------------------")


step_correlate_ReifiedEntities_to_Event=True
if step_correlate_ReifiedEntities_to_Event:
    stepName='StepV6 - Correlate Reified Entities Nodes to Events Node'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.correlate_ReifiedEntities_to_Event, relation[0], relation[1], relation[2], relation[3], relation[4])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V7 -----------------------------------------------------------------------------------")

include_DF=include_DF


step_createDF=True
if step_createDF:
    stepName='StepV7 - Creating DF Relationship for Absolute Entities...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")
    for entity in include_DF:  # per entity
        with driver.session() as session:
            session.execute_write(funcs.createDF, entity[0], entity[1])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V8 -----------------------------------------------------------------------------------")


step_delete_Polluted_Reified_DF=True
if step_delete_Polluted_Reified_DF:
    stepName='StepV8 - Deleting Polluted DF  ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.deletePuluted_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4])


    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V9 -----------------------------------------------------------------------------------")


step_delete_Polluted_Wrong_DF=True
if step_delete_Polluted_Wrong_DF:
    stepName='StepV9 - Deleting Wrong DF  ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.deleteWrong_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V10 -----------------------------------------------------------------------------------")


step_deleteExtra_Reified_DF=True
if step_deleteExtra_Reified_DF:
    stepName='StepV10 - Deleting Reverse Reified Relationship  ...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(funcs.deleteExtra_Reified_DF, relation[0], relation[1], relation[2], relation[3], relation[4])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)





print(" ")
print("---------------------------------------- Step V11 -----------------------------------------------------------------------------------")



step_deletePolluted_CoRR_Reified_Events=True
if step_deletePolluted_CoRR_Reified_Events:
    stepName='StepV11 - Deleting correlation between event and Reified entities  ...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.execute_write(funcs.deletePolluted_CoRR_Reified_Events, relation[0], relation[1], relation[2], relation[3], relation[4])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V12 -----------------------------------------------------------------------------------")



step_deletePolluted_CoRR_Reified_Events_part2=True
if step_deletePolluted_CoRR_Reified_Events_part2:
    stepName='StepV12 - Restoring correlation between event and Reified entities which wrongly deleted in step 12  ...'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.execute_write(funcs.deletePolluted_CoRR_Reified_Events_2, relation[0], relation[1], relation[2], relation[3], relation[4])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print(" ")
print("---------------------------------------- Step V13 -----------------------------------------------------------------------------------")



step_wrong_rel_and_wrong_refied_entity=True
if step_wrong_rel_and_wrong_refied_entity:
    stepName='StepV13 - Deleting _wrong_rel_and_wrong_refied_entity  ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("model_relations=",model_relations)
    print("")
    for relation in model_relations:
        with driver.session() as session:
            session.execute_write(funcs.wrong_reified, relation[0], relation[1], relation[2], relation[3], relation[4])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step V14 -----------------------------------------------------------------------------------")

include_DF=include_DF


step_aggregateDF_Absolute=True
if step_aggregateDF_Absolute:
    stepName='StepV14 - Aggregating Absolute ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")

    for entity in include_DF:
        with driver.session() as session:
            session.execute_write(funcs.aggregateDF_Absolute, entity[0], entity[1])




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V15 -----------------------------------------------------------------------------------")

Final_AG_New=Final_AG_New


step_aggregateDF_Relative=True
if step_aggregateDF_Relative:
    stepName='StepV15 - Aggregating Relatively...'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("Final_AG_New=",Final_AG_New)
    print("")

    En1 = Final_AG_New[0]
    En2 = Final_AG_New[1]
    ID_Value_List = Final_AG_New[2]
    print(En1)
    print(En2)
    print(ID_Value_List)
    for i in range(len(ID_Value_List)):
        eID = ID_Value_List[i]
        print(eID)
        with driver.session() as session:
            session.execute_write(funcs.aggregateDF_Relative, En1, En2, eID)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V16 -----------------------------------------------------------------------------------")

status=status
Final_AG_All=Final_AG_All
Final_AG_All_ID=Final_AG_All_ID

step_aggregateDF_All=True
if step_aggregateDF_All:
    stepName='StepV16 - Aggregating All...'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("status=",status)
    print("Final_AG_All=",Final_AG_All)
    print("Final_AG_All_ID=",Final_AG_All_ID)
    print("")

    if status==11:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_All, entity, entity[0])


    if status==22:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_All, entity, entity[0])


    if status==33:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_All_inactiveID, entity, entity[0])



    if status==44:
        for entity, ID in zip(Final_AG_All, Final_AG_All_ID):
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_All_activeID, entity, entity[0], ID)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V17 -----------------------------------------------------------------------------------")


nodeReal=nodeReal

step_relEntity=True
if step_relEntity:
    stepName='StepV17 - Entities Relations... (DFG 6)'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("nodeReal=",nodeReal)
    print("")
    for relation in nodeReal:  # per relation
        print(relation)
        for item in relation:
            print(item)
            item0ID = item[0][0]
            item0 = item[1][0]
            item1 = item[1][-1]
            item1ID1 = item[2][0]
            item1ID2 = item[2][1]
            with driver.session() as session:
                session.execute_write(funcs.relEntity, item0ID, item0, item1, item1ID1,item1ID2)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V18 -----------------------------------------------------------------------------------")


nodeReal=nodeReal

step_relEntityLower=True
if step_relEntityLower:
    stepName='StepV18 - Entities Relations lower... (DFG 6)'
    print('                      ')
    print(stepName)
    start = time.time()


    print("")
    print("Inputs:")
    print("nodeReal=",nodeReal)
    print("")
    for relation in nodeReal:  # per relation
        print(relation)
        for item in relation:
            print(item)
            item1 = item[1][-1]
            item3 = item[3][0]
            item4 = item[4][0]
            item0ID = item[0][0]
            item0 = item[1][0]
            id1=item[2][0]
            id2 = item[2][1]
            with driver.session() as session:
                session.execute_write(funcs.relEntityLower, item1, item3,item4,item0,item0ID, id1, id2)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V19 -----------------------------------------------------------------------------------")



step_DF_Propery=True
if step_DF_Propery:
    stepName='StepV19 - DF for properties... (DFG Property)'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    with driver.session() as session:
        session.execute_write(funcs.DF_Propery)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V20 -----------------------------------------------------------------------------------")


include_DF=include_DF


step_aggregateDF_Absolute_property=True
if step_aggregateDF_Absolute_property:
    stepName='StepV20 - Aggregating Absolute for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()



    print("")
    print("Inputs:")
    print("include_DF=",include_DF)
    print("")

    for entity in include_DF:
        with driver.session() as session:
            session.execute_write(funcs.aggregateDF_AbsoluteProperty, entity[0], entity[1])
    with driver.session() as session:
        session.execute_write(funcs.DF_AbsolutePropery)



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)




print(" ")
print("---------------------------------------- Step V21 -----------------------------------------------------------------------------------")


Final_AG_New=Final_AG_New


step_aggregateDF_Relative_property=True
if step_aggregateDF_Relative_property:
    stepName='StepV20 - Aggregating Relative for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()




    print("")
    print("Inputs:")
    print("Final_AG_New=",Final_AG_New)
    print("")

    En1 = Final_AG_New[0]
    En2 = Final_AG_New[1]
    ID_Value_List = Final_AG_New[2]
    print(En1)
    print(En2)
    print(ID_Value_List)
    for i in range(len(ID_Value_List)):
        eID = ID_Value_List[i]
        print(eID)
        with driver.session() as session:
            session.execute_write(funcs.aggregateDF_RelativeProperty, En1, En2, eID)

    with driver.session() as session:
        session.execute_write(funcs.DF_RelativePropery)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)



print(" ")
print("---------------------------------------- Step V22 -----------------------------------------------------------------------------------")


status=status
Final_AG_All=Final_AG_All
Final_AG_All_ID=Final_AG_All_ID


step_aggregateDF_All_property=True
if step_aggregateDF_All_property:
    stepName='StepV22 - Aggregating All for Property ...'
    print('                      ')
    print(stepName)
    start = time.time()



    if status==11:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_AllProperty, entity, entity[0])


    if status==22:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_AllProperty, entity, entity[0])


    if status==33:
        for entity in Final_AG_All:  # per entity
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_AllProperty_inactiveID, entity, entity[0])


    if status==44:
        for entity, ID in zip(Final_AG_All, Final_AG_All_ID):
            with driver.session() as session:
                session.execute_write(funcs.aggregateDF_AllProperty_activeID, entity, entity[0], ID)

    with driver.session() as session:
        session.execute_write(funcs.DF_AllPropery)





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step Not Used 1 -----------------------------------------------------------------------------------")


driver.close()

