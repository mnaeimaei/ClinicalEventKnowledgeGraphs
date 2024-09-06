import os
import time

import Script07_SctNode_funcs as funcs
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


SCT_dataSet = "SCTIDNodes"
print("SCT_dataSet=", SCT_dataSet)


SCT_FileName= "H_SCT_Node"
print("SCT_FileName=", SCT_FileName)

SCT_FileName_com = dataPath + "/" + SCT_FileName + Data_Extension
OCT_Input_OCT_Node_FileName = os.path.realpath(SCT_FileName_com)
print("OCT_Input_OCT_Node_FileName=", OCT_Input_OCT_Node_FileName)

OCT_Neo4JImport_OCT_Node_FileName = SCT_FileName + "_Neo4j" + Data_Extension
print("OCT_Neo4JImport_OCT_Node_FileName=", OCT_Neo4JImport_OCT_Node_FileName)




OCT_OCT_Node_conceptId ="SCT_ID"
OCT_OCT_Node_ConceptCode="SCT_Code"
OCT_OCT_Node_termA="SCT_DescriptionA_Type2"
OCT_OCT_Node_termB="SCT_DescriptionB"
OCT_OCT_Node_semanticTags ="SCT_Semantic_Tags"
OCT_OCT_Node_ConceptType="SCT_Type"
OCT_OCT_Node_Levels="SCT_level"
print("OCT_OCT_Node_conceptId=", OCT_OCT_Node_conceptId)
print("OCT_OCT_Node_ConceptCode=", OCT_OCT_Node_ConceptCode)
print("OCT_OCT_Node_termA=", OCT_OCT_Node_termA)
print("OCT_OCT_Node_termB=", OCT_OCT_Node_termB)
print("OCT_OCT_Node_semanticTags=", OCT_OCT_Node_semanticTags)
print("OCT_OCT_Node_ConceptType=", OCT_OCT_Node_ConceptType)
print("OCT_OCT_Node_Levels=", OCT_OCT_Node_Levels)






print("")
print("************************** DF ****************************************************************************")


csv_OCT_Node = funcs.ImportCSV(OCT_Input_OCT_Node_FileName)
print("csv_OCT_Node=\n",csv_OCT_Node)


logSamples21 = funcs.Create_CSV_in_Neo4J_import61(csv_OCT_Node)
print("logSamples21=", logSamples21)

header_OCT_Node, csvLog_header_OCT_Node = funcs.header_csv(logSamples21)
print("header_OCT_Node=", header_OCT_Node)
print("")
print("csvLog_header_OCT_Node=\n", csvLog_header_OCT_Node)
print("")



print("************************** input from OCT_Node ****************************************************************************")

octNodeList=funcs.CreateListNode(csvLog_header_OCT_Node,header_OCT_Node)
#print("octNodeList =", octNodeList)


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




driver = driver
OCTdataSet=SCT_dataSet
Perf_file_path = Perf_file_path


print(" ")
print("---------------------------------------- Step H1 -----------------------------------------------------------------------------------")

############PART I ##########################################
nodeTypesI = ["Concept"]
relationTypesI = ["ANCESTOR_OF"]

############PART DK2 ##########################################

relationTypesDK2 = [f'''INCLUDED {{Type:"last"}}''']

############PART DK ##########################################
relationTypesDK = ["ASSOCIATED", "LINKED_TO", "CONNECTED_TO", "MAPPED_TO", "TIED", "TYPE_OF"]
relTypePartially = ["CORR", "Scenario", "2"]

############PART V ##########################################
relationTypesV = ["REL", "DF", "DF_C", "DF_E"]

nodeTypes = nodeTypesI
relationTypes = relationTypesI + relationTypesDK + relationTypesV + relationTypesDK2

step_Clear_OCT_DB=1
if step_Clear_OCT_DB==1:
    stepName='StepH1 - Clearing DB ....'
    print('                      ')
    print(stepName)
    start = time.time()





    with driver.session() as session:
        session.execute_write(funcs.deleteRelation, relationTypes)
        session.execute_write(funcs.DeleteNodes, nodeTypes)
        session.execute_write(funcs.DeleteAllNodesRels, nodeTypes)
        session.execute_write(funcs.deletePartiallyRel, relTypePartially[0], relTypePartially[1], relTypePartially[2])



    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H2 -----------------------------------------------------------------------------------")


step_Clear_OCT_Constraints=1
if step_Clear_OCT_Constraints==1:
    stepName='StepD2 - Dropping Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()



    with driver.session() as session:
        session.execute_write(funcs.clearConstraint, None, driver, nodeTypes)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)

print(" ")
print("---------------------------------------- Step H3 -----------------------------------------------------------------------------------")



step_createConstraint=1
if step_createConstraint==1:
    stepName='StepD3 - Creating Constraint...'
    print('                      ')
    print(stepName)
    start = time.time()




    with driver.session() as session:
        session.execute_write(funcs.createConstraint, nodeTypesI)




    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)






print(" ")
print("---------------------------------------- Step H4 New -----------------------------------------------------------------------------------")




step_load_OCPS_Concepts=1
if step_load_OCPS_Concepts==1 :
    stepName='StepH4 - Creating OCPS Concepts Nodes ....'
    print('                      ')
    print(stepName)
    octNodeList = octNodeList
    #print(len(octNodeList))
    start = time.time()

    for item in octNodeList:
        with driver.session() as session:
            session.execute_write(funcs.loadOCPSConceptsNew, item[0], item[1], item[2], item[3], item[4], item[5],
                                  item[6], item[7])





    end = time.time()
    funcs.add_row_to_csv(Perf_file_path, start, end, stepName)


print("-------------------------------------------------------------------------------------------------------------------------")





driver.close()


