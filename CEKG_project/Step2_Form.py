import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func2_Form as cl3b

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
ACTdataSet = 'MIMIC'
ACTinputPath = './Test_Input/'

ACT_PoNode_FileName = '2Form'
ACT_Extension = '.csv'
ACT_Input_PoNode_FileName = ACT_PoNode_FileName + ACT_Extension
ACT_Neo4JImport_PoNode_FileName = ACT_PoNode_FileName + '_Neo4j' + ACT_Extension
ACT_Perf_FileName = ACTdataSet + '_Performance' + ACT_Extension
ACT_Activity = "Activity"
ACT_Activity_Synonym = "Activity_Synonym"
ACT_Activity_Origin = "Activity_Origin"


print("************************** From cl2: ****************************************************************************")
Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"

csv_ACT=cl3b.ImportCSV(ACTinputPath, ACT_Input_PoNode_FileName)
cl3b.Create_CSV_in_Neo4J_import(csv_ACT, Neo4JImport, ACT_Neo4JImport_PoNode_FileName)
header_ACT, csvLog_header_ACT = cl3b.LoadLog(Neo4JImport+ACT_Neo4JImport_PoNode_FileName)



print("************************** input from cl1: Potential_Nodes ****************************************************************************")

caseAct= [['aaa', 'a'], ['bbb', 'b'], ['ccc', 'c']]
caseForm= [['O1'], ['O2'], ['O3']]
formAct= [['aaa', 'a', 'O1'], ['bbb', 'b', 'O2'], ['ccc', 'c', 'O3']]




###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

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

    nodeTypes = nodeTypesE + nodeTypesF + nodeTypesG + nodeTypesH
    relationTypes = relationTypesE + relationTypesF + relationTypesH + relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3b.deleteRelation, relationTypes)
        session.execute_write(cl3b.DeleteNodes, nodeTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':ACTdataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end

print("-------------------------------------------------------------------------------------------------------------------------")


step_Clear_OCT_Constraints=True
if step_Clear_OCT_Constraints:
    print('                      ')
    print('Step2 - Dropping Constraint...')

    with driver.session() as session:
        session.execute_write(cl3b.clearConstraint, None, driver)



    end = time.time()
    row={'name':ACTdataSet+'_clearConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint clearing done: took '+str(end - last)+' seconds')
    last = end



print("-------------------------------------------------------------------------------------------------------------------------")


step_createConstraint=True
if step_createConstraint:
    print('                      ')
    print('Step3 - Creating Constraint...')

    with driver.session() as session:
        session.execute_write(cl3b.createConstraint)


    end = time.time()
    row={'name':ACTdataSet+'_createConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint creating done: took '+str(end - last)+' seconds')
    last = end




print("-------------------------------------------------------------------------------------------------------------------------")


step_Form_Nodes=True
if step_Form_Nodes :
    print('                      ')
    print('Step4 - Creating Form Nodes...')
    # convert each record in the CSV table into an Event node

    for item in caseForm:

        with driver.session() as session:
            session.execute_write(cl3b.Form_Nodes, item[0])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':ACTdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end


print("-------------------------------------------------------------------------------------------------------------------------")




step_Act_Nodes=True
if step_Act_Nodes :
    print('                      ')
    print('Step5 - Creating Act Nodes...')
    # convert each record in the CSV table into an Event node

    for item in caseAct:

        with driver.session() as session:
            session.execute_write(cl3b.Act_Nodes, item[0], item[1])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':ACTdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end

print("-------------------------------------------------------------------------------------------------------------------------")




step_Patients_Admission_Rel=True
if step_Patients_Admission_Rel :
    print('                      ')
    print('Step4 - Creating Patients Admission Rel...')
    # convert each record in the CSV table into an Event node

    for item in formAct:

        with driver.session() as session:
            session.execute_write(cl3b.formAct_Rel, item[0], item[1], item[2])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':ACTdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end




print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': ACTdataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, ACT_Perf_FileName)
perf.to_csv(fullname)
driver.close()



