import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func3_Diagnosis as cl3b

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
DIAGdataSet = 'MIMIC'
DIAGinputPath = './Test_Input/'

DIAG_PoNode_FileName = '3Diagnosis'

DIAG_Extension = '.csv'

DIAG_Input_PoNode_FileName = DIAG_PoNode_FileName + DIAG_Extension
DIAG_Neo4JImport_PoNode_FileName = DIAG_PoNode_FileName + '_Neo4j' + DIAG_Extension
DIAG_Perf_FileName = DIAGdataSet + '_Performance' + DIAG_Extension

DIAG_EnpoID = "EnpoID"
DIAG_Entity1_Origin = "Entity1_Origin"
DIAG_Entity1_ID = "Entity1_ID"
DIAG_Entity2_Origin = "Entity2_Origin"
DIAG_Entity2_ID = "Entity2_ID"
DIAG_icd_code_syn = "icd_code_syn"
DIAG_Icd_code_Short = "Icd_code_Short"
DIAG_potentialEntity = "potentialEntity"
DIAG_icd_short = "icd_short"



print("************************** From cl2: ****************************************************************************")

Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"

csv_Diag=cl3b.ImportCSV(DIAGinputPath, DIAG_Input_PoNode_FileName)
cl3b.Create_CSV_in_Neo4J_import(csv_Diag, Neo4JImport, DIAG_Neo4JImport_PoNode_FileName)
header_Diag, csvLog_header_Diag = cl3b.LoadLog(Neo4JImport+DIAG_Neo4JImport_PoNode_FileName)




casePa= [['P', '1'], ['P', '2']]
multi= [['M', '11'], ['M', '22'], ['M', '21'], ['M', '12']]
Diag= [['D01', '1', 'disorder', 'ICD1'], ['D02', '2', 'disorder', 'ICD2'], ['D03', '3', 'disorder', 'ICD3'], ['D05', '5', 'disorder', 'ICD5'], ['D08', '8', 'disorder', 'ICD8'], ['D09', '9', 'disorder', 'ICD9'], ['D10', '10', 'disorder', 'ICD10'], ['D12', '12', 'disorder', 'ICD12'], ['D13', '13', 'disorder', 'ICD13'], ['D16', '16', 'disorder', 'ICD16'], ['D17', '17', 'disorder', 'ICD17'], ['D18', '18', 'disorder', 'ICD18'], ['D22', '22', 'disorder', 'ICD22'], ['D23', '23', 'disorder', 'ICD23'], ['D25', '25', 'disorder', 'ICD25'], ['D27', '27', 'disorder', 'ICD27'], ['D28', '28', 'disorder', 'ICD28'], ['D29', '29', 'disorder', 'ICD29'], ['D30', '30', 'disorder', 'ICD30'], ['D31', '31', 'disorder', 'ICD31'], ['D32', '32', 'disorder', 'ICD32'], ['D34', '34', 'disorder', 'ICD34'], ['D35', '35', 'disorder', 'ICD35'], ['D36', '36', 'disorder', 'ICD36'], ['D40', '40', 'disorder', 'ICD40'], ['D41', '41', 'disorder', 'ICD41'], ['D42', '42', 'disorder', 'ICD42'], ['D43', '43', 'disorder', 'ICD43'], ['D44', '44', 'disorder', 'ICD44'], ['D45', '45', 'disorder', 'ICD45'], ['D46', '46', 'disorder', 'ICD46'], ['D47', '47', 'disorder', 'ICD47'], ['D48', '48', 'disorder', 'ICD48'], ['D49', '49', 'disorder', 'ICD49']]
RelPatMulti= [['1', '11'], ['2', '22'], ['2', '21'], ['1', '12']]
RelMultiDiag= [['11', '1'], ['22', '1'], ['11', '2'], ['21', '2'], ['22', '3'], ['21', '5'], ['22', '8'], ['22', '9'], ['11', '10'], ['22', '10'], ['11', '12'], ['12', '12'], ['21', '12'], ['22', '12'], ['12', '13'], ['22', '13'], ['11', '16'], ['22', '16'], ['12', '17'], ['21', '17'], ['12', '18'], ['21', '18'], ['12', '22'], ['21', '22'], ['22', '22'], ['21', '23'], ['22', '25'], ['21', '27'], ['21', '28'], ['12', '29'], ['22', '29'], ['22', '30'], ['11', '31'], ['21', '31'], ['22', '31'], ['22', '32'], ['22', '34'], ['21', '35'], ['12', '36'], ['21', '40'], ['22', '41'], ['11', '42'], ['21', '42'], ['21', '43'], ['11', '44'], ['21', '45'], ['11', '46'], ['21', '46'], ['22', '46'], ['22', '47'], ['22', '48'], ['11', '49']]












###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################




step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

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

    nodeTypes = nodeTypesF + nodeTypesG + nodeTypesH
    relationTypes = relationTypesF + relationTypesH + relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3b.deleteRelation, relationTypes)
        session.execute_write(cl3b.DeleteNodes, nodeTypes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DIAGdataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
    row={'name':DIAGdataSet+'_clearConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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
    row={'name':DIAGdataSet+'_createConstraint', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('Constraint creating done: took '+str(end - last)+' seconds')
    last = end




print("-------------------------------------------------------------------------------------------------------------------------")


step_Patients_Nodes=True
if step_Patients_Nodes :
    print('                      ')
    print('Step4 - Creating Patients Nodes...')
    # convert each record in the CSV table into an Event node

    for item in casePa:

        with driver.session() as session:
            session.execute_write(cl3b.Patients_Nodes, item[0], item[1])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':DIAGdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end

print("-------------------------------------------------------------------------------------------------------------------------")




step_Admissions_Nodes=True
if step_Admissions_Nodes :
    print('                      ')
    print('Step4 - Creating Admissions Nodes...')
    # convert each record in the CSV table into an Event node

    for item in multi:

        with driver.session() as session:
            session.execute_write(cl3b.Admissions_Nodes, item[0], item[1])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':DIAGdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end


print("-------------------------------------------------------------------------------------------------------------------------")




step_Diagnoses_Nodes=True
if step_Diagnoses_Nodes :
    print('                      ')
    print('Step4 - Creating Diagnoses Nodes...')
    # convert each record in the CSV table into an Event node

    for item in Diag:

        with driver.session() as session:
            session.execute_write(cl3b.Diagnoses_Nodes, item[0], item[1], item[2], item[3])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':DIAGdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
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

    for item in RelPatMulti:

        with driver.session() as session:
            session.execute_write(cl3b.Patients_Admission_Rel, item[0], item[1])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':DIAGdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end

print("-------------------------------------------------------------------------------------------------------------------------")


step_Admission_Diagnoses_Rel=True
if step_Admission_Diagnoses_Rel :
    print('                      ')
    print('Step4 - Creating Admission Diagnoses Rel...')
    # convert each record in the CSV table into an Event node

    for item in RelMultiDiag:

        with driver.session() as session:
            session.execute_write(cl3b.Admission_Diagnoses_Rel, item[0], item[1])
            print(f'\n     * entity nodes done')


        end = time.time()
        row={'name':DIAGdataSet+'_SCT_import', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print('SCT nodes done: took '+str(end - last)+' seconds')
        last = end

print("-------------------------------------------------------------------------------------------------------------------------")



end = time.time()
row = {'name': DIAGdataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DIAG_Perf_FileName)
perf.to_csv(fullname)
driver.close()



