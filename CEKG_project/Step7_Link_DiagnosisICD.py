import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase




import Func7_Link_DiagnosisICD as cl3d

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

DK3dataSet = 'SNOMED_CT_ICD'
DK3inputPath = './Test_Input/'

DK3_Potential_DK3_FileName = 'L1_DiagnosisICD'

DK3_Extension = '.csv'

DK3_Input_Potential_DK3_FileName = DK3_Potential_DK3_FileName + DK3_Extension

DK3_Neo4JImport_Potential_OCPS_FileName = DK3_Potential_DK3_FileName + '_Neo4j' + DK3_Extension

DK3_Perf_FileName = DK3dataSet + '_Performance' + DK3_Extension

DK3_icd9_code = "icd_code"
DK3_icd_version = "icd_version"
DK3_icd_code_syn = "icd_code_syn"


print("************************** input from cl1: Entities_Potential ****************************************************************************")


Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"
DiagClinRel= [['codeICD1', '10', 'D01'], ['codeICD2', '10', 'D02'], ['codeICD3', '10', 'D03'], ['codeICD5', '10', 'D05'], ['codeICD8', '10', 'D08'], ['codeICD9', '10', 'D09'], ['codeICD10', '10', 'D10'], ['codeICD12', '10', 'D12'], ['codeICD13', '10', 'D13'], ['codeICD16', '10', 'D16'], ['codeICD17', '10', 'D17'], ['codeICD18', '10', 'D18'], ['codeICD22', '10', 'D22'], ['codeICD23', '10', 'D23'], ['codeICD25', '10', 'D25'], ['codeICD27', '10', 'D27'], ['codeICD28', '10', 'D28'], ['codeICD29', '10', 'D29'], ['codeICD30', '10', 'D30'], ['codeICD31', '10', 'D31'], ['codeICD32', '10', 'D32'], ['codeICD34', '10', 'D34'], ['codeICD35', '10', 'D35'], ['codeICD36', '10', 'D36'], ['codeICD40', '10', 'D40'], ['codeICD41', '10', 'D41'], ['codeICD42', '10', 'D42'], ['codeICD43', '10', 'D43'], ['codeICD44', '10', 'D44'], ['codeICD45', '10', 'D45'], ['codeICD46', '10', 'D46'], ['codeICD47', '10', 'D47'], ['codeICD48', '10', 'D48'], ['codeICD49', '10', 'D49']]







###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_DK1_DB=True
if step_Clear_DK1_DB:
    print('                      ')
    print('Step1 - Clearing DB...')

    relationTypesDK = [":LINKED_TO", ":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]

    relationTypes = relationTypesDK



    with driver.session() as session:
        session.execute_write(cl3d.deleteRelation, relationTypes)


    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK3inputPath+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end

print("-------------------------------------------------------------------------------------------------------------------------")






step_link_Entity1_Potential=True
if step_link_Entity1_Potential:
    print('                      ')
    print('Step5 - Creating Relationship between Diagnoses and Clinical ......')
    print("")
    print("Inputs:")
    print("DiagClinRel=",DiagClinRel)
    print("")

    for item in DiagClinRel:

        with driver.session() as session:
            session.execute_write(cl3d.Entity1_Potential_Entities, item[0], item[1], item[2])
            print(f'\n     PE relationships done')

        end = time.time()
        row = {'name': DK3inputPath + '_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end),
                   'duration': (end - last)}
        perf2 = pd.DataFrame([row])
        perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
        print(':PE relationships done: took ' + str(end - last) + ' seconds')
        last = end




print("-------------------------------------------------------------------------------------------------------------------------")




end = time.time()
row = {'name': DK3inputPath + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK3_Perf_FileName)
perf.to_csv(fullname)
driver.close()



