import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase


import Func8_Link_icdSNOMED as cl3e

from tqdm import tqdm

print("************************** From cl1: ****************************************************************************")


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
DK4dataSet = 'SNOMED_CT_MIMIC'
DK4inputPath = './Test_Input/'

DK4_ICD_OCT_FileName = 'L2_icdSNOMEDCT'

DK4_Extension = '.csv'

DK4_Input_ICD_OCT_FileName = DK4_ICD_OCT_FileName + DK4_Extension

DK4_Neo4JImport_ICD_OCT_FileName = DK4_ICD_OCT_FileName + '_Neo4j' + DK4_Extension

DK4_Perf_FileName = DK4dataSet + '_Performance' + DK4_Extension

DK4_icd_code = "icd_code"
DK4_icd_code_syn = "icd_code_syn"
DK4_OTC = "OTC"




print("************************** input from cl1: Potential_OCPS ****************************************************************************")

Neo4JImport= "/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-a515f805-111e-4a28-af86-171508c79169/import/"

icdOCT= [['codeICD1', 'D01', '0'], ['codeICD2', 'D02', '1'], ['codeICD3', 'D03', '2'], ['codeICD5', 'D05', '3'], ['codeICD8', 'D08', '4'], ['codeICD9', 'D09', '5'], ['codeICD10', 'D10', '6'], ['codeICD12', 'D12', '7'], ['codeICD13', 'D13', '8'], ['codeICD16', 'D16', '9'], ['codeICD17', 'D17', '10'], ['codeICD18', 'D18', '11'], ['codeICD22', 'D22', '12'], ['codeICD23', 'D23', '13'], ['codeICD25', 'D25', '14'], ['codeICD27', 'D27', '15'], ['codeICD28', 'D28', '16'], ['codeICD29', 'D29', '17'], ['codeICD30', 'D30', '18'], ['codeICD31', 'D31', '19'], ['codeICD32', 'D32', '20'], ['codeICD34', 'D34', '21'], ['codeICD35', 'D35', '22'], ['codeICD36', 'D36', '23'], ['codeICD40', 'D40', '24'], ['codeICD41', 'D41', '25'], ['codeICD42', 'D42', '26'], ['codeICD43', 'D43', '27'], ['codeICD44', 'D44', '28'], ['codeICD45', 'D45', '29'], ['codeICD46', 'D46', '30'], ['codeICD47', 'D47', '31'], ['codeICD48', 'D48', '32'], ['codeICD49', 'D49', '33']]




###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

step_Clear_OCT_DB=True
if step_Clear_OCT_DB:
    print('                      ')
    print('Step1 - Clearing DB...')



    relationTypesDK = [":CONNECTED_TO", ":MAPPED_TO", ":Form_OCT", ":BOND"]

    relationTypes = relationTypesDK


    with driver.session() as session:
        session.execute_write(cl3e.deleteRelation, relationTypes)


    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start

    end = time.time()
    row={'name':DK4dataSet+'_clearDB', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print('DB Clearing done: took '+str(end - last)+' seconds')
    last = end






print("-------------------------------------------------------------------------------------------------------------------------")


step_link_potential_OCPS=True
if step_link_potential_OCPS:
    print('                      ')
    print('Step6 - Creating Relationship between potential to OCPS......')

    for item in icdOCT:
        with driver.session() as session:
            session.execute_write(cl3e.Potential_OCPS, item[0], item[1] , item[2])



    end = time.time()
    row={'name':DK4dataSet+'_Link_concepts', 'start': time.ctime(last), 'end': time.ctime(end), 'duration':(end - last)}
    perf2 = pd.DataFrame([row])
    perf = pd.concat([perf, perf2], axis=0, ignore_index=True)
    print(':HAS relationships done: took '+str(end - last)+' seconds')
    last = end





print("-------------------------------------------------------------------------------------------------------------------------")

step_useful_Query=True
if step_useful_Query:
    print('                      ')
    print('Step8 - Useful Query......')
    cl3e.usefulQuery()




print("-------------------------------------------------------------------------------------------------------------------------")


end = time.time()
row = {'name': DK4dataSet + '_total', 'start': time.ctime(last), 'end': time.ctime(end), 'duration': (end - last)}
perf2 = pd.DataFrame([row])
perf = pd.concat([perf, perf2], axis=0, ignore_index=True)


outdir = './Test_Perfomances_Files'
if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, DK4_Perf_FileName)
perf.to_csv(fullname)
driver.close()



