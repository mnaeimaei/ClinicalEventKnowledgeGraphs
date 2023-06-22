import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase
def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i == 0):
                headerCSV = list(row)
                i += 1
            else:
                datasetList.append(row)

    log = pd.DataFrame(datasetList, columns=headerCSV)

    return headerCSV, log

def Create_CSV_in_Neo4J_import(union, Neo4JImport, outputFileName):
    sampleIds = []
    sampleList = []  # create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in union.iterrows():
        if sampleIds == [] :
            rowList = list(row)  # add the event data to rowList
            #print(rowList)
            sampleList.append(rowList)  # add the extended, single row to the sample dataset
            #print(sampleList)

    header = list(union)  # save the updated header data
    #print(header)
    logSamples = pd.DataFrame(sampleList, columns=header)  # create pandas dataframe and add the samples

    logSamples.fillna(0)
    logSamples.to_csv(Neo4JImport + outputFileName, index=True, index_label="idx", na_rep="Unknown")


def ImportCSV(inputPath, inputFileName):
    csvLog = pd.read_csv(os.path.realpath(inputPath + inputFileName), keep_default_na=True)  # load full log from csv
    csvLog.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    return csvLog




def deleteRelation(tx, relationTypes):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)



def Entity1_Potential_Entities(tx, icd_code, DK3_icd_version, DK3_icd_code_syn):

    qLinkSCTs = f''' 
            MATCH ( s:ICD ) WHERE s.icd_code_syn="{DK3_icd_code_syn}"
            MATCH ( n:Disorder ) WHERE n.icd_code_syn="{DK3_icd_code_syn}" 
            CREATE ( n ) -[:LINKED_TO]-> ( s );
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)

    qTesting = f'''
            #########Testing:#######################################
            MATCH p=( n:Entity ) -[:LINKED_TO]-> (s:Potential)
            RETURN p;
            
            
            ##############################################################
            '''
    print("Testing:")
    print(qTesting)





################################################################################

