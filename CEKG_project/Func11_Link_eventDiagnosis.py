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







def Event_Potential(tx,DK_ID , Activity , Activity_Synonym, Activity_Origin , Activity_Value_ID , Icd9_Code_Short_list):
    qLinkSCTs = f''' 
            MATCH ( e:Event ) where e.Activity_Origin="{Activity_Origin}" and e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Disorder ) where p.Icd_code_Short="{Icd9_Code_Short_list}"
            CREATE ( e ) -[:BOND]-> ( p )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)


################################################################################


def usefulQuery():




    qTesting8 = f'''
            #########Testing: Step 8#######################################
            
            
            MATCH z1=( e ) -[:BOND]-> ( p )
            where  e.Activity_Origin="Respiratory_Therapy" and e.Activity_Value_ID="398032" and p.Icd_code_Short="23"
            return z1


            ##############################################################
             '''
    print("Testing: Step8")
    print(qTesting8)