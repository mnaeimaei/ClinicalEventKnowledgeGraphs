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




def runQuery(driver, query):
    with driver.session() as session:
        result = session.run(query)
        if result != None:
            return result
        else:
            return None

def deleteRelation(tx, relationTypes):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)


def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)

def clearConstraint(tx, x, driver):
    for x in tx.run("CALL db.constraints();"):


        ############PART G ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( icd:ICD ) ASSERT (icd.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:ICD) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:ICD) ASSERT ep.ID  IS UNIQUE;")

        ############PART H ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( concept:Concept ) ASSERT (concept.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Concept) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Concept) ASSERT m.ID  IS UNIQUE;")

        #############################################################

        else:
            print("Database is empty and nothing to drop")


def createConstraint(tx):

    ##################Part2
    qCS6 = f'CREATE CONSTRAINT ON (n:ICD) ASSERT n.ID IS UNIQUE;'



    qTest = f'''
            #########Step6 Testing:#######################################
            :schema'
            ##############################################################
            '''
    print(qCS6)



    print(qTest)


    tx.run(qCS6)






def loadPotential_Nodes(logHeader, Neo4J_import_fileName, LogID=""):
    print('Creating Clinical Nodes from CSV')
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{Neo4J_import_fileName}\" as line  '
    print(logHeader)

    for col in logHeader:

        if col == 'idx' :
            column = f'toInteger(line.{col})'
        elif col in ['timestamp', 'start', 'end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.' + col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (s:ICD {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (s:ICD {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader) - 1):
            newLine = f' {col}: {column} }})'

        query = query + newLine

    testingQ = f'''
            #########Step3 Testing:######################################
            MATCH (s:ICD) 
            return s;

            MATCH (s:ICD) 
            return count(*) as count;
            #############################################################
            '''



    return query, testingQ



