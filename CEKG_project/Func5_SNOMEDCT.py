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

def DeleteAllNodesRels(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)

def clearConstraint(tx, x, driver):
    for x in tx.run("CALL db.constraints();"):

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
    qCS1 = f'CREATE CONSTRAINT ON (m:Concept) ASSERT m.ID IS UNIQUE;'
    qTest = f'''
            #########Step6 Testing:#######################################
            :schema
            ##############################################################
            '''
    print(qCS1)

    print(qTest)

    tx.run(qCS1)





def loadOCPSConcepts(logHeader, Neo4J_import_fileName, LogID=""):
    print('Creating Concept Nodes from CSV')
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{Neo4J_import_fileName}\" as line  '
    print(logHeader)

    for col in logHeader:

        if col == 'idx' or col == 'conceptId' or col == 'conceptCode' :
            column = f'toInteger(line.{col})'
        elif col in ['timestamp', 'start', 'end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.' + col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (s:Concept {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (s:Concept {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader) - 1):
            newLine = f' {col}: {column} }})'

        query = query + newLine


    testingQ = f'''
            #########Step3 Testing:######################################
            MATCH (s:Concept) 
            return s;

            MATCH (s:Concept) 
            return count(*) as count;
            #############################################################
            '''



    return query, testingQ






def link_Concepts(tx, s0, s0_code, s1, s1_code):
    qLinkSCTs1 = f''' 
            MATCH ( n1:Concept )
            MATCH ( n2:Concept ) 
            where n1.conceptId = {s0} AND n1.conceptCode = {s0_code} AND n2.conceptId = {s1}  AND n2.conceptCode = {s1_code}
            CREATE ( n1 ) -[:ANCESTOR_OF]-> ( n2 );
            '''



    print(qLinkSCTs1)
    tx.run(qLinkSCTs1)








################################################################################


def usefulQuery():

    qTesting6 = f'''
            #########Testing: Step 6#######################################

            MATCH p=( c1:Concept ) -[:ANCESTOR_OF]-> ( c2:Concept)
            WHERE c1.conceptId = 85919009 AND c1.conceptCode = 1 AND c2.conceptId = 79787007  AND c2.conceptCode = 1
            RETURN p;
            
            MATCH p=( c1:Concept ) -[:ANCESTOR_OF]-> ( c2:Concept)
            RETURN p;
            
            MATCH p=( c1:Concept ) -[*]-> (c2:Concept)
            where c1.conceptId=85919009
            RETURN p;
            
            #Part of SNOMED CT:
            MATCH p=( c1:Concept ) -[:ANCESTOR_OF]-> (c2:Concept) -[:ANCESTOR_OF]-> (c3:Concept) -[:ANCESTOR_OF]-> (c4:Concept) -[:ANCESTOR_OF]-> (c5:Concept) -[:ANCESTOR_OF]-> (c6:Concept)
            where c6.conceptId=138875005
            RETURN p
            limit 20;        
            
            ##############################################################
             '''
    print("Testing: Step6")
    print(qTesting6)

