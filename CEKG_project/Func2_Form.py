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


def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)

def clearConstraint(tx, x, driver):
    for x in tx.run("CALL db.constraints();"):

        ############PART E ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( act:Act ) ASSERT (act.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Act) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Act) ASSERT ep.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( form:Form ) ASSERT (form.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Form) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Form) ASSERT ep.ID  IS UNIQUE;")


        ############PART F ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( patient:Patient ) ASSERT (patient.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Potential) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Potential) ASSERT ep.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( admission:Admission ) ASSERT (admission.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Admission) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Admission) ASSERT m.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( disorder:Disorder ) ASSERT (disorder.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Disorder) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Disorder) ASSERT m.ID  IS UNIQUE;")

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
    qCS6 = f'CREATE CONSTRAINT ON (n:Form) ASSERT n.ID IS UNIQUE;'
    qCS7 = f'CREATE CONSTRAINT ON (n:Act) ASSERT n.ID IS UNIQUE;'



    qTest = f'''
            #########Step6 Testing:#######################################
            :schema'
            ##############################################################
            '''
    print(qCS6)
    print(qCS7)



    print(qTest)


    tx.run(qCS6)
    tx.run(qCS7)




def Form_Nodes(tx, caseForm):
    qCreateEntity = f'''

            CREATE (f:Form {{FormName: "{caseForm}" }});

            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (a:FormName)
            return a;


            MATCH (a:FormName)
            where a.FormName="{caseForm}"
            return a;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


def Act_Nodes(tx, Activity, Activity_Synonym):
    qCreateEntity = f'''



            CREATE (a:Act {{Activity: "{Activity}", Activity_Synonym: "{Activity_Synonym}" }});

            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (a:Act)
            return a;


            MATCH (a:Act)
            where a.Activity="{Activity}"
            return a;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)



def formAct_Rel(tx, Activity, Activity_Synonym, Activity_Origin):
    qCreateEntity = f'''


            MATCH  (f:Form) WHERE f.FormName ="{Activity_Origin}"
            MATCH (a:Act) WHERE a.Activity ="{Activity}" and a.Activity_Synonym ="{Activity_Synonym}" 
            CREATE (f)-[:INSIDE]->(a);
            '''

    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH k=(f)-[:INSIDE]->(a);
            return k;


            MATCH k=(f)-[:INSIDE]->(a);
            WHERE f.Form ="{Activity_Origin}" and a.Activity ="{Activity}" and a.Activity_Synonym ="{Activity_Synonym}"
            return k;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)
