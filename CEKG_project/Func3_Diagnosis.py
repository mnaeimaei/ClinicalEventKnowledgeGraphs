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
        ############PART F ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( patient:Patient ) ASSERT (patient.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Patient) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Patient) ASSERT ep.ID  IS UNIQUE;")


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
    qCS1 = f'CREATE CONSTRAINT ON (n:Patient) ASSERT n.ID IS UNIQUE;'
    qCS2 = f'CREATE CONSTRAINT ON (n:Admission) ASSERT n.ID IS UNIQUE;'
    qCS3 = f'CREATE CONSTRAINT ON (n:Disorder) ASSERT n.ID IS UNIQUE;'



    qTest = f'''
            #########Step6 Testing:#######################################
            :schema'
            ##############################################################
            '''
    print(qCS1)
    print(qCS2)
    print(qCS3)



    print(qTest)


    tx.run(qCS1)
    tx.run(qCS2)
    tx.run(qCS3)


def Patients_Nodes(tx, Entity1_Origin, Entity1_ID):
    qCreateEntity = f'''

            CREATE (p:Patient {{EntityName: "{Entity1_Origin}", ID: "{Entity1_ID}" }});

            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (p:Patient)
            return p;


            MATCH (p:Patient)
            where p.ID="{Entity1_ID}"
            return p;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


def Admissions_Nodes(tx, Entity2_Origin, Entity2_ID):
    qCreateEntity = f'''

            CREATE (a:Admission {{EntityName: "{Entity2_Origin}", ID: "{Entity2_ID}" }});

            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (a:Admission)
            return a;


            MATCH (a:Patient)
            where a.ID="{Entity2_ID}"
            return a;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


def Diagnoses_Nodes(tx, icd_code_syn, Icd_code_Short, potentialEntity, icd_short):
    qCreateEntity = f'''


            CREATE (d:Disorder {{icd_code_syn: "{icd_code_syn}", Icd_code_Short: "{Icd_code_Short}", potentialEntity: "{potentialEntity}", icd_short: "{icd_short}" }});
            
            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (d:Disorder)
            return d;


            MATCH (d:Disorder)
            where d.icd_code_syn="{icd_code_syn}"
            return d;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


def Patients_Admission_Rel(tx, PatientID, AdmissionID):
    qCreateEntity = f'''

           
            MATCH (p:Patient) WHERE p.ID ="{PatientID}"
            MATCH  (a:Admission) WHERE a.ID ="{AdmissionID}"
            CREATE (p)-[:poses]->(a);
            '''


    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH k=(p)-[:poses]->(a)
            return k;


            MATCH k=(p)-[:poses]->(a)
            WHERE p.ID="{PatientID}" and a.ID="{PatientID}" and 
            return k;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


def Admission_Diagnoses_Rel(tx, AdmissionID, DiagnosesID):
    qCreateEntity = f'''
    
            MATCH (a:Admission) WHERE a.ID ="{AdmissionID}"
            MATCH  (d:Disorder) WHERE d.Icd_code_Short ="{DiagnosesID}"
            CREATE (a)-[:owns]->(d);
            '''


    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH k=(a)-[:owns]->(d)
            return k;
            
            MATCH k=(p)-[:poses]->(a)-[:owns]->(d)
            return k;


            MATCH k=(a)-[:owns]->(d)
            WHERE a.ID="{AdmissionID}" and d.Icd_code_Short="{DiagnosesID}"  
            return k;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


