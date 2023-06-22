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







def Patient_Patient(tx):
    qLinkSCTs = f''' 
            MATCH (n:Entity) where n.EntityType="P"
            MATCH (c:Patient) 
            where n.ID=c.ID
            CREATE (n) -[:Patient_Patient]-> (c) 
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)


################################################################################


def usefulQuery():




    qTesting8 = f'''
            #########Testing: Step 8#######################################
            
            MATCH p1=(l)-[:HAS]->(e1)-[:CORR]->(n1)<-[:CORR]-(e2)-[:HAS]-(l)
            MATCH p2=(l)-[:HAS]->(e1)-[:CORR]->(n2)<-[:CORR]-(e2)-[:HAS]-(l)
            MATCH p3=( e1 ) -[:OBSERVED]-> ( c1 )<-[:Activity_Class]-(k1)
            MATCH p4=( e2 ) -[:OBSERVED]-> ( c2)<-[:Activity_Class]-(k2)
            MATCH p5=( n1 ) -[:Patient_Patient]-> (k)-[:poses]->(a)-[:owns]->(d) 

            where e1.Event="e21" and e2.Event="e22" and n1.EntityType="Patient" and n2.EntityType="Admission"
            return p1,p2,p3,p4,p5
            
            
            


            ##############################################################
             '''
    print("Testing: Step8")
    print(qTesting8)