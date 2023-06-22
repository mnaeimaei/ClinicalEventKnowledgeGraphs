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







def Activity_OCPS(tx, Activity,	Activity_Synonym,	Activity_Origin,	OTC,	SCTCode):

    qLinkSCTs = f''' 
            MATCH ( c:Class ) where c.ID="{Activity}" and c.Syn="{Activity_Synonym}"   and c.Origin="{Activity_Origin}"  
            MATCH ( s: Concept ) where s.conceptId={OTC} and s.conceptCode={SCTCode}
            CREATE ( c ) -[:MAPPED_TO]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)


################################################################################


def usefulQuery():




    qTesting8 = f'''
            #########Testing: Step 8#######################################
            MATCH ( c:Class ) where c.ID="Respiratory_Therapy" and c.Syn="RT"
            MATCH ( s: Concept ) where s.conceptId=53950000 and s.conceptCode=1
            return c,s;
            
            MATCH p1=(l)-[:HAS]->(e)-[:CORR]->(n)
            MATCH p2=(e) -[:OBSERVED]-> (c)
            MATCH p3=(c) -[:MAPPED_TO]-> (s)
            where e.Event="e210" and c.TypeName="Respiratory_Therapy" and c.Syn="RT" and s.conceptId=53950000 and s.conceptCode=1
            return p1,p2,p3;

            MATCH p1=(l)-[:HAS]->(e)-[:CORR]->(n)
            MATCH p2=(e) -[:OBSERVED]-> (c)
            MATCH p3=(c) -[:MAPPED_TO]-> (s1:Concept) -[*]-> (s2:Concept)
            where e.Event="e210" and c.TypeName="Respiratory_Therapy" and c.Syn="RT" and s1.conceptId=53950000 and s1.conceptCode=1
            return p1,p2,p3;
            
            MATCH p1=(l)-[:HAS]->(e)-[:CORR]->(n)
            MATCH p2=(e) -[:OBSERVED]-> (c)
            MATCH p3=(c) -[:MAPPED_TO]-> (s1:Concept) -[r:ANCESTOR_OF*3]-> (s2:Concept)
            where e.Event="e210" and c.TypeName="Respiratory_Therapy" and c.Syn="RT" and s1.conceptId=53950000 and s1.conceptCode=1
            return p1,p2,p3;




            
            
            MATCH z1=(l)-[:HAS]->(e)-[:CORR]->(n)-[:EntityPotentialRel]->( p:Potential ) -[:CONNECTED_TO]-> (s3:Concept) -[*]-> (s4:Concept)
            MATCH z2=(e) -[:OBSERVED]-> (c) -[:MAPPED_TO]-> (s1:Concept) -[*]-> (s2:Concept)
            where e.Event="e210" and c.TypeName="Respiratory_Therapy" and c.Syn="RT" and s1.conceptId=53950000 and s1.conceptCode=1 and n.EntityName="Patient" and p.icd_code_syn="D13"
            return z1,z2;
            
            
            
            MATCH z1=(l)-[:HAS]->(e)-[:CORR]->(n)-[:EntityPotentialRel]->( p:Potential ) -[:CONNECTED_TO]-> (s3:Concept) -[*]-> (s4:Concept)
            where e.Event="e210" and n.EntityName="Patient" and p.icd_code_syn="D13"
            return z1;

            ##############################################################
             '''
    print("Testing: Step8")
    print(qTesting8)