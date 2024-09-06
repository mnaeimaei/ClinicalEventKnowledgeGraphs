import pandas as pd
import time, os, csv



import pandas as pd
import os, csv
from neo4j import GraphDatabase
from datetime import datetime
import ast

def Neo4j_import_dir (driver):
    Query =f'''
           Call dbms.listConfig() YIELD name, value WHERE name='server.directories.import' RETURN value
           '''
    with driver.session() as session:
        record = session.run(Query).values()
    #driver.close()
    print(record)
    Neo4JImport = record[0][0] + "/"
    return Neo4JImport


def add_row_to_csv(file_path, start,end,stepName):
    start_time = datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    end_time = datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    duration = end - start
    new_row = [stepName, start_time, end_time, duration]
    print('completed after ' + str(duration * 1000) + ' ms')



    try:
        with open(file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(new_row)
        print("Row added successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def Neo4J_properties_set(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["properties_set"]
        output_string = f'''
            Set {property_num} properties
        '''
    return print(output_string)



def Neo4J_removingConstraint(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["constraints_removed"]
        output_string = f'''
            Removed {property_num} constraint

        '''
    return print(output_string)


def Neo4J_creatingConstraint(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["constraints_added"]
        output_string = f'''
            Added {property_num} constraint

        '''
    return print(output_string)

# Importing Input CSV File
def ImportCSV(inputFileName):
    csvLog = pd.read_csv(os.path.realpath(inputFileName), keep_default_na=True)  # load full log from csv

    csvLog.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates

    return csvLog


def header_csv(csv):
    csvFinal = csv.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csvHeader = csvFinal.columns.tolist()
    return csvHeader, csvFinal


def Create_CSV_in_Neo4J_import62(union):
    sampleIds = []
    sampleList = []  # create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in union.iterrows():
        if sampleIds == []:
            rowList = list(row)  # add the event data to rowList
            # print(rowList)
            sampleList.append(rowList)  # add the extended, single row to the sample dataset
            # print(sampleList)

    header = list(union)  # save the updated header data
    # print(header)
    logSamples = pd.DataFrame(sampleList, columns=header)  # create pandas dataframe and add the samples

    logSamples.fillna(0)
    return logSamples


def CreateMappingRelation1(csvLog, s1, s1code, s2, s2code):
    csvLog9 = csvLog[[s1, s1code, s2, s2code]]
    csvLog9.reset_index(drop=True, inplace=True)

    print(csvLog)

    Concept_Relation = []
    x = len(csvLog.index)
    for i in range(x):
        list1 = []
        a = int(csvLog.iloc[i][s1])  # Convert to Python int
        b = int(csvLog.iloc[i][s1code])  # Convert to Python int
        c = int(csvLog.iloc[i][s2])  # Convert to Python int
        d = int(csvLog.iloc[i][s2code])  # Convert to Python int
        list1.append(a)
        list1.append(b)
        list1.append(c)
        list1.append(d)
        Concept_Relation.append(list1)

    return Concept_Relation







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


#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################
#############################################################################################


def deleteRelation(tx, relationTypes):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r:{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)



def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)



def DeleteAllNodesRels(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)



def deletePartiallyRel(tx, rel, where, val):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)



def clearConstraint(tx, x, driver,NodeEntity):
    for item in NodeEntity:
        #print(item)
        for x in tx.run("SHOW CONSTRAINTS;"):
            if x is not None:
                if x["labelsOrTypes"] == [item]:
                    y = x["name"]
                    dropQ = "DROP CONSTRAINT " + y
                    print(dropQ," // for ",item)
                    Result = tx.run(dropQ).consume().counters
                    Neo4J_removingConstraint(Result)


                else:
                    print("Database is empty and nothing to drop")


            else:
                print("Database is empty and nothing to drop")


    qTest = f'''
            ######### Testing:#######################################
            SHOW CONSTRAINTS;
            ##############################################################
            '''
    print(qTest)

def createConstraint(tx, NodeEntity):

    for item in NodeEntity:
        qC = f'CREATE CONSTRAINT FOR (n:{item}) REQUIRE n.ID IS UNIQUE;'
        print(qC)
        Result = tx.run(qC).consume().counters
        Neo4J_creatingConstraint(Result)



    qTest = f'''
            ######### Testing:#######################################
            :schema'
            ##############################################################
            '''

    print(qTest)





def link_Concepts(tx, s0, s0_code, s1, s1_code):
    qLinkSCTs1 = f''' 
            MATCH ( n1:Concept )
            MATCH ( n2:Concept ) 
            where n1.conceptId = {s0} AND n1.conceptCode = {s0_code} AND n2.conceptId = {s1}  AND n2.conceptCode = {s1_code}
            CREATE ( n1 ) -[:ANCESTOR_OF]-> ( n2 );
            '''



    print(qLinkSCTs1)
    tx.run(qLinkSCTs1)




def modify_concept(tx):
    qLinkSCTs1 = f''' 
            MATCH (n:Concept) 
            SET
                n.Category= "Absolute" ,
                n.ID= toString(n.conceptId)
                '''

    print(qLinkSCTs1)
    Result = tx.run(qLinkSCTs1).consume().counters
    Neo4J_properties_set(Result)









################################################################################

