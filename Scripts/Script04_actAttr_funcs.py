


import pandas as pd
import os, csv
from neo4j import GraphDatabase
from datetime import datetime
import ast
from datetime import datetime


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


def Neo4J_label_node_property(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property1 = output["labels_added"]
        property2 = output["nodes_created"]
        property3 = output["properties_set"]
        output_string = f'''
            Added {property1} label, created {property2} node, set {property3} properties

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



def Neo4J_relationship_create(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["relationships_created"]
        output_string = f'''
            Created {property_num} relationship
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


def Create_CSV_in_Neo4J_import3(union):
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




def CreatePro1(csvLog_EnP, AcP_acID, AcP_activityName, AcP_activitySynonym, AcP_label, AcP_Value):
    dataF=csvLog_EnP
    column_names = [AcP_acID, AcP_activityName, AcP_activitySynonym, AcP_label, AcP_Value]
    distinct_values = dataF[column_names].drop_duplicates().values.tolist()
    converted_NodeList = [[str(item[0]), item[1], item[2], item[3], item[4]] for item in distinct_values]
    return converted_NodeList


def CreatePro2(csvLog_EnP, AcP_acID):
    dataF=csvLog_EnP
    column_names = [AcP_proID,AcP_acID]
    distinct_values = dataF[column_names].drop_duplicates().values.tolist()
    return distinct_values

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





def createProperty(tx, acID, activityName, activitySynonym, label, Value):

    qCreateEntity = f'''           
            MERGE (d:Feature {{
            ID:"{acID}",          
            Name:"{activityName}",          
            Synonym:"{activitySynonym}",
            label:"{label}",          
            Value:"{Value}"
            }});
            '''



    qTest = f'''
            ######### Testing:#######################################
            MATCH (d:Feature)
            WHERE n.ID = "{acID}" 
            return n;
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    Neo4J_label_node_property(Result)
    print(qTest)





def createEnProperty(tx, pID):

    qCreateEntity = f'''
            MATCH (e)  WHERE "{pID}" in  e.Activity_Properties_ID
            MATCH (d:Feature)  WHERE d.ID = "{pID}"
            CREATE (e)-[:Assign]->(d);
            '''

    qTest = f'''
            ######### Testing:#######################################
            return (e)-[:Assign]->(d);
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    Neo4J_relationship_create(Result)
    print(qTest)



