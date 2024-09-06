import pandas as pd
import time, os, csv
import copy
from tqdm import tqdm


import pandas as pd
import os, csv
from neo4j import GraphDatabase
from datetime import datetime
import ast

# Importing Input CSV File


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




def Neo4J_relationship_massage(result):
    output = ast.literal_eval(str(result))
    # print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["properties_set"]
        reltionship_num = output["relationships_created"]
        output_string = f'''
            Set {property_num} properties, created {reltionship_num} relationships
        '''
    return print(output_string)

def ImportCSV(inputFileName):
    csvLog = pd.read_csv(os.path.realpath(inputFileName), keep_default_na=True)  # load full log from csv

    csvLog.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates

    return csvLog


def header_csv(csv):
    csvFinal = csv.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csvHeader = csvFinal.columns.tolist()
    return csvHeader, csvFinal


print("************************** Step13 ****************************************************************************")


def Create_CSV_in_Neo4J_import13(union):
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





def CreateMappingRelation2(csvLog,  Activity_Value_ID , Icd9_Code_Short_list):
    import numpy as np
    csvLog9 = csvLog[[Activity_Value_ID , Icd9_Code_Short_list]]
    csvLog9.reset_index(drop=True, inplace=True)

    Concept_Relation=[]
    x =len(csvLog.index)
    for i in range(x):
        list1=[]


        e=csvLog.iloc[i][Activity_Value_ID]
        f=csvLog.iloc[i][Icd9_Code_Short_list]


        list1.append(e)
        list1.append(f)


        Concept_Relation.append(list1)


    for sublist in Concept_Relation:
        for i in range(len(sublist)):
            if sublist[i] == 'Unknown':
                sublist[i] = np.nan
    return Concept_Relation

import math



def dkSplit(Event_MappingRelation):
    result_list = []
    for sublist in Event_MappingRelation:
        if isinstance(sublist[-1], str):
            split_values = sublist[-1].split(',')
            for value in split_values:
                new_list = sublist[:-1] + [value]
                result_list.append(new_list)

    return result_list

def sc2(driver, org_list):
    myList=copy.deepcopy(org_list)
    # print(listFinal)
    return myList

def sc3(driver, org_list):
    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    for i in range(len(myList)):
        item1=[myList[i][0]]
        #print(item1)
        disorderID = myList[i][1]
        #print(disorderID)

        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) 
        where d.ID="{disorderID}"
        return c.icdCode
        '''

        # print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)


        if flat_list:
            item1.append(flat_list[0])
            print("item1=", item1)
            listFinal.append(item1)
            #print("listFinal=", listFinal)


    # print(listFinal)
    return listFinal




def sc4(driver, org_list):
    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    for i in range(len(myList)):
        item1=[myList[i][0]]
        #print(item1)
        disorderID = myList[i][1]
        #print(disorderID)

        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) 
        where d.ID="{disorderID}"
        return c.icdCode
        '''

        # print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)



        if flat_list:
            item1.append(flat_list[0])
            print("item1=", item1)
            listFinal.append(item1)
            #print("listFinal=", listFinal)


    # print(listFinal)
    return listFinal


def sc5(driver, org_list):
    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    for i in range(len(myList)):
        item1=[myList[i][0]]
        #print(item1)
        disorderID = myList[i][1]
        #print(disorderID)

        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) -[:CONNECTED_TO]->(s:Concept)
        where d.ID="{disorderID}"
        return s.conceptId
        '''
        print(query1)
        with driver.session() as session:
            record1 = session.run(query1).values()
            print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            print("flat_list=", flat_list)

        if flat_list:
            item1.append(flat_list[0])
            print("item1=", item1)
            listFinal.append(item1)
            #print("listFinal=", listFinal)

    #print(listFinal)


    return listFinal



def sc6(driver, org_list, upperAncestorLen,Semanti_tags,ConceptType,TLC_Semanti_tags):
    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    length=len(myList)
    #for i in range(len(myList)):
    for i in range(len(myList)):
        item1=[myList[i][0]]
        #print(item1)
        disorderID = myList[i][1]
        #print(disorderID)

        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) -[:CONNECTED_TO]->(s1:Concept)-[r1:ANCESTOR_OF*]->(s2:Concept) -[r2:ANCESTOR_OF*{upperAncestorLen}]->(s3:Concept) 
        where d.ID="{disorderID}" and s2.Semanti_tags="{Semanti_tags}" and s2.ConceptType="{ConceptType}" and s3.ConceptType="TLC" and s3.Semanti_tags="{TLC_Semanti_tags}"
        return distinct s2.conceptId
        '''

        print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)

        list_C = [[item1[0], item] for item in flat_list]

        #print("list_C=", list_C)
        listFinal.extend(list_C)
        #print("listFinal=", listFinal)
        formatted_number = "{:.2f}".format(100*i/length)
        print("Completed: ",formatted_number,"%")

    # print(listFinal)
    return listFinal



def sc7(driver, org_list, upperAncestorLen,Semanti_tags,ConceptType,TLC_Semanti_tags):
    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    length = len(myList)
    for i in range(len(myList)):
        item1=[myList[i][0]]
        #print(item1)
        disorderID = myList[i][1]
        #print(disorderID)

        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) -[:CONNECTED_TO]->(s1:Concept)-[r1:ANCESTOR_OF*]->(s2:Concept) -[r2:ANCESTOR_OF*{upperAncestorLen}]->(s3:Concept) 
        where d.ID="{disorderID}" and s2.Semanti_tags="{Semanti_tags}" and s2.ConceptType="{ConceptType}" and s3.ConceptType="TLC" and s3.Semanti_tags="{TLC_Semanti_tags}"
        return distinct s2.conceptId
        '''

        print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)

        list_C = [[item1[0], item] for item in flat_list]

        #print("list_C=", list_C)
        listFinal.extend(list_C)
        #print("listFinal=", listFinal)

        formatted_number = "{:.2f}".format(100 * i / length)
        print("Completed: ", formatted_number, "%")

    # print(listFinal)
    myList2 = copy.deepcopy(listFinal)
    df = pd.DataFrame(myList2, columns=['Column1', 'Column2'])
    df['Column3'] = df.groupby('Column2')['Column2'].transform('count')
    df = df.sort_values(by='Column3', ascending=False)
    df = df.drop_duplicates(subset='Column1', keep='first')
    df = df.reset_index(drop=True)
    df = df.drop(columns='Column3')
    list_of_lists = df.values.tolist()

    return list_of_lists


def sc8_icdFinder(driver):
    query0 = f'''     
    MATCH ()-[r:LINKED_TO]->(n:Clinical)
    RETURN n
    ORDER BY rand()
    LIMIT 1
    '''
    #print(query0)
    with driver.session() as session:
        record1 = session.run(query0).values()
        icdCode = [item[0]["ID"] for item in record1]
    value=int(icdCode[0])
    return value


def sc8(driver, org_list,icdCode):

    myList=copy.deepcopy(org_list)
    #print(myList)
    listFinal = []
    query1 = f'''     
    MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical)
    where c.icdCode="{icdCode}" 
    return distinct d.ID
    '''
    print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        #print("record1=", record1)
        flat_list = [item for sublist in record1 for item in sublist]
        print("flat_list=", flat_list)

    newList = [item for item in myList if item[1] in flat_list]
    final = [[item[0], icdCode] for item in newList]

    return final

def sc9_sctIDFinder(driver):
    query0 = f'''     
    MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) -[:CONNECTED_TO]->(s0:Concept)
    with s0
    ORDER BY rand()
    LIMIT 1
    MATCH (s1:Concept)-[r1:ANCESTOR_OF*1]->(s2:Concept) 
    where s1.conceptId=s0.conceptId
    return s2
    ORDER BY rand()
    LIMIT 1
    '''
    #print(query0)
    with driver.session() as session:
        record1 = session.run(query0).values()
        icdCode = [item[0]["ID"] for item in record1]
    value=int(icdCode[0])
    return value


def sc9(driver, org_list,conceptId):
    myList=copy.deepcopy(org_list)
    #print(myList)
    query1 = f'''     
    MATCH (n:Concept) where n.conceptId={conceptId} RETURN n.level
    '''
    print(query1)

    with driver.session() as session:
        record0 = session.run(query1).values()
        record1 = [item for sublist in record0 for item in sublist]
        record2 = list(map(int, record1[0].split(',')))

    listFinal = []
    for i in range(len(record2)):

        item1=record2[i]
        #print(item1)


        query1 = f'''     
        MATCH p=(d:Disorder)-[r:LINKED_TO]->(c:Clinical) -[:CONNECTED_TO]->(s1:Concept)-[r1:ANCESTOR_OF*{item1}]->(s2:Concept) 
        where s2.conceptId={conceptId} 
        return distinct d.ID
        '''

        #print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)

        listFinal.extend(flat_list)
        listFinal = list(set(listFinal))


    newList = [item for item in myList if item[1] in listFinal]
    final = [[item[0], conceptId] for item in newList]

    # print(listFinal)
    return final


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



def deletePartiallyRel(tx, rel, where, val):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)


def deletePartRel(tx):
    qDeleteAllNodes = f'''MATCH (e)-[c:CORR]->(r) where r.Category="Relative" DELETE c;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)





def deletePartNode(tx,EntityItems):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Relative" delete n;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)




def deleteProperty(tx,EntityItems):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Absolute" remove n.uID, n.Type;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)







def Event_Scenario_1(tx, Activity_Value_ID , DisorderID,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Disorder ) where p.ID="{DisorderID}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)





def Event_Scenario_2(tx, Activity_Value_ID , icd_Code,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{icd_Code}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_3(tx, Activity_Value_ID , icd_code_up,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{icd_code_up}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_4(tx, Activity_Value_ID , Snomed,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={Snomed}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_5(tx, Activity_Value_ID , Snomed_up,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={Snomed_up}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_6(tx, Activity_Value_ID , snomed_up_common,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={snomed_up_common}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_7(tx, Activity_Value_ID , specificICD,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Clinical ) where p.icdCode="{specificICD}"
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)




def Event_Scenario_8(tx, Activity_Value_ID , specificSCT,Scenario):
    qLinkSCTs = f''' 
            MATCH ( e: Event ) where e.Activity_Value_ID="{Activity_Value_ID}"
            MATCH ( p: Concept ) where p.conceptId={specificSCT}
            CREATE ( e ) -[:CORR{{Scenario:"{Scenario}"}}]-> ( p )
            '''

    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)





