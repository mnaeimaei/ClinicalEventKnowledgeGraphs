import pandas as pd
import time, os, csv
import numpy as np
import copy


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


print("************************** Step12 ****************************************************************************")


def Create_CSV_in_Neo4J_import12(union):
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


def CreateMappingRelation3(csvLog, DK6_Activity_Origin, DK6_OTC, DK6_SCTCode):
    csvLog9 = csvLog[[DK6_Activity_Origin, DK6_OTC, DK6_SCTCode]]
    csvLog9.reset_index(drop=True, inplace=True)

    Concept_Relation = []
    x = len(csvLog.index)
    for i in range(x):
        list1 = []
        a = csvLog.iloc[i][DK6_Activity_Origin]
        b = csvLog.iloc[i][DK6_OTC]
        c = csvLog.iloc[i][DK6_SCTCode]

        # Convert to Python int if necessary
        if isinstance(a, (np.integer, np.int64)):
            a = int(a)
        if isinstance(b, (np.integer, np.int64)):
            b = int(b)
        if isinstance(c, (np.integer, np.int64)):
            c = int(c)

        list1.append(a)
        list1.append(b)
        list1.append(c)
        Concept_Relation.append(list1)

    return Concept_Relation

def sc1(driver, org_list):
    myList=copy.deepcopy(org_list)
    # print(listFinal)
    return myList

def sc2(driver, rel1, rel2):
    myList1=copy.deepcopy(rel1)
    myList2 = copy.deepcopy(rel2)
    #print(myList1)
    #print(myList2)
    listFinal = []
    for i in range(len(myList1)):
        list1 = []
        item1=[myList1[i][0]]
        item2 =[myList1[i][1]]
        item3 = myList1[i][2]
        #print(item1)
        #print(item2)
        #print(item3)
        for j in range(len(myList2)):
            num1 = myList2[j][0]
            num2 = [myList2[j][1]]
            num3 = [myList2[j][2]]
            #print(num1)
            #print(num2)
            #print(num3)
            if item3 == num1:
                item1.extend(item2)
                item1.extend(num2)
                item1.extend(num3)
                #print(item1)
                list1.append(item1)
                #print(list1)
                listFinal.extend(list1)
                #print(listFinal)
    return listFinal



def sc3(driver, rel1, rel2, upperAncestorLen,Semanti_tags,ConceptType,TLC_Semanti_tags):
    myList1=copy.deepcopy(rel1)
    myList2 = copy.deepcopy(rel2)
    #print(myList1)
    #print(myList2)
    listFinal = []
    length = len(myList2)
    for k in range(len(myList2)):
        x1=myList2[k][0]
        x2 =myList2[k][1]
        x3 = myList2[k][2]
        #print(x1)
        #print(x2)
        #print(x3)
        if upperAncestorLen==0:
            query1 = f'''     
            MATCH (s1:Concept)-[r1:ANCESTOR_OF]->(s2:Concept) -[r2:ANCESTOR_OF]->(s3:Concept) 
            where s1.conceptId={x2} and s2.Semanti_tags="{Semanti_tags}" and s2.ConceptType="{ConceptType}" and s3.ConceptType="TLC" and s3.Semanti_tags="{TLC_Semanti_tags}"
            return distinct s2.conceptId, s2.conceptCode
            '''
            print(query1)
            with driver.session() as session:
                record1 = session.run(query1).values()
                #print("record1=", record1)
                for sublist in record1:
                    sublist.insert(0, x1)
                #print("record1=", record1)
                listFinal.extend(record1)
                #print("listFinal=", listFinal)

        if upperAncestorLen != 0:
            query1 = f'''     
            MATCH (s1:Concept)-[r1:ANCESTOR_OF*]->(s2:Concept) -[r2:ANCESTOR_OF*{upperAncestorLen}]->(s3:Concept) 
            where s1.conceptId={x2} and s2.Semanti_tags="{Semanti_tags}" and s2.ConceptType="{ConceptType}" and s3.ConceptType="TLC" and s3.Semanti_tags="{TLC_Semanti_tags}"
            return distinct s2.conceptId, s2.conceptCode
            '''
            print(query1)
            with driver.session() as session:
                record1 = session.run(query1).values()
                #print("record1=", record1)
                for sublist in record1:
                    sublist.insert(0, x1)
                #print("record1=", record1)
                listFinal.extend(record1)
                #print("listFinal=", listFinal)

        formatted_number = "{:.2f}".format(100 * k / length)
        print("Completed: ", formatted_number, "%")

    myList4 = copy.deepcopy(listFinal)
    df = pd.DataFrame(myList4, columns=['Column1', 'Column2','Column3'])
    df['Column4'] = df.groupby('Column2')['Column2'].transform('count')
    df = df.sort_values(by='Column4', ascending=False)
    df = df.drop_duplicates(subset='Column1', keep='first')
    df = df.reset_index(drop=True)
    df = df.drop(columns='Column4')
    list_of_lists = df.values.tolist()
    #print(list_of_lists)
    #print(myList1)


    list3=[]
    for i in range(len(myList1)):
        #print(i)
        list1 = []
        item1=[myList1[i][0]]
        item2 =[myList1[i][1]]
        item3 = myList1[i][2]
        for j in range(len(list_of_lists)):
            num1 = list_of_lists[j][0]
            num2 = [list_of_lists[j][1]]
            num3 = [list_of_lists[j][2]]
            if item3 == num1:
                item1.extend(item2)
                item1.extend(num2)
                item1.extend(num3)
                #print(item1)
                list1.append(item1)
                #print(list1)
                list3.extend(list1)
                #print(list3)
    return list3


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


def deletePartNode(tx, EntityItems):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Relative" delete n;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)


def deleteProperty(tx, EntityItems):
    qDeleteAllNodes = f'''MATCH (n:{EntityItems})  where n.Category="Absolute" remove n.uID, n.Type;'''
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)


def Activity_OCPS(tx, DomainName, SCTID, SCTCode):
    qLinkSCTs = f''' 
            MATCH ( f:Domain ) where f.Name="{DomainName}"   
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( f ) -[:TIED]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)


def Domain_Scenario_1(tx, Activity, Activity_Synonym, Domain):
    qLinkSCTs = f''' 

            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (d:Domain) WHERE d.Name ="{Domain}"
            CREATE (a)-[:TYPE_OF{{Scenario:"1"}}]->(d);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


def Domain_Scenario_2(tx, Activity, Activity_Synonym, conceptId, conceptCode):
    qLinkSCTs = f''' 
            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"2"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


def Domain_Scenario_3(tx, Activity, Activity_Synonym, conceptId, conceptCode):
    qLinkSCTs = f''' 
            MATCH  (a:Activity) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"3"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


def Domain_Scenario_1_Proprty(tx, Activity, Activity_Synonym, Domain):
    qLinkSCTs = f''' 

            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (d:Domain) WHERE d.Name ="{Domain}"
            CREATE (a)-[:TYPE_OF{{Scenario:"1"}}]->(d);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


def Domain_Scenario_2_Proprty(tx, Activity, Activity_Synonym, conceptId, conceptCode):
    qLinkSCTs = f''' 
            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"2"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


def Domain_Scenario_3_Proprty(tx, Activity, Activity_Synonym, conceptId, conceptCode):
    qLinkSCTs = f''' 
            MATCH  (a:ActivityPropery) WHERE a.Syn ="{Activity_Synonym}" and a.Name ="{Activity}" 
            MATCH  (c:Concept) WHERE c.conceptId ={conceptId} and c.conceptCode ={conceptCode}
            CREATE (a)-[:TYPE_OF{{Scenario:"3"}}]->(c);
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_relationship_massage(Result)


