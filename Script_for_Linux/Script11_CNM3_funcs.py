import pandas as pd
import time, os, csv
import copy
from collections import Counter


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


def Create_CSV_in_Neo4J_import11(union):
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


def CreateMappingRelation3(csvLog,Activity,Activity_Synonym,OTC,SCTCode):
    csvLog9 = csvLog[[Activity,Activity_Synonym,OTC,SCTCode]]
    csvLog9.reset_index(drop=True, inplace=True)

    Concept_Relation=[]
    x =len(csvLog.index)
    for i in range(x):
        list1=[]
        a=csvLog.iloc[i][Activity]
        b=csvLog.iloc[i][Activity_Synonym]
        d=int(csvLog.iloc[i][OTC])
        e=int(csvLog.iloc[i][SCTCode])

        list1.append(a)
        list1.append(b)
        list1.append(d)
        list1.append(e)
        Concept_Relation.append(list1)
    return Concept_Relation


def sc3(driver, Activity_OCT_MappingRelation, distanceFrom_ActConc):
    myList1=copy.deepcopy(Activity_OCT_MappingRelation)
    #print(myList1)
    listFinal = []
    length=len(myList1)
    #print(length)
    for k in range(len(myList1)):
        x1=myList1[k][0]
        x2 =myList1[k][1]
        x3 = myList1[k][2]
        #print("x1=",x1)
        #print("x2=", x2)
        #("x3=", x3)


        query1 = f'''     
        
        
        Match (s1:Concept) -[r2:ANCESTOR_OF*{distanceFrom_ActConc}]->(s2:Concept)
        where s1.conceptId={x3}
        return distinct s2.conceptId, s2.conceptCode, s2.Semanti_tags
        '''
        print(query1)
        with driver.session() as session:
            record1 = session.run(query1).values()
            if record1 :
                counts = Counter(sublist[2] for sublist in record1)
                most_common_category = counts.most_common(1)[0][0]
                filtered_list = [sublist for sublist in record1 if sublist[2] == most_common_category]
                filtered_list_2=filtered_list[0]
                filtered_list_3 = filtered_list_2[:2]  # Keeps the first two elements
                #print("filtered_list_3=", filtered_list_3)


                filtered_list_3 = [x2] + filtered_list_3
                filtered_list_3 = [x1] + filtered_list_3

                #print("filtered_list_3=", filtered_list_3)

                listFinal.append(filtered_list_3)
                #print("listFinal=", listFinal)



        formatted_number = "{:.2f}".format(100 * k / length)
        print("Completed: ", formatted_number, "%")

    return listFinal


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




def Activity_OCPS(tx, Activity,	Activity_Synonym,	SCTID,	SCTCode):

    qLinkSCTs = f''' 
            MATCH ( c:Activity ) where c.Name="{Activity}" and c.Syn="{Activity_Synonym}"  
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( c ) -[:MAPPED_TO]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)




def ActivityProperty_OCPS(tx, Activity,	Activity_Synonym,	SCTID,	SCTCode):

    qLinkSCTs = f''' 
            MATCH ( c:ActivityPropery ) where c.Name="{Activity}" and c.Syn="{Activity_Synonym}"  
            MATCH ( s: Concept ) where s.conceptId={SCTID} and s.conceptCode={SCTCode}
            CREATE ( c ) -[:MAPPED_TO]-> ( s )
            '''
    print(qLinkSCTs)
    tx.run(qLinkSCTs)



################################################################################


