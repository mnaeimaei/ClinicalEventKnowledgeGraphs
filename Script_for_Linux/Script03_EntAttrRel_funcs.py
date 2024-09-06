


import pandas as pd
import os, csv
from neo4j import GraphDatabase
from datetime import datetime
import ast
from datetime import datetime

#Importing Input CSV File

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

def ImportCSV(inputFileName):
    csvLog = pd.read_csv(os.path.realpath(inputFileName), keep_default_na=True)  # load full log from csv

    csvLog.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates


    return csvLog

def Create_CSV_in_Neo4J_import22(union):
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
    return logSamples



def header_csv(csv):
    csvFinal = csv.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csvHeader = csvFinal.columns.tolist()
    return csvHeader, csvFinal




def CreateLoL(dataset):
    list_of_lists = dataset.values.tolist()
    modified_list_of_lists = [sublist[0:] for sublist in list_of_lists]
    converted_NodeList = [[item[0], str(item[1]), item[2], item[3]] for item in modified_list_of_lists]
    converted_NodeList.append(['Disorder', 'UNK', 'UNK','UNK', 'Absolute'])
    converted_NodeList.append(['Disorder', 'Nothing', 'Nothing','Nothing', 'Absolute'])
    return converted_NodeList




def ListMaker(A):
    B = []
    for sublist in A:
        new_sublist = []
        i1 = sublist[0]
        i2 = sublist[1]
        i3 = sublist[2]
        i4 = sublist[3]
        new_sublist.append(i1)
        new_sublist.append(i2)
        new_sublist.append(i3)
        if ',' in i4:
            list1 = []
            x = i4.split(',')
            for item in x:
                immutable_copy = new_sublist[:]
                immutable_copy.append(item)
                list1.append(immutable_copy)
        else:
            immutable_copy2 = new_sublist[:]
            immutable_copy2.append(i4)
            list1=[immutable_copy2]

        B.extend(list1)
    return B

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



def clearConstraint(tx, x, driver,nodeTypes):
    for item in nodeTypes:
        for x in tx.run("SHOW CONSTRAINTS;"):
            if x is not None:
                if x["labelsOrTypes"] == [item]:
                    y = x["name"]
                    dropQ = "DROP CONSTRAINT " + y
                    print(dropQ," // for ",item)
                    Result = tx.run(dropQ).consume().counters
                    Neo4J_removingConstraint(Result)

                else:
                    print("Database is empty and nothing to dro")

            else:
                print("Database is empty and nothing to drop")


    qTest = f'''
            ######### Testing:#######################################
            SHOW CONSTRAINTS;
            ##############################################################
            '''
    print(qTest)

def intraEntitiesRel(tx, Type1, ID1, Type2,ID2):

    qCreateEntity = f'''
            MATCH (n1:{Type1})  WHERE n1.ID = "{ID1}" 
            MATCH (n2:{Type2})  WHERE n2.ID = "{ID2}" 
            CREATE (n1)-[:INCLUDED]->(n2);
            '''

    qTest = f'''
            ######### Testing:#######################################
            match p= (n1)-[:INCLUDED]->(n2) return p limit 25;
            ##############################################################
            '''


    print(qCreateEntity)
    Result = tx.run(qCreateEntity).consume().counters
    Neo4J_relationship_create(Result)
    print(qTest)



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













def creatingDfFromGraph(driver):
    query1 = f'''     
    MATCH  (p:Patient)<-[:CORR]-(e:Event)-[:CORR]->(a:Admission)-[:INCLUDED]->(m:Multimorbidity) -[:INCLUDED]->(d:Disorder) 
    where p.Category="Absolute" and a.Category="Absolute"  
    RETURN p.ID, e.timestamp, a.ID, m.Name, d.Name
    ;
    '''
    print(query1)
    with driver.session() as session:
        record = session.run(query1).values()
        print("record=", record)
    x=[]
    for item in record:
        #print(item)
        item[1] = item[1].isoformat()
        #print(item)
        x.append(item)

    df = pd.DataFrame(x, columns=['Patient', 'Timestamp', 'Admission', 'Multimorbidity', 'Disorder'])
    return df



def rankingAdm(df):
    columns_order = ['Patient', 'Admission', 'Timestamp']
    new_df = df[columns_order]
    result = new_df.groupby(['Patient', 'Admission'])['Timestamp'].max().reset_index(name='MaxT')
    #print(result)
    result_sorted = result.sort_values(by=['Patient', 'MaxT'])
    #print(result_sorted)
    # Group by 'A1', 'A2', and 'C' and use cumcount to create the 'Row' column

    result_sorted['Row'] = result_sorted.groupby(['Patient']).cumcount('Admission') + 1
    #print(result_sorted)
    fianl_df = result_sorted[['Patient', 'Admission','Row']]
    # Now, df includes the 'Row' column as in Table2
    return fianl_df


def groupingDisorder(df1):
    df3 = df1[['Patient', 'Admission','Disorder']]
    df2 = df3.drop_duplicates()
    #print(df2)
    conditioned_sorted_df = df2.sort_values(by='Disorder')
    result = conditioned_sorted_df.groupby(['Patient', 'Admission'])['Disorder'].agg(','.join).reset_index(name='Disorders')
    return result

def lefJoinTable(df1,df2):
    result = pd.merge(df1, df2, left_on='Admission', right_on='Admission', how='left')
    df3 = result[['Patient_x', 'Admission','Row', 'Disorders']]
    df4=df3.copy()
    df4.rename(columns={'Patient_x': 'Patient'}, inplace=True)

    return df4


import ast

def comparing(df1):
    df2=df1.copy()
    df13=df1.copy()
    df2['Row'] = df2['Row'] - 1
    df13['Row'] = df13['Row'] + 1
    result = pd.merge(df1, df2, left_on=['Patient', 'Row'], right_on=['Patient', 'Row'], how='left')
    df3=result.copy()
    df3 = df3[['Patient', 'Admission_x','Row', 'Disorders_x', 'Disorders_y']]
    df4=df3.copy()
    df4.rename(columns={'Admission_x': 'Admission','Disorders_x': 'Disorders','Disorders_y': 'Next', }, inplace=True)
    df5 = pd.merge(df4, df13, left_on=['Patient', 'Row'], right_on=['Patient', 'Row'], how='left')
    df5.rename(columns={'Admission_x': 'Admission','Disorders_y': 'Previous' ,'Disorders_x': 'Disorders' }, inplace=True)
    df6=df5.copy()
    df6 = df6[['Patient', 'Admission','Row', 'Disorders', 'Next', 'Previous']]
    df6.fillna('UNK', inplace=True)
    df6['Disorders'] = df6['Disorders'].str.split(',')
    df6['Next'] = df6['Next'].str.split(',')
    df6['Previous'] = df6['Previous'].str.split(',')
    return df6

def createFinal(df):
    df['Treated'] = df.apply(lambda x: [item for item in x['Disorders'] if item not in x['Next']], axis=1)
    df['New'] = df.apply(lambda x: [item for item in x['Disorders'] if item not in x['Previous']], axis=1)
    df['notTreated'] = df.apply(lambda x: [item for item in x['Next'] if item in x['Disorders']], axis=1)
    df.loc[df['Previous'].apply(lambda x: 'UNK' in x), 'New'] = 'UNK'
    df.loc[df['Next'].apply(lambda x: 'UNK' in x), 'Treated'] = 'UNK'
    df.loc[df['Next'].apply(lambda x: 'UNK' in x), 'notTreated'] = 'UNK'
    df = df.map(lambda x: 'Nothing' if x == [] else x)
    df2=df.copy()
    df2 = df2[['Patient', 'Admission','Row', 'Treated', 'New', 'notTreated']]
    #print(df.to_string())
    return df2

def createTreated(df,value):
    df2=df.copy()
    df2 = df2[['Patient', 'Admission', value]]
    df_exploded = df2.explode(value)
    df3=df_exploded.copy()
    print(f'''\ndf{value}=\n''', df3)
    df3 = df3[[ 'Admission', value]]
    list_of_lists = df3.values.tolist()
    return list_of_lists

def multiDis(df):
    df2=df.copy()
    df3 = df2[['Admission', "Disorders"]]
    list_of_lists = df3.values.tolist()
    return list_of_lists


def treatVal(df):
    df['Treated'] = df['Treated'].apply(lambda x: x if isinstance(x, list) else [x])
    df2=df.copy()
    df3 = df2[['Admission', "Treated"]]
    list_of_lists = df3.values.tolist()
    treatedValue = [[x[0], ','.join(x[1])] for x in list_of_lists]
    return treatedValue


def notVal(df):
    df['New'] = df['New'].apply(lambda x: x if isinstance(x, list) else [x])
    df2=df.copy()
    df3 = df2[['Admission', "New"]]
    list_of_lists = df3.values.tolist()
    treatedValue = [[x[0], ','.join(x[1])] for x in list_of_lists]
    return treatedValue

def newVal(df):
    df['notTreated'] = df['notTreated'].apply(lambda x: x if isinstance(x, list) else [x])
    df2=df.copy()
    df3 = df2[['Admission', "notTreated"]]
    list_of_lists = df3.values.tolist()
    treatedValue = [[x[0], ','.join(x[1])] for x in list_of_lists]
    return treatedValue


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


def clearConstraint(tx, x, driver,nodeTypes):
    for item in nodeTypes:
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


def admTreated_Fun(tx, admID, disorderName):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)




def admNotTreated_Fun(tx,  admID, disorderName):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)



def admNew_Fun(tx,  admID, disorderName):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            CREATE (t)-[:INCLUDED {{Type:"last"}}]->(d);
            '''

    qTesting = f'''
            #########Testing:#######################################
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) where a.ID="{admID}"
            MATCH (d:Disorder) where d.Name="{disorderName}"
            RETURN (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids)-[:INCLUDED]->(d);
            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)




def admMulti_Value(tx, admID, value):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)
            Where a.ID="{admID}"
            SET m.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)



def admTreated_Value(tx, admID, value):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:treatedMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"

            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)




def admNotTreated_Value(tx,  admID, value):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:untreatedMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)





def admNew_Value(tx,  admID, value):
    qLinkSCTs = f'''           
            MATCH  (a:Admission)-[:INCLUDED]->(m:Multimorbidity)-[:INCLUDED]->(t:newMorbids) 
            where a.ID="{admID}"
            SET t.Value = "{value}"
            '''

    qTesting = f'''
            #########Testing:#######################################

            ##############################################################
            '''
    print(qLinkSCTs)
    Result = tx.run(qLinkSCTs).consume().counters
    Neo4J_properties_set(Result)
    print(qTesting)

