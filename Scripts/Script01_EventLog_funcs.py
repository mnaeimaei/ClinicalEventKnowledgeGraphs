from datetime import datetime

import pandas as pd
import os, csv
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

def header_csv(csv):
    csvFinal = csv.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csvHeader = csvFinal.columns.tolist()
    return csvHeader, csvFinal


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



# print(df22.to_string())
def Create_CSV_in_Neo4J_import1(csvLog, EntityIDColumnList, ED_Activity_Properties_ID, Timestamp):

    sampleIds = []
    sampleList = []  # create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in csvLog.iterrows():
        if sampleIds == [] :
            rowList = list(row)  # add the event data to rowList
            #print(rowList)
            sampleList.append(rowList)  # add the extended, single row to the sample dataset
            #print(sampleList)

    header = list(csvLog)  # save the updated header data
    #print(header)
    logSamples = pd.DataFrame(sampleList, columns=header)  # create pandas dataframe and add the samples

    logSamples[Timestamp] = pd.to_datetime(logSamples[Timestamp], format='%Y-%m-%d %H:%M:%S')

    logSamples.fillna(0)

    logSamples[Timestamp] = logSamples[Timestamp].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')




    #------converting IDs Columns values to list in dataframe-------
    for i in range(len(EntityIDColumnList)):
        item=EntityIDColumnList[i]
        print("item=",item)
        print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(",") if ("," in str(x) and str(x) != 'nan')
                                                     else (list(x.split(" ")) if ("," not in str(x) and str(x) != 'nan')
                                                     else "Unknown" )
                                                )
        #print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(" ") if (str(x) == 'Unknown') else (x))

        #print(logSamples[item])
    #------converting IDs Columns values to list in dataframe-------

    #------converting Act Properties Columns values to list in dataframe-------
    logSamples[ED_Activity_Properties_ID] = logSamples[ED_Activity_Properties_ID].map(
                                                 lambda x: sorted(x.split(",")) if ("," in str(x) and str(x) != 'nan')
                                                 else (list(x.split(" ")) if ("," not in str(x) and str(x) != 'nan')
                                                 else "Unknown" )
                                            )


    logSamples[ED_Activity_Properties_ID] = logSamples[ED_Activity_Properties_ID].map(lambda x: x.split(" ") if (str(x) == 'Unknown') else (x))
    #------converting IDs Columns values to list in dataframe-------

    return logSamples
    #print(logSamples.to_string())





def removeDecimalInAct(csvlog, colTitle):

    def trim_fraction(text):   #removing Decimal from IDs
        if text != 'nan':
            if '.0' in text:
                x = text[:text.rfind('.0')]
            else:
                x=text
        if text == 'nan':
            x = float('nan')
        return x

    csvlog[colTitle] = csvlog[colTitle].astype(str)
    csvlog[colTitle] = csvlog[colTitle].apply(trim_fraction)
    return csvlog



def removeDecimalInIDs(csvlog, dicEntID, EnNum):

    def trim_fraction(text):   #removing Decimal from IDs
        if text != 'nan':
            if '.0' in text:
                x = text[:text.rfind('.0')]
            else:
                x=text
        if text == 'nan':
            x = float('nan')
        return x

    for k in range(EnNum):
        IDcol= list(dicEntID.values())[k]
        #print("IDcol=",IDcol)
        csvlog[IDcol] = csvlog[IDcol].astype(str)
        #print("csvlog[IDcol]=", csvlog[IDcol])
        csvlog[IDcol] = csvlog[IDcol].apply(trim_fraction)
    return csvlog


def create_csv_with_row(file_path):
    header = ['name', 'start', 'end', 'duration (second)']
    # Check if the file exists and delete it if it does
    if os.path.exists(file_path):
        os.remove(file_path)
    try:
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write the header row
            writer.writerow(header)
        print(f"CSV file '{file_path}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def ImportCSV(inputFileName):

    csvLog = pd.read_csv(os.path.realpath(inputFileName), keep_default_na=True)  # load full log from csv

    csvLog.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates


    return csvLog

def removeDecimalInIDs(csvlog, dicEntID, EnNum):

    def trim_fraction(text):   #removing Decimal from IDs
        if text != 'nan':
            if '.0' in text:
                x = text[:text.rfind('.0')]
            else:
                x=text
        if text == 'nan':
            x = float('nan')
        return x

    for k in range(EnNum):
        IDcol= list(dicEntID.values())[k]
        #print("IDcol=",IDcol)
        csvlog[IDcol] = csvlog[IDcol].astype(str)
        #print("csvlog[IDcol]=", csvlog[IDcol])
        csvlog[IDcol] = csvlog[IDcol].apply(trim_fraction)
    return csvlog


def removeDecimalInAct(csvlog, colTitle):

    def trim_fraction(text):   #removing Decimal from IDs
        if text != 'nan':
            if '.0' in text:
                x = text[:text.rfind('.0')]
            else:
                x=text
        if text == 'nan':
            x = float('nan')
        return x

    csvlog[colTitle] = csvlog[colTitle].astype(str)
    csvlog[colTitle] = csvlog[colTitle].apply(trim_fraction)
    return csvlog



def EntityOriginValue_Temp(EntityOrgValue, EnNum):
    flat_list = [item for sublist in EntityOrgValue for item in sublist]
    list1 = []
    for i in range(EnNum):
        list1.append(flat_list)

    return list1



def NumberEntityOriginAbr(EntityOriginValue_First, EnNum):
    dicNumEntOrgAbr = {}
    for s in range(0, EnNum):
        list4 = EntityOriginValue_First[s]
        if not list4:
            moduleVaribale = float("nan")
        else:
            # findingNumber###################################
            x = len(list4)
            result = max(len(x) for x in list4)
            for s2 in range(result):
                final = s2 + 1
                p = []
                for i in range(x):
                    k = list4[i]
                    d = k[:s2 + 1]
                    p.append(d)
                p = list(dict.fromkeys(p))
                if len(p) == x:
                    break
            # findingNumber####################################

            moduleVaribale = final

        dicNumEntOrgAbr["nEnt{0}Org_Abr".format(s + 1)] = moduleVaribale
    return dicNumEntOrgAbr



def EntityAliasAbr(dicEnt, dicNumEntOrgAbr, EnNum):
    dicEntAliaAbr = {}
    for i in range(EnNum):
        # print(i)
        y1 = f"Entity{i + 1}Alias"
        # print(y)
        x = dicEnt[y1]
        y2 = f"nEnt{i + 1}Org_Abr"
        nEnt_Abr = dicNumEntOrgAbr[y2]
        moduleVaribale = x[0:nEnt_Abr]

        dicEntAliaAbr["nEnt{0}Org_Abr".format(i + 1)] = moduleVaribale
    return dicEntAliaAbr

#Reanmeing column
def ImportCSVRename(csvLog, activityTitle, Timestamp):

    csvLog = csvLog.rename(columns={activityTitle: 'Activity', Timestamp: 'timestamp'})


    activityTitle = 'Activity'
    Timestamp = 'timestamp'
    return csvLog, activityTitle, Timestamp

#Changing column
def Create_CSV_in_Neo4J_import(csvLog, Neo4JImport, outputFileName, EntityIDColumnList, ED_Activity_Properties_ID):
    sampleIds = []
    sampleList = []  # create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in csvLog.iterrows():
        if sampleIds == [] :
            rowList = list(row)  # add the event data to rowList
            #print(rowList)
            sampleList.append(rowList)  # add the extended, single row to the sample dataset
            #print(sampleList)

    header = list(csvLog)  # save the updated header data
    #print(header)
    logSamples = pd.DataFrame(sampleList, columns=header)  # create pandas dataframe and add the samples

    logSamples['timestamp'] = pd.to_datetime(logSamples['timestamp'], format='%Y-%m-%d %H:%M:%S')

    logSamples.fillna(0)

    logSamples['timestamp'] = logSamples['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')




    #------converting IDs Columns values to list in dataframe-------
    for i in range(len(EntityIDColumnList)):
        item=EntityIDColumnList[i]
        print("item=",item)
        print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(",") if ("," in str(x) and str(x) != 'nan')
                                                     else (list(x.split(" ")) if ("," not in str(x) and str(x) != 'nan')
                                                     else "Unknown" )
                                                )
        #print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(" ") if (str(x) == 'Unknown') else (x))

        #print(logSamples[item])
    #------converting IDs Columns values to list in dataframe-------

    #------converting Act Properties Columns values to list in dataframe-------
    logSamples[ED_Activity_Properties_ID] = logSamples[ED_Activity_Properties_ID].map(
                                                 lambda x: sorted(x.split(",")) if ("," in str(x) and str(x) != 'nan')
                                                 else (list(x.split(" ")) if ("," not in str(x) and str(x) != 'nan')
                                                 else "Unknown" )
                                            )


    logSamples[ED_Activity_Properties_ID] = logSamples[ED_Activity_Properties_ID].map(lambda x: x.split(" ") if (str(x) == 'Unknown') else (x))
    #------converting IDs Columns values to list in dataframe-------

    print(Neo4JImport)
    logSamples.to_csv(Neo4JImport + outputFileName, index=True, index_label="idx", na_rep="Unknown")
    #print(logSamples.to_string())



def ImportCSV_Local(Neo4JImport, ED_Input_Event_FileName):
    file_path = os.path.realpath(os.path.join(Neo4JImport, ED_Input_Event_FileName))
    csvLog = pd.read_csv(file_path, keep_default_na=True)
    csvLog = csvLog.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    with open(file_path, 'r') as file:
        header = file.readline().strip().split(',')

    return header, csvLog





def Entities_Alias_values (EnNum, EntityOrgValue):
    dicEnt = {}
    for x in range(1, EnNum+1):
        #print(x)
        dicEnt["Entity{0}Alias".format(x)] = EntityOrgValue[x-1][0]
    # print(dicEntity)
    return dicEnt


def EntityOrigins_values (EnNum, input):
    dicEntOrigin = {}
    for x in range(1, EnNum+1):
        #print(x)
        moduleVaribale = 'Entity' + str(x) + 'Origin'
        #print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
            #print(moduleVaribale2)
            dicEntOrigin["Entity{0}Origin".format(x)] = moduleVaribale2
        else:
            moduleVaribale2 = float("nan")
            #print(moduleVaribale2)
            dicEntOrigin["Entity{0}Origin".format(x)] = moduleVaribale2
    # print(dicEntity)
    return dicEntOrigin





def EntityIDColumn (EnNum, input):
    dicEntID = {}
    for x in range(1, EnNum+1):
        #print(x)
        moduleVaribale = 'Entity' + str(x) + 'ID'
        #print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
            # print(moduleVaribale2)
            dicEntID["Entity{0}ID".format(x)] = moduleVaribale2
        else:
            moduleVaribale2 = float("nan")
            #print(moduleVaribale2)
            dicEntID["Entity{0}ID".format(x)] = moduleVaribale2

    # print(dicEntity)
    return dicEntID









def EntityOriginValue(df,dicEntOrigin,EnNum):
    list1=[]
    for i in range(EnNum):
        #print("SSSSSSSSSSSSSSSSSSSSSSSS")
        x = list(dicEntOrigin.values())[i]
        #print("x=",x)
        l1 = df[x].tolist()
        #print(l1)
        EntityTypeValue1 = list(dict.fromkeys(l1))
        EntityTypeValue = [k for k in EntityTypeValue1 if str(k) != 'nan']
        #print("EntityTypeValue=", EntityTypeValue)
        list1.append(EntityTypeValue)
        #print(list1)
    return list1




def flat_list(original_list):
    new_list = [item[0] if isinstance(item, list) and len(item) == 1 else item for item in original_list]
    return new_list




def EntityOriginIDValue(df,dicEntOrigin,EnNum):
    #print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa")

    list1=[]
    for i in range(EnNum):
        x = list(dicEntOrigin.values())[i]
        #print(" ")
        #print("x=",x)
        l1 = df[x].tolist()
        #print("l1=",l1)
        l2 = []
        for i in range(len(l1)):
            item=l1[i]
            #print("item=", item)
            if "," in str(item) and str(item) != 'nan':

                my_list1 = item.split(",")
                #print("my_list11=",my_list1)
            if "," not in str(item) and str(item) != 'nan':
                my_list1 = [str(int(item))]

                #print("my_list12=", my_list1)
            if str(item) == 'nan' :
                my_list1 = []
            l2.extend(my_list1)
        #print("l2=", l2)

        EntityTypeValue = list(dict.fromkeys(l2))
        #print("EntityTypeValue=", EntityTypeValue)
        if isinstance(EntityTypeValue[0], float):
            #print(type(EntityTypeValue[0]))
            #print("EntityTypeValue=", EntityTypeValue)
            EntityTypeValue = [str(int(float(k))) for k in EntityTypeValue]
        if isinstance(EntityTypeValue[0], str):
            EntityTypeValue = [str(k) for k in EntityTypeValue]
        list1.append(EntityTypeValue)
        #print("EntityTypeValue=", EntityTypeValue)
        #print("list1=",list1)
    #print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa")

    return list1


def EntityOriginIDValue2(df,dicEntOrigin,EnNum):

    #print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa")

    list1=[]
    for i in range(EnNum):
        x = list(dicEntOrigin.values())[i]
        #print(" ")
        #print("x=",x)
        l1 = df[x].tolist()
        #print("l1=",l1)
        l2 = []
        for i in range(len(l1)):
            item=l1[i]
            #print("item=", item)

            #item = ast.literal_eval(item)
            #print("item=", item)
            item = item[0]
            #print("item=", item)
            if "," in str(item) and str(item) != 'nan':

                my_list1 = item.split(",")
                #print("my_list11=",my_list1)
            if "," not in str(item) and str(item) != 'nan':
                my_list1 = [str(int(item))]

                #print("my_list12=", my_list1)
            if str(item) == 'nan' :
                my_list1 = []
            l2.extend(my_list1)
        #print("l2=", l2)

        EntityTypeValue = list(dict.fromkeys(l2))
        #print("EntityTypeValue=", EntityTypeValue)
        if isinstance(EntityTypeValue[0], float):
            #print(type(EntityTypeValue[0]))
            #print("EntityTypeValue=", EntityTypeValue)
            EntityTypeValue = [str(int(float(k))) for k in EntityTypeValue]
        if isinstance(EntityTypeValue[0], str):
            EntityTypeValue = [str(k) for k in EntityTypeValue]
        list1.append(EntityTypeValue)
        #print("EntityTypeValue=", EntityTypeValue)
        #print("list1=",list1)
    #print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa")

    return list1
def activityNode(df, ED_Activity, ED_ActivitySynonym):
    dataF = df
    column_names = [ED_Activity, ED_ActivitySynonym]
    # print(column_names)

    # Get distinct values from the specified columns
    distinct_values = dataF[column_names].drop_duplicates().values.tolist()

    return distinct_values


def activityNodewithID(df, ED_Activity, ED_ActivitySynonym,ED_Activity_Properties_ID):
    dataF = df
    column_names = [ED_Activity, ED_ActivitySynonym,ED_Activity_Properties_ID]
    #(column_names)
    dataF2=dataF[column_names].values.tolist()
    #print("dataF2=",dataF2)


    '''
    #Sorting:
    dataF3 = []
    for item in dataF2:
        numbers_str = item[2].strip("[]").split(', ')
        sorted_numbers = sorted([int(num.strip("'")) for num in numbers_str])
        string_list = [str(num) for num in sorted_numbers]
        new_item = [item[0], item[1], str(string_list)]
        dataF3.append(new_item)
    #print("dataF3=",dataF3)
    '''

    #Distinct
    distinct_items = set()
    dataF4 = []

    for item in dataF2:
        item_str = tuple(item[2])  # Assuming the distinctness is judged based on the third element of each sublist
        if item_str not in distinct_items:
            distinct_items.add(item_str)
            dataF4.append(item)
    #print("dataF4=",dataF4)

    for i in range(len(dataF4)):
        dataF4[i].insert(0, 'ap' + str(i + 1))

    return dataF4


def eventAct_Rel(actNode, ED_Activity, ED_ActivitySynonym):
    list3 = []
    for list1 in actNode:
        list2 = []
        txt1 = f" e.{ED_Activity} =\"{list1[0]}\""
        txt2 = f" e.{ED_ActivitySynonym} =\"{list1[1]}\""
        list2.append(txt1)
        list2.append(txt2)
        list2.append(list1[0])
        list2.append(list1[1])
        list3.append(list2)
    return list3






def EntityAndEntityOrg_Creater(dicEntOrigin,EntityOriginIDValue, dicEntID, EnNum, dicEnt):
    #print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    list6=[]
    for k1 in range(EnNum):
        EntityOrgValue=EntityOriginIDValue[k1]
        EntityOrgCol = list(dicEntOrigin.values())[k1]
        EntityIDCol= list(dicEntID.values())[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        list5 = []

        for i in EntityOrgValue:
            list4=[]
            list4.append(i)
            list4.append(EntityIDCol)
            txt = f"WHERE e.{EntityOrgCol} =\"{EntityEntCol}\""
            list4.append(txt)
            list4.append(EntityEntCol)
            list5.append(list4)
        list6.append(list5)

    return list6


def EntityAndEntityOrg_Creater_Final(dicEntOrigin,EntityOriginIDValue, dicEntID, EnNum, dicEnt):
    #print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    list6=[]
    for k1 in range(EnNum):
        EntityOrgValue=EntityOriginIDValue[k1]
        EntityOrgCol = list(dicEntOrigin.values())[k1]
        EntityIDCol= list(dicEntID.values())[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        list5 = []

        for i in EntityOrgValue:
            list4=[]
            list4.append(i)
            list4.append(EntityIDCol)
            txt = f"WHERE e.{EntityOrgCol} =\"{EntityEntCol}\""
            list4.append(txt)
            list4.append(EntityEntCol)
            list5.append(list4)
        list6.append(list5)

    return list6



def EntityAndEntityOrg_Creater2(EntityOriginIDValue, EnNum, dicEnt):
    list6=[]
    for k1 in range(EnNum):
        EntityOrgValue=EntityOriginIDValue[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        list5 = []
        for i in EntityOrgValue:
            list4=[]
            list4.append(i)
            list4.append(EntityEntCol)
            list5.append(list4)
        list6.append(list5)

    return list6


def EntityAndEntityOrg_Creater3(EntityOriginIDValue, EnNum, dicEnt,dicEntAliasAbr):
    list6=[]
    for k1 in range(EnNum):
        EntityOrgValue=EntityOriginIDValue[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        AbrEntCol = list(dicEntAliasAbr.values())[k1]
        list5 = []
        for i in EntityOrgValue:
            list4=[]
            list4.append(AbrEntCol)
            list4.append(i)
            list4.append(EntityEntCol)
            list5.append(list4)
        list6.append(list5)

    return list6

















def model_entities(EntityAndEntityOrg,EnNum):

    model_entities=[]
    for i in range(EnNum):
        model_entities.extend(EntityAndEntityOrg[i])

    return model_entities









def EntityIDColumnL(dicEntID, EnNum):
    l1=[]
    for i in range(EnNum):
        #print(i)
        x = list(dicEntID.values())[i]
        #print(x)
        l1.append(x)
    return l1



def CreateActivity(csvLog_ED,ED_Activity,ED_ActivitySynonym):
    dataF=csvLog_ED
    column_names = [ED_Activity, ED_ActivitySynonym]
    #print(column_names)

    # Get distinct values from the specified columns
    distinct_values = dataF[column_names].drop_duplicates().values.tolist()

    return distinct_values

#########################################################################
def CreateActivityList(dataset, ct1, ct2, ct3, ct4, ct5, ct6, ct7, ct8):
    list1 = dataset[ct1].values.tolist()
    list2 = dataset[ct2].values.tolist()
    list3 = dataset[ct3].values.tolist()
    list4 = dataset[ct4].values.tolist()
    list5 = dataset[ct5].values.tolist()
    list6 = dataset[ct6].values.tolist()
    list7 = dataset[ct7].values.tolist()
    list8 = dataset[ct8].values.tolist()

    return     list1, list2 ,   list3 ,   list4 ,   list5 ,   list6 ,   list7,   list8

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









###############################################################################################



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



################################## Step C1 ###########################################################


def deleteRelation(tx, relationTypes):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r:{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)



def deleteAllRelations(tx):
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r;"
    print(qDeleteAllRelations)
    tx.run(qDeleteAllRelations)



def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n:{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)



def deleteAllNodes(tx):
    qDeleteAllNodes = "MATCH (n) DELETE n;"
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)



def deleteAllNodesandRel(tx):
    qDeleteAllNodes = "MATCH (n) DETACH DELETE n;"
    print(qDeleteAllNodes)
    tx.run(qDeleteAllNodes)


def deletePartiallyRel(tx, rel, where, val):
    qDeleteRelation = f'''MATCH ( e ) -[r:{rel}]-> ( p ) where r.{where}="{val}" DELETE r;'''
    print(qDeleteRelation)
    tx.run(qDeleteRelation)


################################## Step C2 ###########################################################


def clearConstraint(tx):
    for x in tx.run("SHOW CONSTRAINTS;"):
        if x is not None:
            y = x["name"]
            z = x["labelsOrTypes"]
            #print(y)
            dropQ = "DROP CONSTRAINT " + y
            print(dropQ, " // for ", z[0])
            Result = tx.run(dropQ).consume().counters
            Neo4J_removingConstraint(Result)


        else:
            print("Database is empty and nothing to drop")



    qTest = f'''
            ######### Testing:#######################################
            SHOW CONSTRAINTS;
            ##############################################################
            '''
    print(qTest)


################################## Step C3 ###########################################################

def createConstraint(tx,nodeTypesC):
    for i in range(len(nodeTypesC)):
        queryCreate = f'''CREATE CONSTRAINT FOR (z:{nodeTypesC[i]}) REQUIRE z.ID IS UNIQUE;'''
        print(queryCreate)
        Result = tx.run(queryCreate).consume().counters
        Neo4J_creatingConstraint(Result)


    qTest = f'''
                ######### Testing:#######################################
                :schema'
                ##############################################################
                '''

    print(qTest)



################################## Step C4 ###########################################################


def createLogNode(tx, log_id):
    print("")
    print("Inputs:")
    print("log_id=", log_id)
    print("")
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'


    qTest = f'''
            ######### Testing:#######################################
            MATCH (l:Log) return l;

            MATCH (l:Log) return count(*) as count;
            ##############################################################
            '''
    print(qCreateLog)


    Result = tx.run(qCreateLog).consume().counters
    Neo4J_label_node_property(Result)
    print(qTest)

################################## Step C5 ###########################################################

# create events from CSV table: one event node per row, one property per column
def CreateEventNode(driver, logHeader, fileName, EntityIDColumnList, LogID="" ):
    print("")
    print("Inputs:")
    print("logHeader=", logHeader)
    print("fileName=", fileName)
    print("EntityIDColumnList=", EntityIDColumnList)
    print("LogID=", LogID)
    print("")

    print('Creating Event Nodes from CSV:\n')
    query = f''' LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line 
    CALL {{ 
    with line'''

    for col in logHeader:
        if col == 'idx' or col == 'row':
            column = f'''toInteger(line.{col})'''
        elif col in ['timestamp', 'start', 'end']:
            column = f'''datetime(line.{col})'''
        elif col in EntityIDColumnList or col == 'Activity_Properties_ID':
            column = f'''apoc.convert.fromJsonList(line.{col})'''
        else:
            column = 'line.' + col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f''' 
            CREATE (e:Event {{Log: "{LogID}",
            {col}: {column},'''
        elif (logHeader.index(col) == 0):
            newLine = f''' 
            CREATE (e:Event {{ {col}: {column},'''
        else:
            newLine = f'''
            {col}: {column},'''
        if (logHeader.index(col) == len(logHeader) - 1):
            newLine = f'''
            {col}: {column} '''

        query = query + newLine

    finalQuery = query + f'''
          }})
    }} IN TRANSACTIONS ;
    '''

    testingQ = f'''
            ######### Testing:######################################
            MATCH (e:Event) 
            return e;

            MATCH (e:Event) 
            return count(*) as count;
            #############################################################
            '''

    print(finalQuery)


    with driver.session() as session:
        Result=session.run(finalQuery).consume().counters
        Neo4J_label_node_property(Result)
        print(testingQ)

    #return finalQuery, testingQ

def CreateEventNodeNew(tx, actList, actProIdList, actSynList, actValdList, entityIdList, entityOriginList, eventList, timeList):
    print("")
    print("Inputs:")
    #print("actList=", actList)
    #print("actProIdList=", actProIdList)
    #print("actSynList=", actSynList)
    #print("actValdList=", actValdList)
    #print("entityIdList=", entityIdList)
    #print("entityOriginList=", entityOriginList)
    #print("eventList=", eventList)
    #print("timeList=", timeList)
    #print("")



    #print('Creating Event Nodes:\n')


    newLine1 = f''' Activity: "{actList}",'''
    #print(newLine1)

    newLine2 = f''' Activity_Properties_ID: apoc.convert.fromJsonList("{actProIdList}"), '''
    #print(newLine2)

    newLine3 = f''' Activity_Synonym: "{actSynList}",'''
    #print(newLine3)

    newLine4 = f''' Activity_Value_ID: "{actValdList}",'''
    #print(newLine4)

    newLine5 = f''' '''
    for i in range(len(entityIdList)):
        newLine = f''' Entity{i+1}_ID: apoc.convert.fromJsonList("{entityIdList[i]}")'''
        newLine5 = newLine + "," + newLine5
    #print(newLine5)

    newLine6 = f''' '''
    for i in range(len(entityOriginList)):
        newLine = f''' Entity{i+1}_Origin: "{entityOriginList[i]}"'''
        newLine6 = newLine + "," + newLine6
    #print(newLine6)

    newLine7 = f''' Event: "{eventList}",'''
    #print(newLine7)

    newLine8 = f''' Log: "EventLog",'''
    #print(newLine8)

    newLine9 = f''' timestamp: datetime("{timeList}")'''
    #print(newLine9)

    finalQuery = f'''  CREATE (p:Event {{ ''' + newLine1 + newLine2 +  newLine3 +  newLine4 +  newLine5 +  newLine6 +  newLine7 +   newLine8 +  newLine9 +   f'''}});'''


    testingQ = f'''
            ######### Testing:######################################
            MATCH (e:Event) 
            return e;

            MATCH (e:Event) 
            return count(*) as count;
            #############################################################
            '''

    print(finalQuery)

    tx.run(finalQuery)
    '''
    try:
        tx.run(finalQuery)
        logger.info("Query executed successfully")
    except Exception as e:
        logger.info(f"Failed to execute query: {e}")
    '''

################################## Step C6 ###########################################################

def createEntitiesNode(tx, entity_id, entityCol, entityWhere, MainEntity):
    qCreateEntity = f'''
            MERGE (n:{MainEntity} {{
            EntityCol:"{entityCol}",          
            ID:toString("{entity_id}"),
            Category:"Absolute",
            Value:toString("{entity_id}")
            }});'''
    print(qCreateEntity)


    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;

            MATCH (n:{MainEntity}) 
            where n.EntityCol="{entityCol}"
            return n;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)

################################## Step C7 ###########################################################


def createActivityNode(tx, act, syn):
    qCreateEC = f'''
            MERGE (n:Activity {{
            ID:"{act}", 
            Type:"Activity",
            Name:"{act}",
            Syn: "{syn}"
            }});'''
    print(qCreateEC)


    qTest = f'''
            ######### Testing:######################################
            MATCH (n:Activity) 
            RETURN n 
            #############################################################
            '''
    print(qTest)
    tx.run(qCreateEC)


################################## Step C8 ###########################################################


def createActivityPropertiesNode(tx, id, Name, Syn, featureID):
    qCreateEC = f'''
            MERGE (n:ActivityPropery {{
            ID:"{id}", 
            Type:"ActivityPropery",
            Name:"{Name}",
            Syn: "{Syn}",
            featureID: apoc.convert.fromJsonList("{featureID}")
            }});'''
    print(qCreateEC)


    qTest = f'''
            ######### Testing:######################################
            MATCH (n:ActivityPropery) 
            RETURN n 
            #############################################################
            '''
    print(qTest)
    tx.run(qCreateEC)

################################## Step C9 ###########################################################

def link_log_events(tx, log_id):
    print("")
    print("Inputs:")
    print("log_id=", log_id)
    print("")
    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:HAS]->(e);'''
    print(qLinkEventsToLog)


    qTest = f'''
            ######### Testing:#######################################
            MATCH p=(l)-[:HAS]->(e)
            return p;
            ##############################################################
            '''

    print(qTest)

    tx.run(qLinkEventsToLog)



################################## Step C10 ###########################################################

def link_events_Entities(tx, entity_id, entityCol, entityWhere, MainEntity):
    qCorrelate = f'''
            MATCH (e:Event) {entityWhere} and "{entity_id}" in e.{entityCol}
            MATCH (n:{MainEntity} {{EntityCol: "{entityCol}" }}) WHERE n.ID = "{entity_id}"
            CREATE (e)-[:CORR{{Scenario:"1"}}]->(n)'''
    print(qCorrelate)


    qTest = f''' 
            ######### Testing:#######################################         
            MATCH p=(e)-[:CORR]->(n)
            WHERE n.EntityCol= "{entityCol}"and n.ID="{entity_id}"
            return p;
            ##############################################################
            '''

    print(qTest)
    tx.run(qCorrelate)





################################## Step C11 ###########################################################

def link_events_Activity(tx, cond1, cond2, act, syn):
    qLinkEventToClass = f'''
            MATCH ( c : Activity ) WHERE c.Name ="{act}" and c.Syn="{syn}"
            MATCH ( e : Event ) where {cond1} and {cond2}
            CREATE ( e ) -[:OBSERVED{{Scenario:"1"}}]-> ( c )'''
    print(qLinkEventToClass)


    qTest = f'''
            ######### Testing:######################################
            MATCH l=( e ) -[:OBSERVED]-> ( c ) 
            RETURN l
            #############################################################
            '''
    print(qTest)
    tx.run(qLinkEventToClass)



################################## Step C12 ###########################################################

def link_events_ActivityProperty(tx):
    qLinkEventToClass = f'''
            MATCH ( c : ActivityPropery )
            MATCH ( e : Event ) 
            where e.Activity_Properties_ID = c.featureID and e.Activity=c.Name and e.Activity_Synonym=c.Syn
            CREATE ( e ) -[:MONITORED{{Scenario:"1"}}]-> ( c )'''
    print(qLinkEventToClass)


    qTest = f'''
            ######### Testing:######################################
            MATCH l=( e ) -[:MONITORED]-> ( c ) 
            RETURN l
            #############################################################
            '''
    print(qTest)
    tx.run(qLinkEventToClass)





