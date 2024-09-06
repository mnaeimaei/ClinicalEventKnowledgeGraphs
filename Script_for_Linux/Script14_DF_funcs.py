import pandas as pd
import time, os, csv
from itertools import combinations
from itertools import chain, permutations
import itertools
import re
import copy
from neo4j import GraphDatabase
import random
import pandas as pd
import time, csv
from neo4j import GraphDatabase



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

def Neo4J_relationship_delete(result):
    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        property_num = output["relationships_deleted"]
        output_string = f'''
            Deleted {property_num} relationship
        '''
    return print(output_string)


def Neo4J_relationship_and_Node_delete(result):

    output = ast.literal_eval(str(result))
    #print(output)
    if not output:
        output_string = f'''
            (no changes, no records)
        '''
    else:
        rel_del = output["relationships_deleted"]
        node_del = output["nodes_deleted"]
        output_string = f'''
            Deleted {node_del} node, deleted {rel_del} relationship
        '''
    return print(output_string)

def EntityOriginValue_Temp(EntityOrgValue, EnNum):
    flat_list = [item for sublist in EntityOrgValue for item in sublist]
    list1 = []
    for i in range(EnNum):
        list1.append(flat_list)

    return list1





def select_two_random_items(input_list):
    # Check if the list has at least two elements
    if len(input_list) < 2:
        raise ValueError("List must have at least two elements.")

    # Use random.sample() to select two random items from the list
    random_items = random.sample(input_list, 2)

    return random_items





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


def idColumnAndValueMaler(EntityIDColumnList, EntityOriginIDValue, EnNum):
    list2 = []
    for i in range(EnNum):
        list1 = []
        list1.append(EntityIDColumnList[i])
        list1.append(EntityOriginIDValue[i])
        list2.append(list1)

    return list2


def eventListMaker(df3, eventIdTitle):
    EventList = df3[eventIdTitle].tolist()

    def get_sort_key(s):
        m = re.match('e([0-9]+)', s)
        return (int(m.group(1)))

    EventList.sort(key=get_sort_key)

    return EventList


def EntitiesOrgList(EntityOrgValueAbr, EnNum):
    list1 = []
    for k1 in range(EnNum):
        EntityOrgValueAbr1 = EntityOrgValueAbr[k1]
        # print("EntityOrgValueAbr1=",EntityOrgValueAbr1)
        if len(EntityOrgValueAbr1) == 1:
            # print("OK")
            tempList = list(EntityOrgValueAbr1)
            # print(tempList)
        else:
            temp = combinations(EntityOrgValueAbr1, 2)
            tempList = list(temp)
            # print("tempList=", tempList)
        list1.append(tempList)
        # print(list1)
    return list1


def create_adjacent_pairs(input_list,ED_EnNum):
    list1=[]
    for j in range(ED_EnNum):

        myList = copy.deepcopy(input_list)
        myList2=myList[j]
        pair_list = []
        for i in range(len(myList2) - 1):
            pair = (myList2[i], myList2[i + 1])
            pair_list.append(pair)

        unique_list = []
        for item in pair_list:
            if item not in unique_list:
                unique_list.append(item)
        list1.append(unique_list)

    return list1



def create_adjacent_pairs_unique(input_list,ED_EnNum):
    list1=[]
    for j in range(ED_EnNum):

        myList = copy.deepcopy(input_list)
        myList2=myList[j]
        pair_list = []
        for i in range(len(myList2)):
            pair = (myList2[i], myList2[i])
            pair_list.append(pair)

        list1.append(pair_list)

    return list1



def EntitiesOrgList_Dual(EntityOrgValueAbr, EnNum):
    list1 = []
    for k1 in range(EnNum):
        # print(k1)
        EntityOrgValueAbr1 = EntityOrgValueAbr[k1]
        # print("EntityOrgValueAbr1=",EntityOrgValueAbr1)
        if len(EntityOrgValueAbr1) == 1:
            # print("OK")
            tempList = list(EntityOrgValueAbr1)
            # print(tempList)
        else:
            temp = combinations(EntityOrgValueAbr1, 2)
            tempList = list(temp)
            # print("tempList=", tempList)
            tempList_2 = [t[::-1] for t in tempList]
            # print("tempList_2=", tempList_2)
            tempList.extend(tempList_2)
            # print("tempList=", tempList)

        list1.append(tempList)
        # print(list1)
    return list1


def pair_memebr2(EntitiesTypeList, EnNum, dicEntAliasAbr):
    # print("EntitiesTypeList=",EntitiesTypeList)
    # print("EnNum=",EnNum)

    Temp = []
    list1 = []
    for k2 in range(EnNum):
        tempList = EntitiesTypeList[k2]
        stages = []
        for i in tempList:
            a = list(i[0:1])
            b = list(i[1:2])
            a = [str(i) for i in a]
            b = [str(i) for i in b]
            # print("a=",a)
            # print("b=",b)
            lst_tuple = list(zip(a, b))
            # print("lst_tuple=",lst_tuple)
            stages.extend(lst_tuple)

        # print("stages=",stages)
        def join_tuple_string(strings_tuple) -> str:
            return '_'.join(strings_tuple)

        result = map(join_tuple_string, stages)
        # print("result=",result)
        final1 = list(result)
        # print("final1=",final1)
        final1.sort()
        preFix1 = list(dicEntAliasAbr.values())[k2]
        # print(preFix1)
        model_entities_derived = [preFix1 + "_" + s for s in final1]
        model_entities_derived_temp = [preFix1 + "_" + s for s in final1]
        list1.extend(model_entities_derived)
        Temp.append(model_entities_derived_temp)

    return list1, Temp


def model_relations(model_entities_derived_Temp, EntityOrgValue, EnNum, dicEnt, dicEntAliasAbr):
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    list2 = []
    for s in range(EnNum):
        #print("s=", s)
        EntityOrgVal = EntityOrgValue[s]
        Ent = list(dicEnt.values())[s]
        Alias = list(dicEntAliasAbr.values())[s]

        # print("EntityOrgValue=",EntityOrgValue)
        # print("EnID=",EnID)
        # print("model_entities_derived=",model_entities_derived_Temp)

        list1 = []
        for x in model_entities_derived_Temp[s]:
            #print("x=",x)
            d1 = re.search('_(.*)_', x)
            d1 = d1.group(1)
            #print(d1)
            d2 = re.search('_(.*)', x)
            d2 = d2.group(1)
            d2 = d2.split("_", 1)[1]
            #print(d2)
            Mix = []
            Mix.append(Alias)
            Mix.append(x)
            Mix.append(d1)
            Mix.append(d2)
            Mix.append(Ent)
            list1.append(Mix)
        list2.extend(list1)

    return list2



def include_entities(EntityOrgValue, model_entities_derived_Temp, EnNum):
    include_entities = []
    for i in range(EnNum):
        for j in EntityOrgValue[i]:
            include_entities.append(j)
    for i in range(EnNum):
        for j in model_entities_derived_Temp[i]:
            include_entities.append(j)
    return include_entities


def include_DF1(EntityOrgValue, model_entities_derived_Temp, EnNum):
    myList1 = copy.deepcopy(EntityOrgValue)
    myList2 = [[item[0], item[0]] for item in myList1]
    return myList2


def include_DF2(EntityOrgValue, model_entities_derived_Temp, EnNum):
    myList1 = copy.deepcopy(EntityOrgValue)
    myList3 = copy.deepcopy(model_entities_derived_Temp)
    myList2=[]
    for i,j in zip(myList1,myList3):
        for k in j:
            list2 = []
            list2.append(i[0])
            list2.append(k)
            myList2.append(list2)
    return myList2





def convert_to_list_of_lists(input_list):

    return [[item] for item in input_list]

def Entities_Alias_values (EnNum, EntityOrgValue):
    dicEnt = {}
    for x in range(1, EnNum+1):
        #print(x)
        dicEnt["Entity{0}Alias".format(x)] = EntityOrgValue[x-1][0]
    # print(dicEntity)
    return dicEnt


def EntityIDColumnL(dicEntID, EnNum):
    l1=[]
    for i in range(EnNum):
        #print(i)
        x = list(dicEntID.values())[i]
        #print(x)
        l1.append(x)
    return l1



def Finading_Entities_ID(driver,col1,entityListIDproperty, conditionProperty, conditionPropertyValue):
    listFinal=[]
    for i in range(len(col1)):
        EnityName=col1[i]
        idCol=entityListIDproperty[i]
        prop=conditionProperty[i]
        val = conditionPropertyValue[i]
        #print(EnityName)

        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where n.{prop}="{val}"
        return distinct n.{idCol}
        '''

        print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)

        listFinal.append(flat_list)

    #print(listFinal)
    return listFinal




def Finading_ID_List(driver,col1,IDcol, conditionProperty, conditionPropertyValue):
    listFinal=[]
    for i in range(len(col1)):
        EnityName=col1[i]
        id=IDcol[i]
        prop = conditionProperty[i]
        val = conditionPropertyValue[i]
        #print(EnityName)

        query1 = f'''     
        MATCH (e)-[r:CORR]->(n:{EnityName})
        where n.{prop}="{val}"
        with n.{id} AS id, e.timestamp as time
        order by time
        WITH id, time
        RETURN id
        '''

        #print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)
            result = []
            for i, item in zip(range(len(flat_list)), flat_list):
                #print(item, "   ", i)
                if i==0:
                    result.append(item)
                if i!=0:
                    if item!=flat_list[i-1]:
                        result.append(item)
                        #print(item)
            #print(result)

        listFinal.append(result)

    #print(listFinal)
    return listFinal



def Finading_ID_ListUnique(driver,col1,Type_selection,Type_selection_ID,Type_selection_ID_instances,entityListIDproperty, conditionProperty, conditionPropertyValue):
    listFinal=[]
    for i in range(len(col1)):
        EnityName=col1[i]
        id=entityListIDproperty[i]
        prop = conditionProperty[i]
        val = conditionPropertyValue[i]

        query1 = f'''     
        MATCH (e)-[r:CORR]->(n:{EnityName})
        where n.{prop}="{val}"
        RETURN distinct n.{id}
        '''

        #print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            #print("flat_list=", flat_list)


        listFinal.append(flat_list)

    #print(listFinal)
    return listFinal

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

def EntityAndEntityOrg_CreaterAG(dicEntOrigin, EntityOriginIDValue, dicEntID, EnNum, dicEnt):
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    list6 = []
    for k1 in range(EnNum):
        EntityOrgValue = EntityOriginIDValue[k1]
        EntityOrgCol = list(dicEntOrigin.values())[k1]
        EntityIDCol = list(dicEntID.values())[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        # print("EntityOrgValue=",EntityOrgValue)
        # print("EntityOrgCol=",EntityOrgCol)
        # print("EntityIDCol=",EntityIDCol)
        # print("EntityEntCol=", EntityEntCol)

        list5 = []
        # print("list5=",list5)
        for i in EntityOrgValue:
            list4 = []
            # print("i=",i)
            # list4.append(i)
            list4.append(EntityEntCol)
            # print("list4=",list4)
            txt = f"{EntityOrgCol}"
            # print("txt=", txt)
            list4.append(txt)
            list4.append(EntityIDCol)
            # print("list4=",list4)
            list5.append(list4)
        # print("list5=", list5)

        list6.append(list5)

    return list6


def model_entities_AG(EntityAndEntityOrg, EnNum):
    model_entities = []
    for i in range(EnNum):
        model_entities.extend(EntityAndEntityOrg[i])
    return model_entities


def removeDuplicateLists(myListA):
    myList = copy.deepcopy(myListA)
    myList.sort()
    myList2 = list(k for k, _ in itertools.groupby(myList))
    return myList2


def pair_memebr3(EntitiesTypeList, EnNum,dicEntAliasAbr):
    #print("EntitiesTypeList=",EntitiesTypeList)
    #print("EnNum=",EnNum)

    Temp=[]
    list1=[]
    for k2 in range(EnNum):
        tempList=EntitiesTypeList[k2]
        stages = []
        for i in tempList:
            a = list(i[0:1])
            b = list(i[1:2])
            a = [str(i) for i in a]
            b = [str(i) for i in b]
            #print("a=",a)
            #print("b=",b)
            lst_tuple = list(zip(a, b))
            #print("lst_tuple=",lst_tuple)
            stages.extend(lst_tuple)

        #print("stages=",stages)
        def join_tuple_string(strings_tuple) -> str:
            return '_'.join(strings_tuple)

        result = map(join_tuple_string, stages)
        #print("result=",result)
        final1 = list(result)
        #print("final1=",final1)
        final1.sort()
        preFix1 = list(dicEntAliasAbr.values())[k2]
        #print(preFix1)
        model_entities_derived = [s for s in final1]
        model_entities_derived_temp = [s for s in final1]

        list1.extend(model_entities_derived)
        Temp.append(model_entities_derived_temp)



    return list1, Temp



def combining_IDs_List_func(EnNum, myListA, myListB):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    myList = []
    for i in range(EnNum):
        k1 = myList1[i]
        k2 = myList2[i]
        k1.extend(k2)
        myList.append(k1)

    return myList


def color_combine(combining_IDs_List):
    k = -1
    list2 = []
    for i in range(len(combining_IDs_List)):
        list1 = []
        for j in range(len(combining_IDs_List[i])):
            list1.append(k + 1)
            k = k + 1
        list2.append(list1)

    return list2


def entity_basedon_column(EnNum, myListA, myListB):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    myList = []
    for i in range(EnNum):
        k1 = myList1[i]
        k2 = myList2[i]
        k1.extend(k2)
        myList.append(k1)

    return myList


def entity_basedon_column_2(EnNum, myListA, myListB):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    myList = []
    for i in range(EnNum):
        k1 = myList1[i]
        k2 = myList2[i]
        k1.extend(k2)
        myList.append(k1)

    return myList


def final_DFG_List_Absolute(EnNum, myListA, myListB):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    # print("myList1=",myList1)
    # print("myList2=",myList2)
    myList = []
    for i in range(EnNum):
        k1 = myList1[i]
        # print("k1=", k1)
        t1 = myList2[i]
        # print("k3=", t1)
        e3 = list(itertools.product(k1, t1))
        e4 = [list(tup) for tup in e3]
        # print("e4=", e4)
        myList.append(e4)
    return myList


def final_DFG_List_Absolute_2(EnNum, myListA, myListB, myListC):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    myList3 = copy.deepcopy(myListC)
    # print("myList1=",myList1)
    # print("myList2=",myList2)
    myList = []
    for i in range(EnNum):
        k1 = myList1[i]
        # print("k1=", k1)
        t1 = myList2[i]
        # print("k3=", t1)
        s1 = myList3[i]
        # print("s1=", s1)
        e3 = list(itertools.product(k1, t1))
        e4 = [list(tup) for tup in e3]
        # print("e4=", e4)
        for f in range(len(e4)):
            e4[f].append(s1[f])
        myList.append(e4)
    return myList


def two_pair_permutations(myList):
    result = list(chain.from_iterable([permutations(myList, x) for x in range(len(myList) + 1)]))
    result = [list(k) for k in result]

    final = []
    for i in result:
        if len(i) == 2:
            final.append(i)

    return final


def combining_two_list(myListA, myListB):
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)

    myList = []
    for i in myList1:
        # print(i)
        for j in myList2:
            # print(j)
            if i[1] == j[0]:
                i.extend(j)
                i.pop(1)
        # print(i)
        myList.append(i)

    return myList


def Final_AG_forListID(En1, En2, idColumnAndValue):
    list2 = []
    list2.append(En1)
    list2.append(En2)
    for i in range(len(idColumnAndValue)):
        if En2 == idColumnAndValue[i][0]:
            list2.append(idColumnAndValue[i][1])
    return list2





def discrete_combination_fun(myListA):
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    myList1 = copy.deepcopy(myListA)
    myList2 = []
    # print(myList1)
    if myList1:
        for i in range(len(myListA)):
            s1 = []
            s2 = []
            s3 = []
            k1 = myList1[i][0]
            k2 = myList1[i][1]
            s1.insert(0, k1)
            s2.insert(0, k2)
            s3.append(s1)
            s3.append(s2)
            myList2.append(s3)
    else:
        myList2 = []

    return myList2


def EntityOrgAG3_Func(myListA, myListB):
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    # print("myList1=",myList1)
    # print("myList2=", myList2)
    listFinal = []
    for i in range(len(myList2)):
        # print("AAAAAAAAAAAAAAAAAAAAa")
        item1myList2 = myList2[i][0][0]
        # print("item1myList2=", item1myList2)
        for j in range(len(myList1)):
            item1myList1 = myList1[j][0]
            # print("item1myList1=", item1myList1)
            if item1myList2 == item1myList1:
                # print("yes")
                myList2[i].append(myList1[j][1:])
                # print(myList2[i])
        listFinal.append(myList2[i])

    return listFinal


def EntityOrgAG3_Func_2(myListA, myListB, myListC):
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    myList1 = copy.deepcopy(myListA)
    myList2 = copy.deepcopy(myListB)
    myList3 = copy.deepcopy(myListC)
    # print("myList1=",myList1)
    # print("myList2=", myList2)
    listFinal = []
    for i in range(len(myList2)):
        # print("AAAAAAAAAAAAAAAAAAAAa")
        item1myList2 = myList2[i][0][0]
        # print("item1myList2=", item1myList2)
        for j in range(len(myList1)):
            item1myList1 = myList1[j][0]
            item1myList3 = myList3[j][0]
            # print("item1myList1=", item1myList1)
            if item1myList2 == item1myList1:
                # print("yes")
                myList2[i].append(myList1[j][1:])
                # print(myList2[i])
            if item1myList2 == item1myList3:
                # print("yes")
                myList2[i].append(myList3[j][1:])
                # print(myList2[i])

        listFinal.append(myList2[i])

    return listFinal


def final_DFG_List_func(myListA):
    myList1 = copy.deepcopy(myListA)
    # print("myList1=",myList1)
    # print("len(myList1)=", len(myList1))

    if myList1:
        myList = []
        for i in range(len(myList1)):
            k1 = myList1[i][0]
            k2 = myList1[i][1]
            t1 = myList1[i][2]
            # print("k1=", k1)
            # print("k2=", k2)
            # print("t3=", t1)
            e3 = list(itertools.product(k1, k2, t1))
            e4 = [list(tup) for tup in e3]
            # print("e4=", e4)
            myList.append(e4)
    else:
        myList = []

    return myList


def final_DFG_List_func_2(myListA):
    myList1 = copy.deepcopy(myListA)
    # print("myList1=",myList1)
    # print("len(myList1)=", len(myList1))

    if myList1:
        myList = []
        for i in range(len(myList1)):
            k1 = myList1[i][0]
            k2 = myList1[i][1]
            t1 = myList1[i][2]
            s1 = myList1[i][3]

            # print("k1=", k1)
            # print("k2=", k2)
            # print("t3=", t1)
            e3 = list(itertools.product(k1, k2, t1))
            e4 = [list(tup) for tup in e3]
            # print("e4=", e4)

            for f in range(len(e4)):
                e4[f].append(s1[f])

            myList.append(e4)
    else:
        myList = []

    return myList



def final_DFG_6( model_relations, Type6_Rel_1_DF_Show , Type6_Rel_2_DF_Show):
    myList1 = copy.deepcopy(model_relations)
    en1=Type6_Rel_1_DF_Show
    en2=Type6_Rel_2_DF_Show
    list2=[]
    for i in range(len(myList1)):
        list1=[]
        k1 = myList1[i][0]
        k2 = myList1[i][1]
        k3 = myList1[i][2]
        k4 = myList1[i][3]
        k5 = myList1[i][4]
        if en1 == k5:
            list1.append(k2)
            list1.append(k3)
            list1.append(k4)
            list1.append(k5)
            list1.append(en2)
            list2.append(list1)
    return list2


def final_DFG_7( model_relations, Type7_Rel_1_DF_Show , Type7_Rel_2_DF_Show,Type7_Rel_1_Node):
    myList1 = copy.deepcopy(model_relations)
    en1=Type7_Rel_1_DF_Show
    en2=Type7_Rel_2_DF_Show
    en3=Type7_Rel_1_Node
    list2=[]
    for i in range(len(myList1)):
        list1=[]
        k1 = myList1[i][0]
        k2 = myList1[i][1]
        k3 = myList1[i][2]
        k4 = myList1[i][3]
        k5 = myList1[i][4]
        if en1 == k5:
            list1.append(k2)
            list1.append(k3)
            list1.append(k4)
            list1.append(k5)
            list1.append(en2)
            list1.append(en3)
            list2.append(list1)
    return list2


def final_DFG_property_7(A, A1 ,A2 ,A3):
    original_list=[A, A1 ,A2 ,A3]
    result_list = [[original_list[0], item] for item in original_list[1:]]
    return result_list






def final_DFG_extend( lsit1, list2 ):
    myList1 = copy.deepcopy(lsit1)
    myList2 = copy.deepcopy(list2)

    myList1.extend(myList2)
    return myList1


def final_DFG_8( model_relations, Type8_Rel_1_DF_Show , Type8_Rel_2_DF_Show,Type8_Rel_3_DF_Show):
    myList1 = copy.deepcopy(model_relations)
    en1=Type8_Rel_1_DF_Show
    en2=Type8_Rel_2_DF_Show
    en3=Type8_Rel_3_DF_Show
    #print("en1=", en1)
    #print("en2=", en2)
    #print("en3=", en3)


    # print("len(myList1)=", len(myList1))
    list2=[]
    for i in range(len(myList1)):
        list1=[]
        k1 = myList1[i][0]
        k2 = myList1[i][1]
        k3 = myList1[i][2]
        k4 = myList1[i][3]
        k5 = myList1[i][4]

        # print("k1=", k1)
        # print("k2=", k2)
        # print("t3=", t1)
        if en2 == k5:
            list1.append(k1)
            list1.append(k2)
            list1.append(k3)
            list1.append(k4)
            list1.append(en1)
            list1.append(en2)
            list1.append(en3)
            list2.append(list1)
    return list2


def create_string_list(Type8_Rel_1_DF_Show, Type8_Rel_2_DF_Show, Type8_Rel_3_DF_Show):
    string_list = [Type8_Rel_1_DF_Show, Type8_Rel_2_DF_Show, Type8_Rel_3_DF_Show]
    return string_list

def EntityAndEntityOrg_Creater_Final(EntityOriginIDValue, EnNum, dicEnt,dicEntAliasAbr):
    #print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    list6=[]
    for k1 in range(EnNum):
        EntityOrgValue=EntityOriginIDValue[k1]
        EntityEntCol = list(dicEnt.values())[k1]
        EntityAliasCol = list(dicEntAliasAbr.values())[k1]
        list5 = []

        for i in EntityOrgValue:
            list4=[]
            list4.append(EntityAliasCol)
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

import re



def node1Finder(driver):
    query1 = f'''     
    MATCH (n)-[:INCLUDED]->(z)
    WHERE NOT EXISTS {{ (x)-[:INCLUDED]->(n) }} and n.Category="Absolute"
    RETURN  COLLECT (distinct LABELS(n))
    '''
    #print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        #print("record1=", record1)
        flat_list = record1[0][0]
        flattened_list = [label for sublist in flat_list for label in sublist]

    return flattened_list



def nodeLastFinder(driver):
    query1 = f'''     
    MATCH (n)-[:INCLUDED]->(z)
    WHERE NOT EXISTS {{ (z)-[:INCLUDED]->(y) }} and z.Category="Absolute"
    RETURN  COLLECT (distinct LABELS(z))
    '''
    #print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        #print("record1=", record1)
        flat_list = record1[0][0]
        flattened_list = [label for sublist in flat_list for label in sublist]

    return flattened_list



def otherLastFinder(driver):
    query1 = f'''     
    MATCH (n)-[:INCLUDED]->(z)
    WHERE NOT EXISTS {{ (x)-[:INCLUDED]->(n) }} and n.Category="Absolute"
    RETURN  COLLECT (distinct LABELS(n))
    '''
    #print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        #print("record1=", record1)
        flat_list = record1[0][0]
        flattened_list = [label for sublist in flat_list for label in sublist]



    query2 = f'''     
    MATCH (n)-[:INCLUDED]->(z)
    WHERE NOT EXISTS {{ (z)-[:INCLUDED]->(y) }} and z.Category="Absolute"
    RETURN  COLLECT (distinct LABELS(z))
    '''
    #print(query1)
    with driver.session() as session:
        record2 = session.run(query2).values()
        #print("record1=", record1)
        flat_list2 = record2[0][0]
        flattened_list2 = [label for sublist in flat_list2 for label in sublist]




    query3 = f'''     
    MATCH (n)-[:INCLUDED]->(z)
    WHERE z.Category="Absolute" and n.Category="Absolute"
    RETURN COLLECT(DISTINCT LABELS(z)) + COLLECT(DISTINCT LABELS(n)) AS combinedLabels
    '''
    #print(query1)
    with driver.session() as session:
        record3 = session.run(query3).values()
        #print("record3=", record3)
        flat_list3 = record3[0][0]
        flattened_list3 = [label for sublist in flat_list3 for label in sublist]
        no_duplicates_list = list(set(flattened_list3))

    #print(no_duplicates_list)

    result1 = [item for item in no_duplicates_list if item not in flattened_list]
    result2 = [item for item in result1 if item not in flattened_list2]




    return result2




def maxLengthFinder(driver):
    query1 = f'''     
    MATCH path = (start)-[:INCLUDED*]->(end)
    WHERE NOT ()-[:INCLUDED]->(start) AND NOT (end)-[:INCLUDED]->()
    RETURN LENGTH(path) AS maxLength
    ORDER BY maxLength DESC
    LIMIT 1
    '''
    #print(query1)

    with driver.session() as session:
        record1 = session.run(query1).values()
        if record1:
            #print("record1=", record1)
            flat_list = record1[0][0]
        else:
            flat_list = None

    return flat_list

def node2Finder(driver, node1):
    list1=[]
    for item in node1:
        list3=[]
        list2=[item]
        list3.append(list2)
        #print(list3)
        query1 = f'''     
        MATCH (n:{item})-[:INCLUDED]->(z)
        WHERE  z.Category="Absolute"
        RETURN  COLLECT (distinct LABELS(z))
        '''
        #print(query1)
        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = record1[0][0]
            flattened_list = [label for sublist in flat_list for label in sublist]
            list3.append(flattened_list)
            #print(list3)
        list1.append(list3)

    return list1


def node3Finder(driver, node1, nodeLast,maxLength):
    list1=[]
    for item in node1:
        list3=[]
        list2=[item]
        list3.append(list2)
        #print(list3)
        query1 = f'''     
        MATCH (n:{item})-[:INCLUDED]->(z)
        WHERE  z.Category="Absolute"
        RETURN  COLLECT (distinct LABELS(z))
        '''
        #print(query1)
        with driver.session() as session:
            record1 = session.run(query1).values()
            #print("record1=", record1)
            flat_list = record1[0][0]
            flattened_list = [label for sublist in flat_list for label in sublist]
            list3.append(flattened_list)
            #print(list3)
        list1.append(list3)
    #print("list1=",list1)

    for each in list1:
        for item in each[1]:
            #print(item)
            if item not in nodeLast:
                #print("yes")
                list3 = []
                list2 = [item]
                list3.append(list2)
                # print(list3)
                query1 = f'''     
                MATCH (n:{item})-[:INCLUDED]->(z)
                WHERE  z.Category="Absolute"
                RETURN  COLLECT (distinct LABELS(z))
                '''
                # print(query1)
                with driver.session() as session:
                    record1 = session.run(query1).values()
                    # print("record1=", record1)
                    flat_list = record1[0][0]
                    flattened_list = [label for sublist in flat_list for label in sublist]
                    list3.append(flattened_list)
                    # print(list3)

                list1.append(list3)
                #print("list11=",list1)


    return list1



def converted_lister(node):
    converted_nodeOther = [[first, [second]] for first, seconds in node for second in seconds]
    converted_list = [first + second for first, second in converted_nodeOther]

    return converted_list


def pathFinder(node):

    list1=[]
    for x in range(len(node)):
        i=node[x]
        #print(x)
        #print("i=",i)
        list1.append(node[x])
        #print("i[-1]=",i[-1])
        for j in node[x+1:]:
            #print(j)
            #print("j[0]=", j[0])

            if i[-1] == j[0]:
                #print("i=", i)
                #print("j=", j)
                index_of_b = node.index(j)
                #print("index_of_b=",index_of_b)
                j=i[:-1]+j
                #print("newJ=", j)

                node[index_of_b] = j
                #print(node)
    return node


def nodeExcluder(driver):
    query1 = f'''     
    MATCH p=(a)-[r:LINKED_TO]->(b)
    RETURN  COLLECT (distinct LABELS(a))
    '''
    # print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        if record1 != [[[]]]:
            print("record1=", record1)
            flat_list = record1[0][0][0]
        else:
            flat_list = []
    return flat_list


def creatingDfFromGraph(driver, pathNode,excludedNode):
    list1=[]
    for item in pathNode:
        #print(item)
        new = item.copy()
        #print(new)
        n=(len(item))
        #print(n)
        z=item[-1]
        #print(z)
        if z not in excludedNode:
            #print("yes")
            my_list = [f'a{i}' for i in range(1, n + 1)]
            x=my_list[-1]
            if len(my_list)==2: k=my_list[0]
            if len(my_list)!=2: k=my_list[1]
            my_list = [i+":" for i in my_list]

            #print(my_list)
            for i in range(min(len(item), len(my_list))):
                item[i] = "(" + my_list[i] + item[i] + ")"
            #print(item)
            part1=' -[:INCLUDED]- '.join(item)
            part1b= "MATCH (e)-[:CORR]-> " + part1
            part2=f'''RETURN a1.ID AS Patient, {x}.ID AS {z}, e.timestamp AS Time;'''
            #print(part1b)
            #print(part2)


            query1 = f'''     
            {part1b}
            {part2}
            '''
            #print(query1)
            with driver.session() as session:
                record = session.run(query1).values()
                #print("record=", record)
            x=[]
            for each in record:
                #print(each)
                each[2] = each[2].isoformat()
                #print(each)
                x.append(each)

            df = pd.DataFrame(x, columns=['Patient', z, 'Time'])

            #print(df)

            columns_order = ['Patient', z, 'Time']
            new_df = df[columns_order]
            result = new_df.groupby(['Patient', z])['Time'].max().reset_index(name='MaxT')
            #print(result)
            result_sorted = result.sort_values(by=['Patient', 'MaxT'])
            #print(result_sorted)
            # Group by 'A1', 'A2', and 'C' and use cumcount to create the 'Row' column

            result_sorted['Row'] = result_sorted.groupby(['Patient']).cumcount(z) + 1
            #print(result_sorted)
            fianl_df = result_sorted[['Patient', z,'Row']]
            # Now, df includes the 'Row' column as in Table2

            conditioned_sorted_df = fianl_df.sort_values(by=['Patient', 'Row'])
            #print(conditioned_sorted_df)
            result = conditioned_sorted_df.groupby(['Patient'])[z].agg(','.join).reset_index(name=z)
            original_list = result.values.tolist()

            # Convert the original list into the desired format
            converted_list = [
                [[sublist[0]],
                 [[sublist[1].split(',')[i], sublist[1].split(',')[i + 1]] for i in range(len(sublist[1].split(',')) - 1)]]
                for sublist in original_list
            ]

            # Convert the original list into the desired format
            converted_list1 = [[[sublist[0][0]], pair] for sublist in converted_list for pair in sublist[1]]

            updated_list = [[z[0], new, z[1] , [part1b], [k]] for z in converted_list1]
            #print("updated_list=",updated_list)
            if updated_list :
                list1.append(updated_list)
            #print(list1)

    return list1


def txtExistanceQ(FilaeName):
    import os

    ##################################################
    userDirectory = f"../Data/registration/0_username.txt"
    userPath = os.path.realpath(userDirectory)
    with open(userPath, 'r') as file:
        for line in file:
            username = line
    ##################################################

    relDirectory = f"../Data/users/{username}/0_qRelationship"
    confPath = os.path.realpath(relDirectory)
    #print(confPath)

    DFG_FilePath = confPath + "/" + FilaeName + ".txt"

    if os.path.exists(DFG_FilePath):
        return True
    else:
        return False


def TypeCount(FilaeName, searchText):
    import os
    import ast
    ##################################################
    userDirectory = f"../Data/registration/0_username.txt"
    userPath = os.path.realpath(userDirectory)
    with open(userPath, 'r') as file:
        for line in file:
            username = line
    ##################################################

    relDirectory = f"../Data/users/{username}/0_qRelationship"
    confPath = os.path.realpath(relDirectory)
    #print(confPath)

    DFG_FilePath = confPath + "/" + str(FilaeName) + ".txt"

    with open(DFG_FilePath, 'r') as file:
        for line in file:
            if line.startswith(searchText):
                variable_name, value = line.split('=')
                variable_name = variable_name.strip()
                if variable_name == searchText:
                    value = value.strip()
                    value = ast.literal_eval(value)
                    TypeApproach = value
    return TypeApproach



def TrueFalseTxt(FilaeName, searchText):
    import os
    ##################################################
    userDirectory = f"../Data/registration/0_username.txt"
    userPath = os.path.realpath(userDirectory)
    with open(userPath, 'r') as file:
        for line in file:
            username = line
    ##################################################

    relDirectory = f"../Data/users/{username}/0_qRelationship"
    confPath = os.path.realpath(relDirectory)
    #print(confPath)

    DFG_FilePath = confPath + "/" + str(FilaeName) + ".txt"

    with open(DFG_FilePath, 'r') as file:
        for line in file:
            if line.startswith(searchText):
                variable_name, value = line.split('=')
                variable_name = variable_name.strip()
                if variable_name == searchText:
                    value = value.strip()
                    if value.lower() == 'true':
                        value = True
                    elif value.lower() == 'false':
                        value = False
                    case_selector_activation = value
    return case_selector_activation


def listIntTxt(FilaeName, searchText):
    import os
    import ast
    ##################################################
    userDirectory = f"../Data/registration/0_username.txt"
    userPath = os.path.realpath(userDirectory)
    with open(userPath, 'r') as file:
        for line in file:
            username = line
    ##################################################

    relDirectory = f"../Data/users/{username}/0_qRelationship"
    confPath = os.path.realpath(relDirectory)
    #print(confPath)

    DFG_FilePath = confPath + "/" + str(FilaeName) + ".txt"

    with open(DFG_FilePath, 'r') as file:
        for line in file:
            if line.startswith(searchText):
                variable_name, value = line.split('=')
                variable_name = variable_name.strip()
                if variable_name == searchText:
                    value = value.strip()
                    value = ast.literal_eval(value)
                    Type1_selection_ID = value
    return Type1_selection_ID

def stringTxt(FilaeName, searchText):
    import os
    import ast
    ##################################################
    userDirectory = f"../Data/registration/0_username.txt"
    userPath = os.path.realpath(userDirectory)
    with open(userPath, 'r') as file:
        for line in file:
            username = line
    ##################################################

    relDirectory = f"../Data/users/{username}/0_qRelationship"
    confPath = os.path.realpath(relDirectory)
    #print(confPath)

    DFG_FilePath = confPath + "/" + str(FilaeName) + ".txt"

    with open(DFG_FilePath, 'r') as file:
        for line in file:
            if line.startswith(searchText):
                variable_name, value = line.split('=')
                variable_name = variable_name.strip()
                if variable_name == searchText:
                    value = value.strip()
                    Type1_selection_ID = value
    return Type1_selection_ID


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
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)


def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
        print(qDeleteNodes)
        tx.run(qDeleteNodes)


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


################################## Step V2 ###########################################################

def modifyEntities(tx, alias, ID, MainEntity):
    qModifyEntities = f'''
            MATCH (n:{MainEntity}) WHERE n.ID = "{ID}"
            SET
                n.Type= "{MainEntity}",
                n.uID="{alias}"+toString("{ID}")+toString("{ID}")
                '''

    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;


            MATCH (n:{MainEntity}) 
            where n.Type: "{MainEntity}",
            return n;
            ##############################################################
            '''

    print(qModifyEntities)
    Result = tx.run(qModifyEntities).consume().counters
    Neo4J_properties_set(Result)
    print(qTest)


################################## Step V3 ###########################################################


def createReifiedEntities(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qCreateReifiedEntity = f'''

            Create (n:{MainEntity} {{ 
                Category:"Relative" ,          
                uID:"{alias}"+toString("{entityID1}")+toString("{entityID2}")+toString("{entityID1}")+toString("{entityID2}"),
                Type: "{relation_type}",
                ID:toString("{entityID1}")+"_"+toString("{entityID2}") }});'''

    qTest = f'''
            ######### Testing:#######################################
            MATCH (n:{MainEntity}) 
            return n;

            MATCH (n:{MainEntity}) 
            where n.Name: "{relation_type}
            return n;

            MATCH (n:{MainEntity}) 
            where n.Source: "{MainEntity}",
            return n;
            ##############################################################
            '''

    print(qCreateReifiedEntity)
    Result = tx.run(qCreateReifiedEntity).consume().counters
    Neo4J_label_node_property(Result)
    print(qTest)


################################## Step V4 ###########################################################

def entities_with_diff_ID_relationships(tx, Alias, relation_type, entityID1, entityID2, MainEntity):
    qEntities_with_diff_ID_relationships = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:{MainEntity} ) WHERE n1.ID="{entityID1}" AND n1.Type="{MainEntity}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:{MainEntity} ) WHERE n2.ID="{entityID2}" AND n2.Type="{MainEntity}"
            AND n1 <> n2 AND n1.Type=n2.Type
            WITH DISTINCT n1,n2
            CREATE ( n2 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n1 )'''

    qTest = f'''
            ######### Testing:######################################
            MATCH ( n1 : {MainEntity} ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:{MainEntity} )
            return n1,n2;
            ##############################################################
            '''

    print(qEntities_with_diff_ID_relationships)
    Result = tx.run(qEntities_with_diff_ID_relationships).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V5 ###########################################################

def RelatingReifiedEntitiesAndEntities(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qRelatingReifiedEntitiesAndEntities = f'''
            MATCH ( n1 : {MainEntity} ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:{MainEntity} )
            MATCH ( en : {MainEntity} ) where 
            en.uID="{alias}"+toString("{entityID1}")+toString("{entityID2}")+toString("{entityID1}")+toString("{entityID2}")
            AND en.Type= "{relation_type}"
            AND en.ID=toString("{entityID1}")+"_"+toString("{entityID2}") 
            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (en) -[:REL {{Type:"Reified"}}]-> (n2)'''

    qTest = f'''
            ######### Testing:######################################
            MATCH p=(n1:{MainEntity}) <-[:REL]- (r:{MainEntity}) -[:REL]-> (n2:{MainEntity})
            where r.Type="{relation_type}"
            return p;


            MATCH p=(n1:{MainEntity}) <-[:REL]- (r:{MainEntity}) -[:REL]-> (n2:{MainEntity})
            return p;
            ##############################################################
            '''

    print(qRelatingReifiedEntitiesAndEntities)
    Result = tx.run(qRelatingReifiedEntitiesAndEntities).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V6 ###########################################################

def correlate_ReifiedEntities_to_Event(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qCorrelate_ReifiedEntities_to_Event = f'''
            MATCH (e:Event) -[:CORR]-> (n:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            CREATE (e)-[:CORR]->(en)'''

    qTest = f'''
            ######### Testing:######################################
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
            #############################################################
            '''

    print(qCorrelate_ReifiedEntities_to_Event)
    Result = tx.run(qCorrelate_ReifiedEntities_to_Event).consume().counters
    Neo4J_relationship_create(Result)
    print(qTest)


################################## Step V7 ###########################################################

def createDF(tx, Source, Type):
    qCreateDF = f'''
        MATCH ( n : {Source} ) WHERE n.Type="{Type}"
        MATCH ( n ) <-[:CORR]- ( e )
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second, n.ID as NewID
        MERGE ( first ) -[df:DF {{Type:"{Type}",Name:"{Source}", Category:"woProperty"}} ]->( second )
        ON CREATE SET df.ID=NewID 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH  p=(first)-[df:DF]-> (second)
            where df.Type="{Type}"
            return p;
            #############################################################
            '''

    print(qCreateDF)
    Result = tx.run(qCreateDF).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V8 ###########################################################

def deletePuluted_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted1 = f'''
            MATCH (e1:Event)-[df:DF{{Type: "{relation_type}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{Type: "{MainEntity}" }}]-> (e2:Event)
            DELETE df'''

    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
                After:
            MATCH p=(e:Event) -[:CORR]->(n:{MainEntity}) <-[:REL]- (en:{MainEntity})
            where en.Type="{relation_type}"
            return p;
            #############################################################
            '''

    print(qPuluted1)
    Result = tx.run(qPuluted1).consume().counters
    Neo4J_relationship_delete(Result)
    print(qTest)


################################## Step V9 ###########################################################

def deleteWrong_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted1 = f'''
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp  and df3.Type="{relation_type}"
            DELETE df3
            '''

    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp
            return distinct n1.ID,e1.Event,n2.ID,e2.Event,e2.timestamp,n3.ID,e3.Event,e3.timestamp;

                After:
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df2:DF]-> (e2:Event)-[:CORR]->(n2:{MainEntity})
            MATCH (n1:{MainEntity}) <-[:CORR]-(e1:Event) -[df3:DF]-> (e3:Event)-[:CORR]->(n3:{MainEntity})
            where n1.ID="{entityID1}" and n3.ID="{entityID2}" and n2.ID<>n1.ID and n2.ID<>n3.ID  and n2.Category="Absolute"
            and e2.timestamp<=e3.timestamp
            return distinct n1.ID,e1.Event,n2.ID,e2.Event,e2.timestamp,n3.ID,e3.Event,e3.timestamp;
            #############################################################
            '''

    print(qPuluted1)
    Result = tx.run(qPuluted1).consume().counters
    Neo4J_relationship_delete(Result)
    print(qTest)


################################## Step V10 ###########################################################

def deleteExtra_Reified_DF(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted2 = f'''
            MATCH (n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where n1.ID="{entityID2}" and n2.ID="{entityID1}"
            DELETE df;           
            '''

    qTest = f'''
            ######### Testing:######################################
                Before:
            MATCH p=(n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where (n1.ID="{entityID2}" and n2.ID="{entityID1}") or (n1.ID="{entityID1}" and n2.ID="{entityID2}") 
            return p;

                After:
            MATCH p=(n1:{MainEntity})<-[:CORR ]-(e1:Event) -[df:DF {{Type: "{relation_type}" }}]->(e2:Event)-[:CORR ]->(n2:{MainEntity})
            where (n1.ID="{entityID2}" and n2.ID="{entityID1}") or (n1.ID="{entityID1}" and n2.ID="{entityID2}") 
            return p;
            #############################################################
            '''

    print(qPuluted2)
    Result = tx.run(qPuluted2).consume().counters
    Neo4J_relationship_delete(Result)
    print(qTest)


################################## Step V11 ###########################################################

def deletePolluted_CoRR_Reified_Events(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted3 = f'''

            MATCH (e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH (e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            delete c1,c2;

            '''

    qPuluted32 = f'''

            MATCH p=(e1)-[c1:CORR]->(n1) where n1.Type="{relation_type}" 
            MATCH q=(e1) -[df:DF]-> (e2) where df.Type<> "{relation_type}" 
            delete c1;

            '''

    qTest = f'''
            ######### Testing:######################################
              Before:
            MATCH p1=(e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH p2=(e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            return p1,p2

             After:
            MATCH p1=(e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.Type="{relation_type}" 
            MATCH p2=(e1) -[df]-> (e2) where df.Type <> "{relation_type}" 
            return p1,p2

            #############################################################
            '''

    print(qPuluted3)
    Result = tx.run(qPuluted3).consume().counters
    Neo4J_relationship_delete(Result)
    print(qPuluted32)
    Result = tx.run(qPuluted32).consume().counters
    Neo4J_relationship_delete(Result)

    print(qTest)


################################# Step V12 ###########################################################

def deletePolluted_CoRR_Reified_Events_2(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted4 = f'''

            MATCH  (en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )

            CREATE (e1)-[:CORR]->(en)<-[:CORR]-(e2);  '''

    qTest = f'''
            ######### Testing:######################################
              Before:
              MATCH  p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            return p

             After:
            MATCH  p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
            (e1:Event) -[df:DF {{Type: "{relation_type}" }}]->  (e2:Event)   
            -[:CORR]-> (n1:{MainEntity}) <-[:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
            return p


            #############################################################
            '''

    print(qPuluted4)
    Result = tx.run(qPuluted4).consume().counters
    Neo4J_relationship_create(Result)
    print(qTest)


################################## Step V13 ###########################################################

def wrong_reified(tx, alias, relation_type, entityID1, entityID2, MainEntity):
    qPuluted5 = f'''
                OPTIONAL MATCH   (en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n1:{MainEntity}{{ID:"{entityID1}"}})
               -[r:REL ]->(n2:{MainEntity}{{ID:"{entityID2}"}})<-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )

                OPTIONAL MATCH    p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n1:{MainEntity})<-[:CORR]- 
                (e1:Event) -[df:DF ]->   (e2:Event)   
                -[:CORR]-> (n2:{MainEntity}) <-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
                where df.Type="{relation_type}"

                with p,r,r1,r2,en
                where p is null
                delete r,r1,r2,en
                '''

    qTest = f'''
                #########  Testing: ######################################
                Before/After:
                MATCH (n1:{MainEntity}{{ID:"{entityID1}"}}) -[r:REL ]->(n2:{MainEntity}{{ID:"{entityID2}"}})
                MATCH p=(en:{MainEntity} {{Type:"{relation_type}"}}) - [r1:REL {{Type:"Reified"}}]->(n2:{MainEntity})<-[:CORR]- 
                (e2:Event) -[df:DF ]->   (e1:Event)   
                -[:CORR]-> (n1:{MainEntity}) <-[r2:REL {{Type:"Reified"}}]- (en:{MainEntity} {{Type:"{relation_type}"}} )
                where df.Type<>"{relation_type}"
                return p
                #################################################################
                '''

    print(qPuluted5)
    Result = tx.run(qPuluted5).consume().counters
    Neo4J_relationship_and_Node_delete(Result)
    print(qTest)


################################## Step V14 ###########################################################

def aggregateDF_Absolute(tx, Ent, EntIDbased):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Absolute = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{Ent}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type AND n.ID = df.ID  AND n.Type ="{EntIDbased}"  
        WITH n.Type as EType,c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{Type:"Absolute" , count:df_freq , En1_ID:IDT , En2_ID:IDT , En1:"{Ent}" , En2:"{Ent}" , Category:"woProperty"}}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_Absolute)
    Result = tx.run(qCreateDF_Absolute).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V15 ###########################################################

def aggregateDF_Relative(tx, En1, En2, eID):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Relative = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df1:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df2:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n1:{En1}) <-[:CORR]- (e2)
        MATCH (e1) -[:CORR] -> (n2:{En2}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type 
        AND n1.Type = df1.Type  AND n1.ID = df1.ID
        AND n2.Type = df2.Type  AND n2.ID = df2.ID AND n2.ID="{eID}"
        WITH c1,count(df1) AS df_freq,c2, n1.ID as IDT1, n2.ID as IDT2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"Relative" , count:df_freq , En1_ID:IDT1 ,En2_ID:IDT2 , En1:"{En1}" , En2:"{En2}" , Category:"woProperty"}}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            #############################################################
            '''
    print(qCreateDF_Relative)
    Result = tx.run(qCreateDF_Relative).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V16 ###########################################################

def aggregateDF_All(tx, list, firstItem):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def aggregateDF_All_inactiveID(tx, list, firstItem):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def aggregateDF_All_activeID(tx, list, firstItem, entityID):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : Activity ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Activity )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}  and n.ID in {entityID} 
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"All" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"woProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V17 ###########################################################

def relEntity(tx, item0ID, item0, item1, item1ID1, item1ID2):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateAggEntity = f'''
        MATCH (a1:{item1}) where a1.ID="{item1ID1}"
        MATCH (a2:{item1}) where a2.ID="{item1ID2}"
        MERGE ( a1 ) -[:DF_E {{Type:"One" , Base:"{item1}",  Source:"{item0}" , sourceID:"{item0ID}"  }}]-> ( a2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            match p=( a1 ) -[:DF_E {{Type:"One" , Base:"{item1}",  Source:"{item0}"   }}]-> ( a2 )
            return p
            #############################################################
            '''
    print(qCreateAggEntity)
    Result = tx.run(qCreateAggEntity).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V18 ###########################################################

def relEntityLower(tx, item1, item3, index, source, SourceID, id1, id2):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateTwoEn = f'''
        {item3}<-[:DF_E]- (n:{item1})
        WHERE n.ID="{id1}"
        MERGE ( n ) -[:DF_E {{Type:"Two" , Base:"{item1}",  Source:"{source}" , sourceID:"{SourceID}"  }}]-> ( {index} ) ;
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH p1=( n ) -[:DF_E {{Type:"Two" , Base:"{item1}",  Source:"{source}" , sourceID:"{SourceID}"  }}]-> ( {index} )
            RETURN p1 ;
            #############################################################
            '''
    print(qCreateTwoEn)
    Result = tx.run(qCreateTwoEn).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step V19 ###########################################################

def DF_Propery(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(e1)-[d:DF]->(e2)
        MATCH p2=(e1)-[a:Assign]->(f)
        WITH e2,d.ID as ID, d.Name as Name, d.Type as Type ,f
        MERGE ( f ) -[:DF {{ID:ID, Name:Name , Type:Type , Category:"wProperty" }}]-> ( e2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( e2 ) -[:DF ]-> ( f )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step 20 ###########################################################

def aggregateDF_AbsoluteProperty(tx, Ent, EntIDbased):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Absolute = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{Ent}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type AND n.ID = df.ID  AND n.Type ="{EntIDbased}"  
        WITH n.Type as EType,c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AbsoluteProperty" , count:df_freq , En1_ID:IDT , En2_ID:IDT , En1:"{Ent}" , En2:"{Ent}" , Category:"wProperty"}}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_Absolute)
    Result = tx.run(qCreateDF_Absolute).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def DF_AbsolutePropery(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"AbsoluteProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 
            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step 21 ###########################################################

def aggregateDF_RelativeProperty(tx, En1, En2, eID):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_Relative = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df1:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df2:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n1:{En1}) <-[:CORR]- (e2)
        MATCH (e1) -[:CORR] -> (n2:{En2}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type 
        AND n1.Type = df1.Type  AND n1.ID = df1.ID
        AND n2.Type = df2.Type  AND n2.ID = df2.ID AND n2.ID="{eID}"
        WITH c1,count(df1) AS df_freq,c2, n1.ID as IDT1, n2.ID as IDT2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"RelativeProperty" , count:df_freq , En1_ID:IDT1 ,En2_ID:IDT2 , En1:"{En1}" , En2:"{En2}" , Category:"wProperty" }}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            #############################################################
            '''
    print(qCreateDF_Relative)
    Result = tx.run(qCreateDF_Relative).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def DF_RelativePropery(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"RelativeProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 

            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step 22 ###########################################################

def aggregateDF_AllProperty(tx, list, firstItem):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 

        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def aggregateDF_AllProperty_inactiveID(tx, list, firstItem):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def aggregateDF_AllProperty_activeID(tx, list, firstItem, entityID):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH ( c1 : ActivityPropery ) <-[:MONITORED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:MONITORED]-> ( c2 : ActivityPropery )
        MATCH (e1) -[:CORR] -> (n:{firstItem}) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.Type = df.Type  AND n.Type in {list}  and n.ID in {entityID} 
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{Type:"AllProperty" , count:df_freq , En1_ID:"0" ,En2_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}" , Category:"wProperty" }}]-> ( c2 ) 
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;



            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


def DF_AllPropery(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDF_All = f'''
        MATCH p1=(c1)-[d:DF_C {{Type:"AllProperty" }}]->(c2)
        MATCH p2=(c1)<-[r:MONITORED]-(e1)-[a:Assign]->(f)
        WITH 
        c2,
        d.Category  as Category,
        d.En1  as En1,
        d.En1_ID  as En1_ID,
        d.En2  as En2,
        d.En2_ID  as  En2_ID,
        d.Type  as Type,
        d.count  as count,
        f      
        MERGE ( f ) -[:DF_C {{Category:Category , En1:En1 , En1_ID:En1_ID , En2:En2 , En2_ID:En2_ID , Type:Type , count:count }}]-> ( c2 ) 

            '''

    qTest = f'''
            ######### Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;
            #############################################################
            '''
    print(qCreateDF_All)
    Result = tx.run(qCreateDF_All).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


################################## Step Not Used 1 ###########################################################

def aggregate3Entity2(tx, property1, property2, en1, en2, en3, rel, ID_A, ID_B):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateTwoEn_2 = f'''
        MATCH p1=(s1:{en2})<-[:CORR]-(e1:Event)-[:CORR]->(v1:{en1})-[:ATTRIBUTES]->(d1:{en3}) 
        where d1.Name="{property1}" and s1.Category="Absolute"   and v1.Category="Absolute" and v1.ID="{ID_A}"
        MATCH p2=(s1:{en2})<-[:CORR]-(e2:Event)-[:CORR]->(v2:{en1})-[:ATTRIBUTES]->(d2:{en3}) 
        where d2.Name="{property2}"   and v2.ID="{ID_B}"
        WITH   distinct s1.ID as SID , d1 ,d2
        MERGE ( d1 ) -[rel2:DF_E {{Type:"threeEntity" , count:"1" , En1_ID:SID , En1:"{en2}" , En2:"{property2}",  En2_ID:"{rel}" }}]-> ( d2 ) ;
        '''

    qTest = f'''
            ######### Testing:######################################
            MATCH p1=(s1:{en2})<-[:CORR]-(e1:Event)-[:CORR]->(v1:{en1})-[:ATTRIBUTES]->(d1:{en3}) 
            where d1.Name="{property1}" and s1.Category="Absolute"   and v1.Category="Absolute" and v1.ID="{ID_A}"
            MATCH p2=(s1:{en2})<-[:CORR]-(e2:Event)-[:CORR]->(v2:{en1})-[:ATTRIBUTES]->(d2:{en3}) 
            where d2.Name="{property2}"   and v2.ID="{ID_B}"
            return   distinct s1.ID as SID , d1 ,d2;
            #############################################################
            '''
    print(qCreateTwoEn_2)
    Result = tx.run(qCreateTwoEn_2).consume().counters
    Neo4J_relationship_massage(Result)
    print(qTest)


