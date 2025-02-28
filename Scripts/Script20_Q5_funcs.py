from itertools import chain, permutations
import itertools
import re
import copy
from graphviz import Digraph
#############Intoductory Functions##################################################

Type5_Count = 0

Type5_Rel_1_DF_Show_selection = False  # True if we wamt to select specific ID, False if we wamt to show all the table
Type5_Rel_1_DF_Show_selection_ID_instances = ["43", "2"]  # Works if True # also [47,50]

Type5_Rel_2_DF_Show_selection = False #Q02  # True if we wamt to select specific ID, False if we wamt to show all the table
Type5_Rel_2_DF_Show_selection_ID_instances = ["1","5"] #Q02 # Works if True # also [47,50]


def Finading_Entities_ID(driver,entityList,entityListIDproperty,conditionProperty,conditionPropertyValue):

    listFinal=[]
    for i in range(len(entityList)):
        EnityName=entityList[i]
        id = entityListIDproperty[i]
        pro = conditionProperty[i]
        Value = conditionPropertyValue[i]


        #print(EnityName)
        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where  n.{pro}="{Value}"
        return distinct n.{id}
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


def Finading_Entities_ID_2(driver,Entity2):
    listFinal=[]
    # print(EnityName)
    query1 = f'''     
            MATCH p=(e)-[:CORR]->(n:{Entity2})
            return distinct n.ID
            '''
    print(query1)
    with driver.session() as session:
        record1 = session.run(query1).values()
        # print("record1=", record1)
        flat_list = [item for sublist in record1 for item in sublist]
        # print("flat_list=", flat_list)
    listFinal.append(flat_list)

    #print(listFinal)
    return listFinal

def relationship_rel(Entity1,Entity2,entityIDlists):
    list1=[Entity1]
    list2=[Entity2]
    list3=list1+list2+entityIDlists
    return list3

def convert_to_list_of_lists(input_list):
    return [[item] for item in input_list]


def EntityOriginValue_Temp(EntityOrgValue, EnNum):
    flat_list = [item for sublist in EntityOrgValue for item in sublist]
    list1 = []
    for i in range(EnNum):
        list1.append(flat_list)

    return list1


def Finading_Reified_Entities_ID2(driver, col1):
    listFinal = []
    for i in range(len(col1)):
        EnityName = col1[i]
        # print(EnityName)

        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where  n.Category="Relative" 
        return distinct n.ID
        order by n.ID
        '''

        print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()
            # print("record1=", record1)
            flat_list = [item for sublist in record1 for item in sublist]
            # print("flat_list=", flat_list)

        listFinal.append(flat_list)

    # print(listFinal)
    return listFinal


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


def two_pair_permutations(myList):
    result = list(chain.from_iterable([permutations(myList, x) for x in range(len(myList) + 1)]))
    result = [list(k) for k in result]

    final = []
    for i in result:
        if len(i) == 2:
            final.append(i)

    return final


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



def final_DFG_List_func_3(myListA):
    myList1 = copy.deepcopy(myListA)
    # print("myList1=",myList1)
    # print("len(myList1)=", len(myList1))

    if myList1:
        myList = []
        for i in range(len(myList1[2])):
            listTemp=[]
            k1 = myList1[0]
            k2 = myList1[1]
            k3 = myList1[2][i]
            k4 = i
            listTemp.append(k1)
            listTemp.append(k2)
            listTemp.append(k3)
            listTemp.append(k4)
            myList.append(listTemp)

    else:
        myList = []

    return myList

#############Intoductory Functions##################################################


def Entity_DF_Show(RelNum, input):
    List1 = []
    for x in range(1, RelNum + 1):
        # print(x)
        moduleVaribale = 'Type5_Rel_' + str(x) + '_DF_Show'
        #print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
        else:
            moduleVaribale2 = 0
            #print(moduleVaribale2)
        List1.append(moduleVaribale2)
    # print(dicEntity)
    return List1

def case_Selector1(cases):
    txt = " df.En1_ID IN "
    case_selector = txt + str(cases)
    return case_selector, cases

def case_Selector2(cases):
    txt = " df.En2_ID IN "
    case_selector = txt + str(cases)
    return case_selector, cases

###################Graph Functions"""""""""""""""""""""""""""""""""""

def DFC_based_on_Origins (EntityOrgValue, ID_Colors, count, c_white, c_black, dot, driver,activityScenario, colTitle,case_selector_activation1,case_selector_list1,case_selector1,case_selector_activation2,case_selector_list2,case_selector2 ):
    for model_entities in EntityOrgValue:
        # print(model_entities)
        # print(color)
        En1 = model_entities[0]
        En2 = model_entities[1]
        En1_ID = model_entities[2]

        Color_ID = model_entities[3]
        # print(Color_ID)
        color = ID_Colors[Color_ID]

        number = count

        Node_Color = c_white
        Edge_Color = color
        edge_font_color = color

        Node_Around_Color = c_black
        Edge_width = "1"
        show_lifecycle = False

        if activityScenario == "Activity_Label":
            asExpression1 = " "
            asExpression2 = " "
            asExpression3 = " "
            asExpression4 = " "
        if activityScenario == "Concept_Label":
            asExpression1 = "-[:MAPPED_TO]->(m1)"
            asExpression2 = "-[:MAPPED_TO]->(m2)"
            asExpression3 = ",m1,m2"
            asExpression4 = ",m1"

        if case_selector_activation1 == False:
            actExpression1 = " "
        if case_selector_activation1 == True:
            actExpression1 = " AND " + case_selector1

        if case_selector_activation2 == False:
            actExpression2 = " "
        if case_selector_activation2 == True:
            actExpression2 = " AND " + case_selector2

        dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                 fontsize="8", margin="0")


        query1 = f'''     
                MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                match (c1:Activity){asExpression1}
                match (c2:Activity){asExpression2}
                WHERE df.count >= {number} and df.Type = "Relative" and df.En1="{En1}" and df.En2="{En2}" and df.En1_ID="{En1_ID}"  {actExpression1} {actExpression2} 
                return c1,df,c2 {asExpression3}     
                '''


        print(query1)

        # dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
        #        fontsize="8", margin="0")

        with driver.session() as session:
            record1 = session.run(query1).values()
            print(record1)

        if record1:
            c1_Neo4J_ID = [item[0].id for item in record1]
            c2_Neo4J_ID = [item[2].id for item in record1]
            df_En1 = [item[1]["En1"] for item in record1]
            df_En1_ID = [item[1]["En1_ID"] for item in record1]
            df_count = [item[1]["count"] for item in record1]

            if activityScenario == "Activity_Label":
                c1_Activity = [item[0][colTitle] for item in record1]
                c2_Activity = [item[2][colTitle] for item in record1]
            else:
                c1_Activity = [item[3][colTitle] for item in record1]
                c2_Activity = [item[4][colTitle] for item in record1]

            # print("c1_Activity=", c1_Activity)
            # print("c1_Neo4J_ID=", c1_Neo4J_ID)
            # print("c2_Activity=", c2_Activity)
            # print("c2_Neo4J_ID=", c2_Neo4J_ID)
            # print("df_count=", df_count)

            if show_lifecycle:
                c1_lifecycle = [item[1]["lifecycle"] for item in record1]

            for i in range(len(record1)):
                # print(i)

                if show_lifecycle:
                    c1_name = c1_Activity[i] + '\n' + c1_lifecycle[i][0:5]
                else:
                    c1_label = c1_Activity[i].replace(' ', '\n')
                    c2_label = c2_Activity[i].replace(' ', '\n')

                Rel_label = '#' + str(df_count[i])

                pen_width = Edge_width

                # print("c1_label=", c1_label)
                # print("c2_label=", c2_label)
                # print("Rel_label=", Rel_label)

                dot.node(str(c1_Neo4J_ID[i]), c1_label, color=Node_Around_Color, penwidth="2", style="filled",
                         fillcolor=Node_Color)
                dot.node(str(c2_Neo4J_ID[i]), c2_label, color=Node_Around_Color, penwidth="2", style="filled",
                         fillcolor=Node_Color)
                dot.edge(str(c1_Neo4J_ID[i]), str(c2_Neo4J_ID[i]), label=Rel_label, color=Edge_Color,
                         penwidth=pen_width, fontname="Helvetica", fontsize="8",
                         fontcolor=edge_font_color)


def DFC_Adding_Entities (final_DFG_List, count, ID_Colors, c_white, c_black, dot, driver,case_selector_activation1,case_selector_list1,case_selector1,case_selector_activation2,case_selector_list2,case_selector2 ):
    if case_selector_activation2:
        Entity2ID=",".join(case_selector_list2)
        print(Entity2ID)
    else:
        Entity2ID=" "

    list3 = []
    for model_entities in final_DFG_List:
        list2 = []
        En1 = model_entities[0]
        En2 = model_entities[1]
        En1_ID = model_entities[2]
        number = count
        if case_selector_activation1 == False:
            actExpression1 = " "
        if case_selector_activation1 == True:
            actExpression1 = " AND " + case_selector1

        if case_selector_activation2 == False:
            actExpression2 = " "
        if case_selector_activation2 == True:
            actExpression2 = " AND " + case_selector2

        dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                 fontsize="8", margin="0")


        query1 = f'''     
                            MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                            WHERE df.count >= {number} and df.Type = "Relative" and df.En1="{En1}" and df.En2="{En2}" and df.En1_ID="{En1_ID}" {actExpression1} {actExpression2} 
                            return c1,df,c2
                            '''

        print(query1)

        with driver.session() as session:
            record1 = session.run(query1).values()

        if record1:
            df_En1_ID = [item[1]["En1_ID"] for item in record1]
            # print("df_En1_ID=",df_En1_ID)
            list2.extend(df_En1_ID)

        list3.extend(list2)
    list3 = list(dict.fromkeys(list3))


    for model_entities, posit in zip(final_DFG_List, range(1, 700)):
        # print(model_entities)
        # print(color)
        # print("posit=", posit)
        En1 = model_entities[0]
        #print(En1)
        En2 = model_entities[1]
        En1_ID = model_entities[2]
        Color_ID = model_entities[3]
        color = ID_Colors[Color_ID]
        Node_Around_Color = c_white
        Node_Color = color
        Node_fontcolor = c_black
        NodeColorStriped = f"{Node_Around_Color}:{Node_Around_Color}:{Node_Around_Color}:{Node_Color}"

        if En1_ID in list3:
            #print(En1_ID)

            with dot.subgraph(name="cluster_0", comment='name2') as subDot:
                subDot.attr(style='filled', color=Node_Around_Color)
                subDot.attr(label=f"\n#{En2} {Entity2ID} for {En1}:")

                subDot.node_attr.update(shape="rectangle", fixedsize="True", width="0.8", height="0.2",
                                        fontname="Helvetica",
                                        fontsize="8", margin="0")

                if case_selector_activation1 == True:

                    if En1_ID in case_selector_list1:
                        Node_label = f"{En1_ID}        "
                        subDot.node(str(posit + 999), Node_label, color=Node_Around_Color, style="striped",
                                    fillcolor=NodeColorStriped,
                                    fontcolor=Node_fontcolor)


                else:
                    Node_label = f"{En1_ID}        "
                    subDot.node(str(posit + 999), Node_label, color=Node_Around_Color, style="striped",
                                fillcolor=NodeColorStriped,
                                fontcolor=Node_fontcolor)