import copy
from itertools import chain, permutations
import itertools
from graphviz import Digraph


Type4_Count = 0

Type4_Entity1_DF_Show = 1
Type4_Entity2_DF_Show = 0
Type4_Entity3_DF_Show = 0

Type4_selection = False  # True if we wamt to select specific ID, False if we wamt to show all the table
Type4_selection_ID_instances = ["41","42"]  # Works if True # also [47,50]

#############Intoductory Functions##################################################


def Entity_DF_Show(EnNum, input):
    List1 = []
    for x in range(1, EnNum + 1):
        # print(x)
        moduleVaribale = 'Type4_Entity' + str(x) + '_DF_Show'
        #print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
        else:
            moduleVaribale2 = 0
            #print(moduleVaribale2)
        List1.append(moduleVaribale2)
    # print(dicEntity)
    return List1

def case_Selector(cases):
    txt = " df.En1_ID IN "
    case_selector = txt + str(cases)
    return case_selector, cases



def Finading_Entities_ID(driver,col1,entityListIDproperty,conditionProperty,conditionPropertyValue):
    listFinal=[]
    for i in range(len(col1)):
        EnityName=col1[i]
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

def convert_to_list_of_lists(input_list):

    return [[item] for item in input_list]

def Finading_Reified_Entities_ID2(driver,col1):
    listFinal=[]
    for i in range(len(col1)):
        EnityName=col1[i]
        #print(EnityName)

        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where  n.Category="Relative" 
        return distinct n.ID
        order by n.ID
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

###################Graph Functions"""""""""""""""""""""""""""""""""""

def DFC_based_on_Origins (final_DFG_List, ID_Colors, ListEnDFSHow, count, case_selector, case_selector_activation, c_white, c_black, dot, driver,activityScenario, colTitle):
    for EntityOrgValue_list, Show in zip(final_DFG_List, ListEnDFSHow):
        print("model_entities_derived_list=", EntityOrgValue_list)
        print("Show=", Show)
        if Show == 1:
            for model_entities in EntityOrgValue_list:
                print(model_entities)
                #print(color)
                En1 = model_entities[0]
                En1_ID = model_entities[1]
                Color_ID = model_entities[2]
                color=ID_Colors[Color_ID]

                number=count

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


                if case_selector_activation == False:
                    actExpression1 =  " "
                if case_selector_activation == True:
                    actExpression1 =  " AND " + case_selector


                dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                             fontsize="8", margin="0")


                query1 = f'''     
                        MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                        match (c1:Activity){asExpression1}
                        match (c2:Activity){asExpression2}
                        WHERE df.count >= {number} and df.Type = "Absolute" and df.En1="{En1}" and df.En1_ID="{En1_ID}" {actExpression1} 
                        return c1,df,c2{asExpression3}  
                        '''


                print(query1)



                #dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                 #        fontsize="8", margin="0")

                with driver.session() as session:
                    record1 = session.run(query1).values()



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


                    #print("c1_Activity=", c1_Activity)
                    #print("c1_Neo4J_ID=", c1_Neo4J_ID)
                    #print("c2_Activity=", c2_Activity)
                    #print("c2_Neo4J_ID=", c2_Neo4J_ID)
                    #print("df_count=", df_count)


                    if show_lifecycle:
                        c1_lifecycle = [item[1]["lifecycle"] for item in record1]

                    for i in range(len(record1)):
                        #print(i)

                        if show_lifecycle:
                            c1_name = c1_Activity[i] + '\n' + c1_lifecycle[i][0:5]
                        else:
                            c1_label = c1_Activity[i].replace(' ', '\n')
                            c2_label = c2_Activity[i].replace(' ', '\n')





                        Rel_label = '#' + str(df_count[i])

                        pen_width = Edge_width

                        #print("c1_label=", c1_label)
                        #print("c2_label=", c2_label)
                        #print("Rel_label=", Rel_label)



                        dot.node(str(c1_Neo4J_ID[i]), c1_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.node(str(c2_Neo4J_ID[i]), c2_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.edge(str(c1_Neo4J_ID[i]), str(c2_Neo4J_ID[i]), label=Rel_label, color=Edge_Color,
                                 penwidth=pen_width,  fontname="Helvetica", fontsize="8",
                                 fontcolor=edge_font_color)




def DFC_Adding_Entities (final_DFG_List, ID_Colors, ListEnDFSHow, count, case_selector, case_selector_activation, case_selector_list, c_white, c_black, dot, driver):
    list3 = []
    for EntityOrgValue_list, Show in zip(final_DFG_List, ListEnDFSHow):
        if Show == 1:
            list1 = []
            for model_entities in EntityOrgValue_list:
                list2 = []
                En1 = model_entities[0]
                En1_ID = model_entities[1]
                number=count
                if case_selector_activation == False:
                    actExpression1 =  " "
                if case_selector_activation == True:
                    actExpression1 =  " AND " + case_selector


                query1 = f'''     
                        MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                        WHERE df.count >= {number} and df.Type = "Absolute" and df.En1="{En1}" and df.En1_ID="{En1_ID}"  {actExpression1} 
                        return c1,df,c2
                        '''

                print(query1)

                with driver.session() as session:
                    record1 = session.run(query1).values()


                if record1:
                    df_En1_ID = [item[1]["En1_ID"] for item in record1]
                    list2.extend(df_En1_ID)


                list1.extend(list2)
            list3.extend(list1)
        #print("list3=",list3)
        list3 = list(dict.fromkeys(list3))
        #print("list3=",list3)

    for final_DFG, Show in zip(final_DFG_List, ListEnDFSHow):
        #print("final_DFG=", final_DFG)
        #print("Color=", Color)
        #print("Show=", Show)

        if Show == 1 :
            #print("final_DFG=", final_DFG)
            for model_entities, posit in zip(final_DFG, range(1,700)):
                #print(model_entities)
                #print(color)
                #print("posit=", posit)
                En1 = model_entities[0]
                En1_ID = model_entities[1]
                Color_ID = model_entities[2]
                color=ID_Colors[Color_ID]


                Node_Around_Color = c_white
                Node_Color = color
                Node_fontcolor = c_black
                NodeColorStriped = f"{Node_Around_Color}:{Node_Around_Color}:{Node_Around_Color}:{Node_Color}"

                if En1_ID in list3:
                    #print(En1_ID)
                    with dot.subgraph(name="cluster_0", comment='name2') as subDot:
                        subDot.attr(style='filled', color=Node_Around_Color)
                        subDot.attr(label=f"\n{En1}:")

                        subDot.node_attr.update(shape="rectangle", fixedsize="True", width="0.8", height="0.2",
                                                fontname="Helvetica",
                                                fontsize="8", margin="0")

                        if case_selector_activation == True:

                            if En1_ID in case_selector_list:
                                Node_label = f"{En1_ID}        "
                                subDot.node(str(posit + 999), Node_label, color=Node_Around_Color, style="striped",
                                            fillcolor=NodeColorStriped,
                                            fontcolor=Node_fontcolor)


                        else:
                            Node_label = f"{En1_ID}        "
                            subDot.node(str(posit + 999), Node_label, color=Node_Around_Color, style="striped",
                                        fillcolor=NodeColorStriped,
                                        fontcolor=Node_fontcolor)

