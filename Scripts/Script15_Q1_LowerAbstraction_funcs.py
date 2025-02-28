from graphviz import Digraph

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################



c1_yellow = '#ffffbf'  #lighter #font: c_black
c2_yellow = '#fee090'           #font: c_black         USED
c3_yellow = "#fed47f"           #font: c_black
c4_yellow = "#ffd965"           #font: c_black
c5_yellow = "#feb729"           #font: c_white         USED
c6_yellow = "#ffc000" #darker   #font: c_white


c1_blue = '#e0f3f8'  #lighter   #font: c_black
c2_blue = '#bbd1ff'             #font: c_black
c3_blue = '#abd9e9'             #font: c_black
c4_blue = '#91bfdb'             #font: c_black
c5_blue = "#5b9bd5"             #font: c_black
c6_blue = '#2c7bb6'             #font: c_white       USED
c7_blue = '#4575b4' #darker     #font: c_white       USED



c1_cyan = "#93f0ea"  #lighter   #font: c_black
c2_cyan = "#19b1a7"             #font: c_white
c3_cyan = "#13857d"             #font: c_white      USED
c4_cyan = "#318599"  #darker    #font: c_white


c1_orange = '#fdae61'  #lighter #font: c_black
c2_orange = "#f59d56"           #font: c_white
c3_orange = '#fc8d59'           #font: c_white      USED
c4_orange = "#ea700d"  #darker  #font: c_white


c1_red = '#f9cccc'  #lighter #font: c_black
c2_red = "#ff0000"           #font: c_white
c3_red = '#d73027'           #font: c_white
c4_red = '#d7191c'           #font: c_white         USED
c5_red = '#c81919' #darker   #font: c_white


c1_green = "#4ae087" #lighter   #font: c_black      USED
c2_green = "#70ad47"            #font: c_white
c3_green = "#178544" #darker    #font: c_white      USED


c1_purple = "#e7bdeb" #lighter  #font: c_black
c2_purple= "#a034a8" #darker    #font: c_white      USED


c_white = "#ffffff"                            #    USED
c_black = "#000000"                            #    USED





EntitiesColors=["#e31a1c", "#1f78b4",  "#33a02c", "#ff7f00", "#6a3d9a",  "#b15928", "#b2df8a", "#ffff99" , "#fdbf6f", "#a6cee3"  , "#cab2d6"   ]
ID_Colors=["#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2"]
SubEntities_Color="lightgray"
NodeColors=["violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2","violetred1", "tomato2", "goldenrod3", "orchid", "darkolivegreen1", "wheat1", "khaki2", "tomato2"]


def EntitiesColors_Maker(EntitiesColors):
    list1=[]
    for i in range(len(EntitiesColors)):
        list1.append(EntitiesColors)
    return list1


EntitiesColors_2=EntitiesColors_Maker(ID_Colors)

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

#############Intoductory Functions##################################################

def EntityOrgRel_DF_Show(EnNum, input):
    List1 = []
    for x in range(1, EnNum + 1):
        # print(x)
        moduleVaribale = 'Type2_Entity' + str(x) + 'OrgRel_DF_Show'
        # print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
        else:
            moduleVaribale2 = 0
            # print(moduleVaribale2)
        List1.append(moduleVaribale2)
    # print(dicEntity)
    return List1


def Entity_DF_Show(EnNum, input):
    List1 = []
    for x in range(1, EnNum + 1):
        # print(x)
        moduleVaribale = 'Type2_Entity' + str(x) + '_DF_Show'
        # print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
        else:
            moduleVaribale2 = 0
            # print(moduleVaribale2)
        List1.append(moduleVaribale2)
    # print(dicEntity)
    return List1


def case_Selector1(Attribute, cases):
    concatenated_output = ""
    for i in range(len(Attribute)):
        list1 = ""
        if i == 0:
            txt = " (n1.Type=\"{1}\" AND n1.ID IN {0})"
            case_selector = txt.format(cases[i], Attribute[i])
        else:
            txt = " or (n1.Type=\"{1}\" AND n1.ID IN {0})"
            case_selector = txt.format(cases[i], Attribute[i])
        concatenated_output += case_selector

    return " (" + concatenated_output + " )"


def case_Selector2(Attribute, cases):
    concatenated_output = ""
    for i in range(len(Attribute)):
        list1 = ""
        if i == 0:
            txt = " ( n1.Type=\"{1}\" AND n2.Type=\"{1}\" AND n1.ID IN {0} AND n2.ID IN {0} )"
            case_selector = txt.format(cases[i], Attribute[i])
        else:
            txt = " or  ( n1.Type=\"{1}\" AND n2.Type=\"{1}\" AND n1.ID IN {0} AND n2.ID IN {0} )"
            case_selector = txt.format(cases[i], Attribute[i])
        concatenated_output += case_selector

    return " (" + concatenated_output + " )"


def Finading_Entities_ID(driver, col1, entityListIDproperty, conditionProperty, conditionPropertyValue):
    listFinal = []
    for i in range(len(col1)):
        EnityName = col1[i]
        id = entityListIDproperty[i]
        pro = conditionProperty[i]
        Value = conditionPropertyValue[i]

        # print(EnityName)
        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where  n.{pro}="{Value}"
        return distinct n.{id}
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


def Finading_Reified_Entities_ID(driver, col1):
    listFinal = []
    for i in range(len(col1)):
        EnityName = col1[i]
        # print(EnityName)

        query1 = f'''     
        MATCH p=(e)-[:CORR]->(n:{EnityName})
        where  n.Category="Relative"
        return distinct n.Type
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


def convert_to_list_of_lists(input_list):
    return [[item] for item in input_list]


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


#################################################
#################################################
#################################################
#################################################
#################################################
#################################################
#################################################
#################################################
#################################################



def DF_based_on_Entities(entityList, entityIDlists, EntOrgAbrNum, EntitiesColors, ListEnDFSHow, case_selector1,
                         case_selector2, case_selector_activation, c_white, c_black, dot, driver,activityScenario, colTitle, graphviz_QueryLocation):
    for eachEntity, EntityIDList, Color, Show in zip(entityList, entityIDlists, EntitiesColors, ListEnDFSHow):
        if Show == 1:
            for eachID in EntityIDList:

                Node_Color = c_white
                Edge_Color = Color
                edge_font_color = Color

                Node_Around_Color = c_black
                Edge_width = "1"
                show_lifecycle = False

                if activityScenario == "Activity_Label":
                    asExpression1 = " "
                    asExpression2 = " "
                    asExpression3 = " "
                    asExpression4 = " "
                if activityScenario == "Concept_Label" or activityScenario == "Concept_Label_Level":
                    asExpression1 = "-[:MAPPED_TO]->(m1)"
                    asExpression2 = "-[:MAPPED_TO]->(m2)"
                    asExpression3 = ",m1,m2"
                    asExpression4 = ",m1"


                if case_selector_activation == False:
                    actExpression1 =  " "
                    actExpression2 = " "
                if case_selector_activation == True:
                    actExpression1 =  " AND " + case_selector1
                    actExpression2 = " AND " + case_selector2


                query1 = f'''     

                    match (n1:{eachEntity}{{Type:\"{eachEntity}\"}})<-[:CORR]-(e1: Event) -[df: DF{{Type:\"{eachEntity}\"}}]-> (e2:Event) -[: CORR]-> (n2:{eachEntity} {{Type:\"{eachEntity}\"}})
                    match (e1)-[:OBSERVED]->(a1){asExpression1}
                    match (e2)-[:OBSERVED]->(a2){asExpression2}
                    WHERE n1.ID="{eachID}" AND n2.ID="{eachID}"  and df.ID="{eachID}" {actExpression2} 
                    return distinct a1,e1, df, e2,a2 {asExpression3}  

                                    '''


                print(query1)
                with open(graphviz_QueryLocation, 'a') as file:
                    file.write(f'''//DF_based_on_Entities''')
                    file.write(f'''\n{query1}\n\n''')

                dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                         fontsize="8", margin="0")

                with driver.session() as session:
                    record1 = session.run(query1).values()

                if record1:
                    e1_Neo4J_ID = [item[1].id for item in record1]
                    e2_Neo4J_ID = [item[3].id for item in record1]
                    df_EntityType = [item[2]["Type"] for item in record1]
                    df_Entity_ID = [item[2]["ID"] for item in record1]

                    if activityScenario == "Activity_Label":
                        print("colTitle=",colTitle)
                        e1_Activity = [item[0][colTitle] for item in record1]
                        e2_Activity = [item[4][colTitle] for item in record1]
                    else:
                        e1_Activity = [item[5][colTitle] for item in record1]
                        e2_Activity = [item[6][colTitle] for item in record1]

                    print("e1_Activity=", e1_Activity)
                    print("e1_Neo4J_ID=", e1_Neo4J_ID)
                    print("e2_Activity=", e2_Activity)
                    print("e2_Neo4J_ID=", e2_Neo4J_ID)
                    print("df_Entity_ID=", df_EntityType)
                    print("df_EntityType=", df_Entity_ID)

                    if show_lifecycle:
                        e1_lifecycle = [item[1]["lifecycle"] for item in record1]

                    for i in range(len(record1)):
                        # print(i)

                        if show_lifecycle:
                            e1_name = e1_Activity[i] + '\n' + e1_lifecycle[i][0:5]
                        else:
                            e1_label = e1_Activity[i].replace(' ', '\n')
                            e2_label = e2_Activity[i].replace(' ', '\n')

                        Rel_label = str((df_EntityType[i])[:EntOrgAbrNum]) + '_' + str(df_Entity_ID[i])
                        pen_width = Edge_width

                        # print("e1_label=", e1_label)
                        # print("e2_label=", e2_label)
                        # print("Rel_label=", Rel_label)

                        dot.node(str(e1_Neo4J_ID[i]), e1_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.node(str(e2_Neo4J_ID[i]), e2_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.edge(str(e1_Neo4J_ID[i]), str(e2_Neo4J_ID[i]), label=Rel_label, color=Edge_Color,
                                 penwidth=pen_width, fontname="Helvetica", fontsize="8",
                                 fontcolor=edge_font_color)

                if not record1:



                    query2 = f'''     
                                                match(e1: Event) -[: CORR]-> (n1:{eachEntity} {{Type:\"{eachEntity}\"}})
                                                match (e1)-[:OBSERVED]->(a1){asExpression1}
                                                WHERE n1.ID="{eachID}"   {actExpression1} 
                                                return distinct e1,a1{asExpression4}  
                                         '''



                    print(query2)
                    with open(graphviz_QueryLocation, 'a') as file:
                        file.write(f'''//DF_based_on_Entities''')
                        file.write(f'''\n{query2}\n\n''')

                    dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                             fontsize="8", margin="0")

                    with driver.session() as session:
                        record2 = session.run(query2).values()
                        print(record2)

                        e1_Neo4J_ID = [item[0].id for item in record2]

                        if activityScenario == "Activity_Label":
                            e1_Activity = [item[1][colTitle] for item in record2]
                        else:
                            e1_Activity = [item[2][colTitle] for item in record2]

                        print(e1_Activity)
                        print(e1_Neo4J_ID)

                        if show_lifecycle:
                            e1_lifecycle = [item[1]["lifecycle"] for item in record2]

                        for i in range(len(record2)):
                            # print(i)

                            if show_lifecycle:
                                e1_name = e1_Activity[i] + '\n' + e1_lifecycle[i][0:5]
                            else:
                                e1_label = e1_Activity[i].replace(' ', '\n')

                            dot.node(str(e1_Neo4J_ID[i]), e1_label, color=Node_Around_Color, penwidth="2",
                                     style="filled",
                                     fillcolor=Node_Color)


def DF_based_on_ID(entityList, refEntityIDlists, ListEnOrgRelDFShow, case_selector1, case_selector2,
                   case_selector_activation, c_white, c_black, SubEntities_Color, dot, driver, graphviz_QueryLocation):
    for eachEntity, EntityIDList, Show in zip(entityList, refEntityIDlists, ListEnOrgRelDFShow):
        print("EntityIDList=", EntityIDList)
        if Show == 1 and EntityIDList:
            for eachID in EntityIDList:

                edge_color = SubEntities_Color
                fontcolor = SubEntities_Color
                edge_width = "1"


                if case_selector_activation == True:
                    query = f'''
                                        MATCH (n3:{eachEntity}{{Type:\"{eachID}\"}})<-[:CORR]-(e1) -[df:DF{{Type:\"{eachID}\"}}]-> (e2:Event) -[:CORR]-> (n4:{eachEntity}{{Type:\"{eachID}\"}})
                                        MATCH (n1:{eachEntity}{{Type:\"{eachEntity}\"}})<-[:CORR]-(e1) -[df:DF{{Type:\"{eachID}\"}}]-> (e2:Event) -[:CORR]-> (n2:{eachEntity}{{Type:\"{eachEntity}\"}})  
                                        WHERE {case_selector2}
                                        RETURN distinct e1,df,e2
                                        '''

                else:
                    query = f'''
                                        MATCH (n3:{eachEntity}{{Type:\"{eachID}\"}})<-[:CORR]-(e1) -[df:DF{{Type:\"{eachID}\"}}]-> (e2:Event) -[:CORR]-> (n4:{eachEntity}{{Type:\"{eachID}\"}})
                                        MATCH (e1) -[df:DF{{Type:\"{eachID}\"}}]-> (e2:Event)
                                        RETURN distinct e1,df,e2
                                        '''
                print(query)
                with open(graphviz_QueryLocation, 'a') as file:
                    file.write(f'''//DF_based_on_ID''')
                    file.write(f'''\n{query}\n\n''')

                dot.attr("node", shape="square", fixedsize="true", width="0.4", height="0.4", fontname="Helvetica",
                         fontsize="8", margin="0")

                with driver.session() as session:
                    record = session.run(query).values()

                e1_Neo4J_ID = [item[0].id for item in record]
                e2_Neo4J_ID = [item[2].id for item in record]
                df_EntityType = [item[1]["Type"] for item in record]

                print("e1_Neo4J_ID=", e1_Neo4J_ID)
                print("e2_Neo4J_ID=", e2_Neo4J_ID)
                print("df_EntityType=", df_EntityType)

                for i in range(len(record)):
                    Rel_label = (df_EntityType[i])[0:]
                    pen_width = edge_width

                    dot.edge(str(e1_Neo4J_ID[i]), str(e2_Neo4J_ID[i]), label=Rel_label, color=edge_color,
                             penwidth=pen_width, fontname="Helvetica", fontsize="8", fontcolor=fontcolor)


def Adding_Entities_ID_ForFirstEvent(entityList, entityIDlists, EntitiesColors, ListEnDFSHow, case_selector1,
                                     case_selector_activation, c_white, c_black, dot, driver, graphviz_QueryLocation):
    for eachEntity, EntityIDList, Color, Show in zip(entityList, entityIDlists, EntitiesColors, ListEnDFSHow):
        if Show == 1:
            for eachID in EntityIDList:

                Node_Around_Color = Color
                Node_Color = Color
                Edge_Color = Color
                fontcolor = c_white

                if case_selector_activation == False:
                    actExpression1 =  " "
                if case_selector_activation == True:
                    actExpression1 =  " AND " + case_selector1


                query2 = f'''
                        MATCH(e: Event) -[corr: CORR]-> (n1:{eachEntity})
                        WHERE n1.Type = "{eachEntity}" and n1.ID="{eachID}"  {actExpression1}
                        return e, n1
                        order by e.timestamp, e.idx
                        limit 1
                         '''

                print(query2)
                with open(graphviz_QueryLocation, 'a') as file:
                    file.write(f'''//Adding_Entities_ID_ForFirstEvent''')
                    file.write(f'''\n{query2}\n\n''')

                dot.attr("node", shape="circle", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                         fontsize="8", margin="0")

                with driver.session() as session:
                    record2 = session.run(query2).values()

                if record2:
                    n_Neo4J_ID = [item[1].id for item in record2]
                    e_Neo4J_ID = [item[0].id for item in record2]

                    ID_Value = eachID

                    # print(n_Neo4J_ID)
                    # print(e_Neo4J_ID)

                    Entity_Node_label = str(ID_Value)
                    Event_Node_ID = str(e_Neo4J_ID[0])
                    Entity_Node_ID = str(n_Neo4J_ID[0])

                    dot.node(Entity_Node_ID, Entity_Node_label, color=Node_Around_Color, style="filled",
                             fillcolor=Node_Color,
                             fontcolor=fontcolor)
                    dot.edge(Entity_Node_ID, Event_Node_ID, style="dashed", arrowhead="none",
                             color=Edge_Color)


def Adding_Entities(entityList, entityIDlists, EntitiesColors, ListEnDFSHow, c_white, c_black, dot, driver,
                    case_selector2, case_selector_activation, graphviz_QueryLocation):
    list3 = []
    for eachEntity, EntityIDList, Show in zip(entityList, entityIDlists, ListEnDFSHow):
        if Show == 1:
            list1 = []
            for eachID in EntityIDList:
                list2 = []
                if case_selector_activation == False:
                    actExpression2 =  " "
                if case_selector_activation == True:
                    actExpression2 =  " AND " + case_selector2


                query1 = f'''     

                    match (n1:{eachEntity}{{Type:\"{eachEntity}\"}})<-[:CORR]-(e1: Event) -[df: DF{{Type:\"{eachEntity}\"}}]-> (e2:Event) -[: CORR]-> (n2:{eachEntity} {{Type:\"{eachEntity}\"}})
                    match (e1)-[:OBSERVED]->(a1)
                    match (e2)-[:OBSERVED]->(a2)
                    WHERE n1.ID="{eachID}" AND n2.ID="{eachID}"  {actExpression2} 
                    return a1,e1, df, e2,a2

                                    '''


                print(query1)
                with open(graphviz_QueryLocation, 'a') as file:
                    file.write(f'''//Adding_Entities''')
                    file.write(f'''\n{query1}\n\n''')

                with driver.session() as session:
                    record1 = session.run(query1).values()

                if record1:
                    df_EntityType = [item[2]["Type"] for item in record1]
                    # print("df_EntityType=", df_EntityType)
                    list2.extend(df_EntityType)

                list1.extend(list2)
            list3.extend(list1)
        print("list3=", list3)
        list3 = list(dict.fromkeys(list3))
        print("list3=", list3)

    for Entity_Alias, Color, posit in zip(entityList, EntitiesColors, range(1, 70)):
        print(posit)

        Node_Around_Color = c_white
        Node_Color = Color
        Node_fontcolor = c_black
        Node_label = f"{Entity_Alias}"
        NodeColorStriped = f"{Node_Around_Color}:{Node_Around_Color}:{Node_Around_Color}:{Node_Color}"

        if Entity_Alias in list3:
            print(Entity_Alias)

            with dot.subgraph(name="cluster_0", comment='name2') as subDot:
                subDot.attr(style='filled', color=Node_Around_Color)
                subDot.attr(label='\nEntities:')

                subDot.node_attr.update(shape="rectangle", fixedsize="True", width="0.8", height="0.2",
                                        fontname="Helvetica",
                                        fontsize="8", margin="0")

                subDot.node(str(posit + 99999), Node_label, color=Node_Around_Color, style="striped",
                            fillcolor=NodeColorStriped,
                            fontcolor=Node_fontcolor)


