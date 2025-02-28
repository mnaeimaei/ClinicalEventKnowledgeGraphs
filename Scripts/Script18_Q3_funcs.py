

from graphviz import Digraph
#############Intoductory Functions##################################################
Type3_Entity1_DF_Show = 1
Type3_Entity2_DF_Show = 0
Type3_Entity3_DF_Show = 0
Type3_Count = 0




def Entity_DF_Show(EnNum, input):
    List1 = []
    #print("EnNum=",EnNum)
    for x in range(1, EnNum + 1):
        #print(x)
        moduleVaribale = 'Type3_Entity' + str(x) + '_DF_Show'
        #print(moduleVaribale)
        if hasattr(input, moduleVaribale) == True:
            moduleVaribale2 = (getattr(input, moduleVaribale))
        else:
            moduleVaribale2 = 0
            #print(moduleVaribale2)
        List1.append(moduleVaribale2)
    #print(dicEntity)
    return List1


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


###################Graph Functions"""""""""""""""""""""""""""""""""""

def DFC_based_on_Origins (EntityOrgValue, EntOrgAbrNum, EntitiesColors, ListEnDFSHow, count, c_white, c_black, dot, driver,activityScenario, colTitle):
    for EntityOrgValue_list, Color, Show in zip(EntityOrgValue, EntitiesColors, ListEnDFSHow):
        #print("model_entities_derived_list=", EntityOrgValue_list)
        #print("Color=", Color)
        #print("Show=", Show)
        if Show == 1:
            for model_entities in EntityOrgValue_list:
                En1 = model_entities  # EntityType

                number=count

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
                if activityScenario == "Concept_Label":
                    asExpression1 = "-[:MAPPED_TO]->(m1)"
                    asExpression2 = "-[:MAPPED_TO]->(m2)"
                    asExpression3 = ",m1,m2"
                    asExpression4 = ",m1"


                dot.attr("node", shape="square", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
                             fontsize="8", margin="0")



                query1 = f'''     
                            MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                            match (c1:Activity){asExpression1}
                            match (c2:Activity){asExpression2}
                            WHERE df.count >= {number} and df.Type = "All" and df.En1="{En1}" and df.En1_ID="0"
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
                    df_count = [item[1]["count"] for item in record1]

                    if activityScenario=="Activity_Label":
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




                        Rel_label = str((df_count[i]))
                        pen_width = Edge_width

                        #print("c1_label=", c1_label)
                        #print("c2_label=", c2_label)
                        #print("Rel_label=", Rel_label)



                        dot.node(str(c1_Neo4J_ID[i]), c1_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.node(str(c2_Neo4J_ID[i]), c2_label, color=Node_Around_Color, penwidth="2", style="filled",
                                 fillcolor=Node_Color)
                        dot.edge(str(c1_Neo4J_ID[i]), str(c2_Neo4J_ID[i]), label=Rel_label, color=Edge_Color,
                                 penwidth=pen_width, fontname="Helvetica", fontsize="8",
                                 fontcolor=edge_font_color)






def DFC_Adding_Entities (EntityOrgValue, EntitiesColors, ListEnDFSHow, count, driver, c_white, c_black, dot):
    list3 = []
    for EntityOrgValue_list, Show in zip(EntityOrgValue, ListEnDFSHow):
        #print("model_entities_derived_list=", EntityOrgValue_list)
        if Show == 1:
            list1=[]
            for model_entities in EntityOrgValue_list:
                list2 = []
                En1 = model_entities  # EntityType
                number=count

                query1 = f'''     
                            MATCH (c1:Activity {{Type:"Activity"}}) -[df:DF_C]-> (c2:Activity {{Type:"Activity"}})
                            WHERE df.count >= {number} and df.Type = "All" and df.En1="{En1}" and df.En1_ID="0"
                            return c1,df,c2
                        '''
                print(query1)

                with driver.session() as session:
                    record1 = session.run(query1).values()

                if record1:
                    df_En1 = [item[1]["En1"] for item in record1]
                    print("df_En1=", df_En1)
                    list2.extend(df_En1)

                list1.extend(list2)
            list3.extend(list1)
        # print("list3=",list3)
        list3 = list(dict.fromkeys(list3))
        #print("list3=",list3)

    for Entity_Alias, Color, posit in zip(EntityOrgValue, EntitiesColors, range(1, 700)):
        # print(posit)
        #print("Entity_Alias=", Entity_Alias)

        Node_Around_Color = c_white
        Node_Color = Color
        Node_fontcolor = c_black
        Node_label = f"{Entity_Alias[0]}"
        NodeColorStriped = f"{Node_Around_Color}:{Node_Around_Color}:{Node_Around_Color}:{Node_Color}"

        if Entity_Alias[0] in list3:

            with dot.subgraph(name="cluster_0", comment='name2') as subDot:
                subDot.attr(style='filled', color=Node_Around_Color)
                subDot.attr(label='\nEntity:')

                subDot.node_attr.update(shape="rectangle", fixedsize="True", width="0.8", height="0.2",
                                        fontname="Helvetica",
                                        fontsize="8", margin="0")

                subDot.node(str(posit + 999), Node_label, color=Node_Around_Color, style="striped",
                            fillcolor=NodeColorStriped,
                            fontcolor=Node_fontcolor)


