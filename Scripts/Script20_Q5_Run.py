from neo4j import GraphDatabase
import platform
from graphviz import Digraph
import os


import Script20_Q5_funcs as funcs




print("************************** From cla: ****************************************************************************")

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))


print("************************** From confg: ****************************************************************************")


case_selector_activation1=funcs.Type5_Rel_1_DF_Show_selection
print("case_selector_activation1=", case_selector_activation1)

Type5_selection_ID_instances1=funcs.Type5_Rel_1_DF_Show_selection_ID_instances
print("Type5_selection_ID_instances1=", Type5_selection_ID_instances1)

case_selector_activation2=funcs.Type5_Rel_2_DF_Show_selection
print("case_selector_activation2=", case_selector_activation2)

Type5_selection_ID_instances2=funcs.Type5_Rel_2_DF_Show_selection_ID_instances
print("Type5_selection_ID_instances2=", Type5_selection_ID_instances2)

count=funcs.Type5_Count
print("count =", count)

case_selector1, case_selector_list1=funcs.case_Selector1(Type5_selection_ID_instances1)
print("case_selector1=", case_selector1)
print("case_selector_list1=", case_selector_list1)

case_selector2, case_selector_list2=funcs.case_Selector2(Type5_selection_ID_instances2)
print("case_selector2=", case_selector2)
print("case_selector_list2=", case_selector_list2)


print("************************** From cl1: ****************************************************************************")




myInput="main_Entities_plus_SCT"



#1
if myInput == "main_Entities" :

    entityList = ['Patient', 'Admission']
    entityListIDproperty = ['ID', 'ID']
    conditionProperty = ['Category', 'Category']
    conditionPropertyValue = ['Absolute', 'Absolute']

#2
if myInput == "main_Entities_plus_Disorder" :
    entityList = ["Disorder", 'Patient', 'Admission']
    entityListIDproperty = ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#3
if myInput == "main_Entities_plus_ICD" :
    entityList = ["Clinical", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#5
if myInput == "main_Entities_plus_SCT" :
    entityList = ["Concept", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']

#7
if myInput == "main_Entities_plus_SCT_Level_One":
    entityList = ["Concept", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']


#8
if myInput == "main_Entities_plus_ICD_one":
    entityList = ["Clinical", 'Patient', 'Admission']
    entityListIDproperty =  ["ID", 'ID', 'ID']
    conditionProperty = ["Category", 'Category', 'Category']
    conditionPropertyValue = ["Absolute",'Absolute', 'Absolute']





EnNum=len(entityList)
print("EnNum=", EnNum)








print("************************** From cl0 for Activity: *****************************************************")


Activity = "Concept_Label"
ActivityNode = ["Concept"]
colTitle = "termA"

print("activityScenario=",Activity)
print("ActivityNode=",ActivityNode)
print("colTitle=", colTitle)





print("************************** From cl7 ****************************************************************************")


c_white = "#ffffff"
c_black = "#000000"

ID_Colors=["#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2","#8dd3c7", "#b3de69", "#bebada", "#fb8072", "#80b1d3" , "#fdb462", "#fccde5", "powderblue", "#bc80bd", "#ffed6f", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2", "#d9d9d9","#ccebc5", "#fb9a99", "navajowhite2", "seashell2"]


print("ID_Colors=",ID_Colors)
print(len(ID_Colors))


print("************************** From cl5 ****************************************************************************")






print("*********************************New***********************************************************")


Type5_Rel_1_DF_Show = "Subject"  #Q02
Type5_Rel_2_DF_Show = "Visit" #Q02

Entity1=Type5_Rel_1_DF_Show
Entity2=Type5_Rel_2_DF_Show
print("Entity1=", Entity1)
print("Entity2=", Entity2)


entityIDlists=funcs.Finading_Entities_ID_2(driver,Entity1)
print("entityIDlists=",entityIDlists)

rel_all=funcs.relationship_rel(Entity1,Entity2,entityIDlists)
print("rel_all=",rel_all)

rel_list=funcs.final_DFG_List_func_3(rel_all)
print("rel_list=",rel_list)


print("********************************* Output files *************************************************")

Output_Graph_File_Name="new"
print("Output_Graph_File_Name=",Output_Graph_File_Name)
input='Question5'
print("input=",input)
inputName = input
filename= inputName+ "_" + str(1)
print("filename=",filename)



medDataDirectory = f'Figures'
dataPath = os.path.realpath(medDataDirectory)



outdir = dataPath


if not os.path.exists(outdir):
    os.mkdir(outdir)



i = 1
while os.path.exists(outdir + '/' + inputName + "_%s" % i):
    #print("i=",i)
    i += 1
    filename = inputName + "_" + str(i)
    #print("filename=",filename)

outdirMic = outdir + '/' + filename
print("outdirMic=",outdirMic)
if not os.path.isdir(outdirMic):
    os.mkdir(outdirMic)


graphviz_QueryLocation = outdirMic + "/" + filename + "_graphviz.txt"
with open(graphviz_QueryLocation, 'w') as file:
    file.write(f'''''')
print("\n A txt file for saving Queries was created")

print("*********************************Conditional***********************************************************")


dot = Digraph(comment='Query Result')
dot.attr("graph", rankdir="LR", margin="0")



print("*********************************DF_based_on_Entities***********************************************************")
print("rel_list=",rel_list)
funcs.DFC_based_on_Origins(rel_list, ID_Colors, count, c_white, c_black, dot, driver,Activity, colTitle ,case_selector_activation1,case_selector_list1,case_selector1,case_selector_activation2,case_selector_list2,case_selector2 )



print("*********************************Adding_Entities**********************************************")

funcs.DFC_Adding_Entities(rel_list, count, ID_Colors, c_white, c_black,dot, driver,case_selector_activation1,case_selector_list1,case_selector1,case_selector_activation2,case_selector_list2,case_selector2 )




####################################Output######################################################################


print("")
print("")
print("********************************* Creating Output*******************************************************")



print("input=",input)
print("filename=",filename)
print("outdirMic=",outdirMic)



print("\n Converting to DOT File:")
DOT_Output_Location = f'{outdirMic}/{filename}.dot'
file = open(DOT_Output_Location, "w")
file.write(dot.source)
file.close()
if os.path.exists(DOT_Output_Location):
    print("\n Dot File was created")
else:
    print("\n Dot File was not created")





print("\n Converting to PDF File:")
PDF_Output_Location = f'{outdirMic}/{filename}.pdf'
PDF_Output_creation = f'dot -Tpdf "{DOT_Output_Location}" -o "{PDF_Output_Location}"'
os.system(PDF_Output_creation)
if os.path.exists(PDF_Output_Location):
    print("\n PDF File was created")
else:
    print("\n PDF File was not created")
