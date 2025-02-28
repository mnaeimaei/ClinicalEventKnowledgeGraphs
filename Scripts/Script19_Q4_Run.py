from neo4j import GraphDatabase
import platform
from graphviz import Digraph
import os





import Script19_Q4_funcs as funcs


print("************************** From cla: ****************************************************************************")

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))


print("************************** From cl1: ****************************************************************************")


count=funcs.Type4_Count
print("count =", count)


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




case_selector_activation=funcs.Type4_selection
print("case_selector_activation=", case_selector_activation)

Type4_selection_ID_instances=funcs.Type4_selection_ID_instances
print("Type4_selection_ID_instances=", Type4_selection_ID_instances)




ListEnDFSHow = funcs.Entity_DF_Show(EnNum, funcs)
# print(ListEnDFSHow)
#locals().update(ListEnDFSHow)
print("Entity_DF_Show (ListEnDFSHow)=", ListEnDFSHow)


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


print("************************** From Func: ****************************************************************************")

case_selector, case_selector_list=funcs.case_Selector(Type4_selection_ID_instances)
print("case_selector=", case_selector)
print("case_selector_list=", case_selector_list)



entityIDlists=funcs.Finading_Entities_ID(driver,entityList,entityListIDproperty,conditionProperty,conditionPropertyValue)
print("entityIDlists=",entityIDlists)

EntityOrgValue=funcs.convert_to_list_of_lists(entityList)
print("EntityOrgValue=",EntityOrgValue)

refEntityIDlists2=funcs.Finading_Reified_Entities_ID2(driver,entityList)
print("refEntityIDlists2=",refEntityIDlists2)

combining_IDs_List=funcs.combining_IDs_List_func(EnNum, entityIDlists, refEntityIDlists2)
print("combining_IDs_List=",combining_IDs_List)

combining_IDs_List_color=funcs.color_combine(combining_IDs_List)
print("combining_IDs_List_color=",combining_IDs_List_color)

final_DFG_List_Abs_color=funcs.final_DFG_List_Absolute_2(EnNum, EntityOrgValue, combining_IDs_List, combining_IDs_List_color)
print("final_DFG_List_Abs_color=",final_DFG_List_Abs_color)


print("********************************* Output files *************************************************")

Output_Graph_File_Name="new"
print("Output_Graph_File_Name=",Output_Graph_File_Name)
input='Question4'
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
print("final_DFG_List_Abs_color=",final_DFG_List_Abs_color)
funcs.DFC_based_on_Origins(final_DFG_List_Abs_color, ID_Colors, ListEnDFSHow, count, case_selector, case_selector_activation, c_white, c_black, dot, driver,Activity, colTitle)


print("*********************************Adding_Entities**********************************************")
print("final_DFG_List_Abs_color=",final_DFG_List_Abs_color)
funcs.DFC_Adding_Entities(final_DFG_List_Abs_color, ID_Colors, ListEnDFSHow, count, case_selector, case_selector_activation, case_selector_list, c_white, c_black, dot, driver)



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

