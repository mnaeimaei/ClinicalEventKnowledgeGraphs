from neo4j import GraphDatabase
import platform
from graphviz import Digraph
import os



import Script18_Q3_funcs as funcs



print("************************** From cla: ****************************************************************************")

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

print("************************** From cl1: ****************************************************************************")


count=funcs.Type3_Count
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

print("************************** From cl2: ****************************************************************************")





print("************************** From Func: ****************************************************************************")

EntityOrgValue=funcs.convert_to_list_of_lists(entityList)
print("EntityOrgValue=",EntityOrgValue)



EntityOriginValueTemp=funcs.EntityOriginValue_Temp(EntityOrgValue,  EnNum)
print("Entities Origin Value Temp (EntityOriginValueTemp) =", EntityOriginValueTemp)


dicNumEntOrgAbr=funcs.NumberEntityOriginAbr(EntityOriginValueTemp, EnNum)
# print(dicNumEntTypeAbr)
locals().update(dicNumEntOrgAbr)
print("Number of Entity Org Abr (dicNumEntOrgAbr)=", dicNumEntOrgAbr)

EntOrgAbrNum = list(dicNumEntOrgAbr.values())[0] #accessing to entity Type abbr number
print("EntOrgAbrNum=",EntOrgAbrNum)





print("************************** From cl7: ****************************************************************************")

c_white = "#ffffff"
c_black = "#000000"

EntitiesColors=["#e31a1c", "#1f78b4",  "#33a02c", "#ff7f00", "#6a3d9a",  "#b15928", "#b2df8a", "#ffff99" , "#fdbf6f", "#a6cee3"  , "#cab2d6"   ]


print("EntitiesColors=",EntitiesColors)




print("********************************* Output files *************************************************")

Output_Graph_File_Name="new"
print("Output_Graph_File_Name=",Output_Graph_File_Name)
input='Question3'
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
print("EntityOrgValue=",EntityOrgValue)
funcs.DFC_based_on_Origins(EntityOrgValue, EntOrgAbrNum, EntitiesColors, ListEnDFSHow, count, c_white, c_black, dot, driver,Activity, colTitle)


print("*********************************Adding_Entities**********************************************")
print("EntityOrgValue=",EntityOrgValue)
funcs.DFC_Adding_Entities(EntityOrgValue, EntitiesColors, ListEnDFSHow, count, driver, c_white, c_black, dot)



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

