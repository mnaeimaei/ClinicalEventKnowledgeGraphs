print("************************** This is ""DFG2"" *****************************************************")

from neo4j import GraphDatabase
import platform
from graphviz import Digraph
import os






import Script16_Q1_HigherAbstraction_funcs as funcs



import webbrowser

print("************************** From cla: ****************************************************************************")

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

print("************************** From cl1: *****************************************************")


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





print("entityList=",entityList)

EnNum=len(entityList)
print("EnNum=", EnNum)


print("entityListIDproperty=",entityListIDproperty)


print("conditionProperty=",conditionProperty)


print("conditionPropertyValue=",conditionPropertyValue)



print("************************** From cl0 for Activity: *****************************************************")
Activity = "Concept_Label"
ActivityNode = ["Concept"]
colTitle = "termA"



print("activityScenario=",Activity)
print("ActivityNode=",ActivityNode)
print("colTitle=", colTitle)


print("************************** From cl7: *****************************************************")


c_white=funcs.c_white
c_black=funcs.c_black
SubEntities_Color=funcs.SubEntities_Color

EntitiesColors=funcs.EntitiesColors
print("EntitiesColors=",EntitiesColors)

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


entityIDlists=funcs.Finading_Entities_ID(driver,entityList,entityListIDproperty,conditionProperty,conditionPropertyValue)
print("entityIDlists=",entityIDlists)


refEntityIDlists=funcs.Finading_Reified_Entities_ID(driver,entityList)
print("refEntityIDlists=",refEntityIDlists)





print("*********************************Conf General**************************************************")





generalFileName="_general"
print("generalFileName=", generalFileName)


ListEnDFSHow = []
# Loop through each number up to n
for i in range(EnNum):
    # Append 1 if the current index is even (including 0), otherwise append 0
    ListEnDFSHow.append(1)

print("Entity_DF_Show (ListEnDFSHow)=", ListEnDFSHow)

case_selector_activation = False
print("case_selector_activation=", case_selector_activation)



ListEnOrgRelDFShow = []
# Loop through each number up to n
for i in range(EnNum):
    # Append 1 if the current index is even (including 0), otherwise append 0
    ListEnOrgRelDFShow.append(1)


print("Entity_Org_Rel_DF_Show (ListEnOrgRelDFShow)=", ListEnOrgRelDFShow)


case_selector1 = "Not Exist"
print("case_selector1=", case_selector1)


case_selector2 = "Not Exist"
print("case_selector2=", case_selector2)


print("********************************* Output files *************************************************")

Output_Graph_File_Name="new"
print("Output_Graph_File_Name=",Output_Graph_File_Name)
input='Question1_Higher'
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

funcs.DF_based_on_Entities(entityList,entityIDlists, EntOrgAbrNum, EntitiesColors, ListEnDFSHow, case_selector1,case_selector2, case_selector_activation, c_white, c_black, dot, driver,Activity, colTitle,graphviz_QueryLocation)


print("*********************************DF_based_on_ID************************************************")


funcs.DF_based_on_ID(entityList,refEntityIDlists, ListEnOrgRelDFShow, case_selector1,case_selector2, case_selector_activation, c_white, c_black, SubEntities_Color, dot, driver,graphviz_QueryLocation)


print("*********************************Adding_Entities_ID ForFirstEvent**********************************************")

funcs.Adding_Entities_ID_ForFirstEvent(entityList,entityIDlists, EntitiesColors, ListEnDFSHow, case_selector1,case_selector_activation, c_white, c_black, dot, driver,graphviz_QueryLocation)


print("*********************************Adding_Entities**********************************************")

funcs.Adding_Entities(entityList, entityIDlists, EntitiesColors, ListEnDFSHow, c_white, c_black, dot, driver,case_selector2, case_selector_activation,graphviz_QueryLocation)




####################################Output######################################################################
################################################################################################################
################################################################################################################
################################################################################################################
################################################################################################################

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






