import os

import pandas as pd
import time, csv
from neo4j import GraphDatabase



def ImportCSV(inputPath, inputFileName):
    csvLog = pd.read_csv(os.path.realpath(inputPath + inputFileName), keep_default_na=True)  # load full log from csv
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

def ImportCSVRename(csvLog, activityTitle, Timestamp):
    csvLog = csvLog.rename(columns={activityTitle: 'Activity', Timestamp: 'timestamp'})
    activityTitle = 'Activity'
    Timestamp = 'timestamp'
    return csvLog, activityTitle, Timestamp

def CreateM23(csvLog, Neo4JImport, outputFileName, Entity1Origin, eventIdTitle, EntityIDColumnList):
    sampleIds = []
    csvLog['EventIDraw'] = csvLog[eventIdTitle]

    sampleList = []  # create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in csvLog.iterrows():
        if sampleIds == [] or row[Entity1Origin] in sampleIds:
            rowList = list(row)  # add the event data to rowList
            sampleList.append(rowList)  # add the extended, single row to the sample dataset

    header = list(csvLog)  # save the updated header data
    logSamples = pd.DataFrame(sampleList, columns=header)  # create pandas dataframe and add the samples

    logSamples['timestamp'] = pd.to_datetime(logSamples['timestamp'], format='%Y-%m-%d %H:%M:%S')

    logSamples.fillna(0)
    logSamples.sort_values([Entity1Origin, 'timestamp'], inplace=True)
    logSamples['timestamp'] = logSamples['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')

    logSamples.insert(0, 'row', range(1, 1 + len(logSamples)))

    #------converting IDs Columns values to list in dataframe-------
    for i in range(len(EntityIDColumnList)):
        item=EntityIDColumnList[i]
        #print(item)
        #print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(",") if ("," in str(x) and str(x) != 'nan')
                                                     else (list(x.split(" ")) if ("," not in str(x) and str(x) != 'nan')
                                                     else "Unknown" )
                                                )
        #print(logSamples[item])
        logSamples[item] = logSamples[item].map(lambda x: x.split(" ") if (str(x) == 'Unknown') else (x))

        #print(logSamples[item])
    #------converting IDs Columns values to list in dataframe-------

    logSamples.to_csv(Neo4JImport + outputFileName, index=True, index_label="idx", na_rep="Unknown")
    print(logSamples.to_string())


def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i == 0):
                headerCSV = list(row)
                i += 1
            else:
                datasetList.append(row)

    log = pd.DataFrame(datasetList, columns=headerCSV)

    return headerCSV, log


def runQuery(driver, query):
    with driver.session() as session:
        result = session.run(query)
        if result != None:
            return result
        else:
            return None

###############################################################################################
# Neo4J Output massage functions

def Neo4J_relationship_massage(result):
    import ast
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


################################## Step 1 ###########################################################


def deleteRelation(tx, relationTypes):
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r;'''
        print(qDeleteRelation)
        tx.run(qDeleteRelation)


def deleteAllRelations(tx):
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r;"
    print(qDeleteAllRelations)
    tx.run(qDeleteAllRelations)


def DeleteNodes(tx, nodeTypes):
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n;'''
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
    print("if anything exists maybe Constraints CALL db.constraints, maybe Index CALL db.indexes()")


################################## Step 2 ###########################################################


def clearConstraint(tx, x, driver):
    for x in tx.run("CALL db.constraints();"):

        ############PART C ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( log:Log ) ASSERT (log.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (l:Log) ASSERT l.ID IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (l:Log) ASSERT l.ID IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( event:Event ) ASSERT (event.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (e:Event) ASSERT e.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (e:Event) ASSERT e.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( domain:Domain ) ASSERT (domain.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (d:Domain) ASSERT d.ID IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (d:Domain) ASSERT d.ID IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( entity:Entity ) ASSERT (entity.uID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (en:Entity) ASSERT en.uID IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (en:Entity) ASSERT en.uID IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( class:Class ) ASSERT (class.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (c:Class) ASSERT c.ID IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (c:Class) ASSERT c.ID IS UNIQUE;")

        ############PART E ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( act:Act ) ASSERT (act.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Act) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Act) ASSERT ep.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( form:Form ) ASSERT (form.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Form) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Form) ASSERT ep.ID  IS UNIQUE;")

        ############PART F ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( patient:Patient ) ASSERT (patient.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:Patient) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:Patient) ASSERT ep.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( admission:Admission ) ASSERT (admission.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Admission) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Admission) ASSERT m.ID  IS UNIQUE;")

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( disorder:Disorder ) ASSERT (disorder.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Disorder) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Disorder) ASSERT m.ID  IS UNIQUE;")

        ############PART G ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( icd:ICD ) ASSERT (icd.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (ep:ICD) ASSERT ep.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (ep:ICD) ASSERT ep.ID  IS UNIQUE;")

        ############PART H ##########################################

        if x is not None:
            if x["description"] == "CONSTRAINT ON ( concept:Concept ) ASSERT (concept.ID) IS UNIQUE":
                with driver.session() as session:
                    session.run("DROP CONSTRAINT ON (m:Concept) ASSERT m.ID  IS UNIQUE;").single()
                    print("DROP CONSTRAINT ON (m:Concept) ASSERT m.ID  IS UNIQUE;")

        #############################################################

        else:
            print("Database is empty and nothing to drop")


################################## Step 3 ###########################################################

def createConstraint(tx):
    ##################Part1
    qCS1 = f'CREATE CONSTRAINT ON (e:Event) ASSERT e.ID IS UNIQUE;'
    qCS2 = f'CREATE CONSTRAINT ON (en:Entity) ASSERT en.uID IS UNIQUE;'
    qCS3 = f'CREATE CONSTRAINT ON (l:Log) ASSERT l.ID IS UNIQUE;'
    qCS4 = f'CREATE CONSTRAINT ON (c:Class) ASSERT c.ID IS UNIQUE;'
    qCS5 = f'CREATE CONSTRAINT ON (d:Domain) ASSERT d.ID IS UNIQUE;'

    qTest = f'''
            #########Step4 Testing:#######################################
            :schema
            ##############################################################
            '''

    print(qCS1)
    print(qCS2)
    print(qCS3)
    print(qCS4)
    print(qCS5)

    print(qTest)
    tx.run(qCS1)
    tx.run(qCS2)
    tx.run(qCS3)
    tx.run(qCS4)
    tx.run(qCS5)


################################## Step 4 ###########################################################


# create events from CSV table: one event node per row, one property per column
def CreateEventQuery(logHeader, fileName, EntityIDColumnList, LogID=""):
    print("")
    print("Inputs:")
    print("logHeader=", logHeader)
    print("fileName=", fileName)
    print("EntityIDColumnList=", EntityIDColumnList)
    print("LogID=", LogID)
    print("")

    print('Creating Event Nodes from CSV')
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line'

    for col in logHeader:
        if col == 'idx' or col == 'row':
            column = f'toInteger(line.{col})'
        elif col in ['timestamp', 'start', 'end']:
            column = f'datetime(line.{col})'
        elif col in EntityIDColumnList:

            column = f'apoc.convert.fromJsonList(line.{col})'
        else:
            column = 'line.' + col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (e:Event {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader) - 1):
            newLine = f' {col}: {column} }})'

        query = query + newLine

    testingQ = f'''
            #########Step3 Testing:######################################
            MATCH (e:Event) 
            return e;

            MATCH (e:Event) 
            return count(*) as count;
            #############################################################
            '''

    return query, testingQ


################################## Step5 ###########################################################


def filterEvents(tx, condition):
    qFilterEvents = f'MATCH (e:Event) {condition} DELETE e'
    print(qFilterEvents)
    tx.run(qFilterEvents)


################################## Step 6 ###########################################################

def createLog(tx, log_id):
    print("")
    print("Inputs:")
    print("log_id=", log_id)
    print("")
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'
    qTest = f'''
            #########Step6 Testing:#######################################
            MATCH (l:Log) return l;

            MATCH (l:Log) return count(*) as count;
            ##############################################################
            '''
    print(qCreateLog)
    print(qTest)

    tx.run(qCreateLog)


################################## Step 7 ###########################################################

def link_log_evnts(tx, log_id):
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
            #########Step7 Testing:#######################################
            MATCH p=(l)-[:HAS]->(e)
            return p;
            ##############################################################
            '''

    print(qTest)

    tx.run(qLinkEventsToLog)


################################## Step 8 ###########################################################

def createDomains(tx, WHERE_domain_property, DomainType):
    qCreateEntity = f'''

            MATCH (e:Event) {WHERE_domain_property}
            MERGE (d:Domain {{ DomainType:"{DomainType}" }});

            '''
    print(qCreateEntity)

    qTest = f'''
            #########Step8 Testing:#######################################
            MATCH (d:Domain)
            return d;


            MATCH (d:Domain)
            where d.DomainType="{DomainType}"
            return d;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


################################## Step 9 ###########################################################

def createEntities(tx, entity_id, entity_id_column, WHERE_event_property, MainEntity):
    qCreateEntity = f'''
            MERGE (n:Entity {{
            ID:"{entity_id}", 
            uID:("{MainEntity}"+toString("{entity_id}")), 
            EntityName: "{MainEntity}", 
            EntityType:"{MainEntity}" ,
            EntityCategory:"Absolute" 

            }});'''
    print(qCreateEntity)

    qTest = f'''
            #########Step9 Testing:#######################################
            MATCH (n:Entity) 
            return n;

            MATCH (n:Entity) 
            where n.EntityType="{MainEntity}"
            return n;

            MATCH (n:Entity) 
            where n.EntityName="{MainEntity}"
            return n;

            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateEntity)


################################## Step 10 ###########################################################


def createReifiedEntities(tx, relation_type, entityID1, entityID2, MainEntity):
    # MATCH(n1: Entity ) WHERE
    # n1.EntityType = "{MainEntity}"
    # MATCH(n2: Entity ) WHERE
    # n2.EntityType = "{MainEntity}"
    qCreateReifiedEntity = f'''

            Create (en:Entity {{ 
                ID:toString("{entityID1}")+"_"+toString("{entityID2}"),
                EntityType: "{relation_type}",
                EntityName: "{MainEntity}",
                EntityCategory:"Relative" ,
                uID:"{relation_type}"+toString("{entityID1}")+"_"+toString("{entityID2}") }});'''
    print(qCreateReifiedEntity)

    qTest = f'''
            #########Step10 Testing:#######################################
            MATCH (n:Entity) 
            return n;

            MATCH (n:Entity) 
            where n.EntityType="{relation_type}"
            return n;

            MATCH (n:Entity) 
            where n.EntityName="{MainEntity}"
            return n;
            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateReifiedEntity)


################################## Step 11 ###########################################################


def correlate_Domain_to_Events(tx, DomainValue, domainColTitle):
    qCorrelate = f'''
            MATCH (e:Event) WHERE e.{domainColTitle} ="{DomainValue}"
            MATCH  (d:Domain {{ DomainType:"{DomainValue}" }})
            CREATE (e)-[:INTER]->(d);
            '''
    print(qCorrelate)
    qTest = f''' 
            #########Step11 Testing:#######################################         
            MATCH p=(e)-[:INTER]->(d)
            WHERE d.DomainType="{DomainValue}" 
            return p;
            ##############################################################
            '''

    print(qTest)
    tx.run(qCorrelate)


################################## Step 12 ###########################################################

def correlate_Events_to_Entities(tx, entity_id, entity_id_column, WHERE_event_property, EntityType):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property} and "{entity_id}" in e.{entity_id_column}
            MATCH (n:Entity {{EntityType: "{EntityType}" }}) WHERE n.ID = "{entity_id}"
            CREATE (e)-[:CORR]->(n)'''
    print(qCorrelate)
    qTest = f''' 
            #########Step12 Testing:#######################################         
            MATCH p=(e)-[:CORR]->(n)
            WHERE n.EntityType="{EntityType}" and n.ID="{entity_id}"
            return p;
            ##############################################################
            '''

    print(qTest)
    tx.run(qCorrelate)


################################## Step 13 ###########################################################

def entities_with_diff_ID_relationships(tx, relation_type, entityID1, entityID2, EntityIDcolumn, MainEntity):
    qCreateRelation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:Entity ) WHERE n1.ID="{entityID1}" AND n1.EntityName="{MainEntity}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:Entity ) WHERE n2.ID="{entityID2}" AND n2.EntityName="{MainEntity}"

                AND n1 <> n2 AND n1.EntityName=n2.EntityName
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n2 )'''
    print(qCreateRelation)

    qTest = f'''
            #########Step13 Testing:######################################
            MATCH ( n1 : Entity ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:Entity )
            return n1,n2;
            ##############################################################
            '''

    print(qTest)

    tx.run(qCreateRelation)


################################## Step 14 ###########################################################

def RelatingReifiedEntitiesAndEntities(tx, relation_type):
    qReifyRelation = f'''
            MATCH ( n1 : Entity ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:Entity )

            MATCH ( en : Entity ) where 
            en.ID=toString(n2.ID)+"_"+toString(n1.ID)
            AND en.EntityType= "{relation_type}"
            AND en.EntityName= n1.EntityName
            AND en.EntityName= n2.EntityName
            AND en.uID="{relation_type}"+toString(n2.ID)+"_"+toString(n1.ID)

            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (en) -[:REL {{Type:"Reified"}}]-> (n2)'''
    print(qReifyRelation)

    qTest = f'''
            #########Step14 Testing:######################################
            MATCH p=(n1:Entity) <-[:REL]- (r:Entity) -[:REL]-> (n2:Entity)
            where r.EntityType="{relation_type}"
            return p;


            MATCH p=(n1:Entity) <-[:REL]- (r:Entity) -[:REL]-> (n2:Entity)
            return p;
            ##############################################################
            '''

    print(qTest)

    tx.run(qReifyRelation)


################################## Step 15 ###########################################################


def correlate_ReifiedEntities_to_Event(tx, derived_entity_type, ID1, ID2, ID, EntityName):
    qCorrelate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REL {{Type:"Reified"}}]- (r:Entity {{EntityType:"{derived_entity_type}"}} )
            CREATE (e)-[:CORR]->(r)'''
    print(qCorrelate)

    qTest = f'''
            #########Step15 Testing:######################################
            MATCH p=(e:Event) -[:CORR]->(n:Entity) <-[:REL]- (r:Entity)
            where r.EntityType="{derived_entity_type}"
            return p;
            #############################################################
            '''

    print(qTest)

    tx.run(qCorrelate)


################################## Step 16 ###########################################################


def createDF(tx, entity_type):
    qCreateDF = f'''
        MATCH ( n : Entity ) WHERE n.EntityType="{entity_type}"
        MATCH ( n ) <-[:CORR]- ( e )

        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second, n.ID as NewID
        MERGE ( first ) -[df:DF {{EntityType:"{entity_type}"}} ]->( second )
        ON CREATE SET df.ID=NewID '''

    print(qCreateDF)

    qTest = f'''
            #########Step16 Testing:######################################
            MATCH  p=(first)-[df:DF]-> (second)
            where df.EntityType="{entity_type}"
            return p;
            #############################################################
            '''

    print(qTest)

    tx.run(qCreateDF)


################################## Step 17 ###########################################################


def deletePuluted_Reified_DF(tx, reifiedEntityType, firstID, secondID, IDcolumn, EntityName):
    qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF {{EntityType: "{reifiedEntityType}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{EntityType: "{EntityName}" }}]-> (e2:Event)
            DELETE df'''

    print(qDeleteDF)

    qTest = f'''
            #########Step17 Testing:######################################
            MATCH (e1:Event) -[df:DF ]-> (e2:Event)
            where ( "{firstID}" in e1.{IDcolumn} and "{secondID}" in e2.{IDcolumn} ) or (  "{secondID}" in e1.ID1 and "{firstID}" in e2.ID1  )
            return e1,e2;
            #############################################################
            '''

    print(qTest)
    tx.run(qDeleteDF)


################################## Step 18 ###########################################################


def deleteExtra_Reified_DF(tx, derived_entity, ID_A, ID_B, ID_Column):
    qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF {{EntityType: "{derived_entity}" }}]-> (e2:Event)
            WHERE "{ID_B}" in e1.{ID_Column} and  "{ID_A}" in e2.{ID_Column} 
            DELETE df;           

            '''

    print(qDeleteDF)

    qTest = f'''
            #########Step18 Testing:######################################
            MATCH (e1:Event) -[df:DF ]-> (e2:Event)
            where ( "{ID_B}" in e1.{ID_Column} and "{ID_A}"  in e2.{ID_Column}) or ( "{ID_A}" in e2.{ID_Column} and "{ID_B}" in e1.{ID_Column} )
            return e1,e2;
            #############################################################
            '''

    print(qTest)
    tx.run(qDeleteDF)


################################## Step 19 ###########################################################


def deletePolluted_CoRR_Reified_Events(tx, derived_entity, ID_A, ID_B, ID_Column, Value):
    qDeleteDF = f'''

            MATCH (e1)-[c1:CORR]->(n)<-[c2:CORR]-(e2) where n.EntityType="{derived_entity}" 
            MATCH (e1:Event) -[df]-> (e2:Event) where df.EntityType <> "{derived_entity}" 
            delete c1,c2;
            '''

    qCorrelate1 = f'''  
            MATCH  (r:Entity {{EntityType:"{derived_entity}"}}) - [:REL {{Type:"Reified"}}]->(n2:Entity)<-[:CORR]- 
            (e1:Event) -[df:DF {{EntityType: "{derived_entity}" }}]->   (e2:Event)   
            -[:CORR]-> (n1:Entity) <-[:REL {{Type:"Reified"}}]- (r:Entity {{EntityType:"{derived_entity}"}} )

            CREATE (e1)-[:CORR]->(r)<-[:CORR]-(e2);  '''

    print(qDeleteDF)
    print(qCorrelate1)

    qTest = f'''
            #########Step19 Testing:######################################
            MATCH p1=(l)-[:HAS]->(e1)-[:CORR]->(n1)  where e1.EventID="e10"
            MATCH p2=(l)-[:HAS]->(e2)-[:CORR]->(n2) where e2.EventID="e91"
            return p1,p2;

            MATCH p1=(l)-[:HAS]->(e1)-[:CORR]->(n1)  
            MATCH p2=(l)-[:HAS]->(e2)-[:CORR]->(n2) 
            return p1,p2;

            #############################################################
            '''

    print(qTest)
    tx.run(qDeleteDF)
    tx.run(qCorrelate1)


################################## Step 20 ###########################################################


def createActivityClasses(tx, DomainValue, domainColTitle):
    if DomainValue:
        qCreateEC = f'''
            MATCH ( e : Event ) where e.{domainColTitle}<>"Unknown"
            WITH distinct e.Activity AS actName, e.{domainColTitle} AS Domain, e.Activity_Synonym as actSyn, e.Activity_Origin AS Origin
            MERGE ( c : Class {{ TypeName:actName, Type:"Activity", ID: actName, Domain: Domain, Syn: actSyn, Origin: Origin}})'''
    else:
        qCreateEC = f'''
            MATCH ( e : Event )
            WITH distinct e.Activity AS actName, e.{domainColTitle} AS Domain, e.Activity_Synonym as actSyn, e.Activity_Origin AS Origin
            MERGE ( c : Class {{ TypeName:actName, Type:"Activity", ID: actName, Syn: actSyn, Origin: Origin}})'''

    print(qCreateEC)

    qTest = f'''
            #########Step20 Testing:######################################
            MATCH (n:Class) 
            RETURN n 
            #############################################################
            '''
    print(qTest)
    tx.run(qCreateEC)


################################## Step 21 ###########################################################


def linkingActivityClassToEvent(tx):
    qLinkEventToClass = f'''
            MATCH ( c : Class ) WHERE c.Type = "Activity"
            MATCH ( e : Event ) WHERE c.TypeName = e.Activity
            CREATE ( e ) -[:OBSERVED]-> ( c )'''


    print(qLinkEventToClass)

    qTest = f'''
            #########Step21 Testing:######################################
            MATCH l=( e ) -[:OBSERVED]-> ( c ) 
            RETURN l
            #############################################################
            '''
    print(qTest)
    tx.run(qLinkEventToClass)


################################## Step 22 ###########################################################


def aggregateDF_Absolute(tx, Ent, EntIDbased):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType AND n.ID = df.ID  AND n.EntityType ="{EntIDbased}"  
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:"Absolute" , count:df_freq , En1_ID:IDT , En1:"{Ent}" , En2:"{Ent}" }}]-> ( c2 ) 
        '''

    print(qCreateDFC)

    Result = tx.run(qCreateDFC).consume().counters
    Neo4J_relationship_massage(Result)

    qTest = f'''
            #########Step22 Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            MATCH rel=( c1 ) -[]-> ( c2 ) where c1.TypeName="A8" and  c2.TypeName="A9"
            return rel;
            #############################################################
            '''
    print(qTest)


################################## Step 23 ###########################################################

def aggregateDF_Relative(tx, En1, En2, En2_OriginColumn, En2_IDColumn, eID):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType  AND n.ID = df.ID 
        AND e1.{En2_OriginColumn} <> "Unknown" 
        AND e2.{En2_OriginColumn}<> "Unknown" 
        AND "{eID}" in e1.{En2_IDColumn}   and "{eID}" in e2.{En2_IDColumn}
        AND n.EntityType<> "{En2}"
        WITH c1,count(df) AS df_freq,c2, n.ID as IDT
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:"Relative" , count:df_freq , En1_ID:IDT , En1:"{En1}" , En2:"{En2}" }}]-> ( c2 ) 
        '''
    print(qCreateDFC)

    Result = tx.run(qCreateDFC).consume().counters
    Neo4J_relationship_massage(Result)

    qTest = f'''
            #########Step23 Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;

            MATCH rel=( c1 ) -[]-> ( c2 ) where c1.TypeName="A8" and  c2.TypeName="A9"
            return rel;

            #############################################################
            '''
    print(qTest)


################################## Step 24 ###########################################################

def aggregateDF_All(tx, list, firstItem):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType  AND n.EntityType in {list}
        WITH c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:"All" , count:df_freq , En1_ID:"0" , En1:"{firstItem}" , En2:"{firstItem}"  }}]-> ( c2 ) 

        '''
    print(qCreateDFC)

    Result = tx.run(qCreateDFC).consume().counters
    Neo4J_relationship_massage(Result)

    qTest = f'''
            #########Step24 Testing:######################################
            MATCH rel=( c1 ) -[:DF_C ]-> ( c2 )
            return rel;


            MATCH rel=( c1 ) -[]-> ( c2 ) where c1.TypeName="A8" and  c2.TypeName="A9"
            return rel;
            #############################################################
            '''
    print(qTest)








