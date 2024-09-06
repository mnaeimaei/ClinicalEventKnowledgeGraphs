# ClinicalEventKnowledgeGraphs (CEKG) 
This repository contains scripts and datasets for building a clinical event knowledge graph locally. The work is related to the paper "Clinical Event Knowledge Graphs: Enriching Healthcare Event Data with Entities and Clinical Concepts," which was accepted at the 6th International Workshop on Process-Oriented Data Science for Healthcare (PODS4H23), held in conjunction with the 5th International Conference on Process Mining (ICPM 2023).


**Citation
If you use or modify the scripts in this repository for your project, please cite the [Paper](https://doi.org/10.1007/978-3-031-56107-8_23).

**License**
This project is copyrighted by the authors (2023-now).




Authors: 
Milad Naeimaei Aali
Norwegian University of Science and Technology, Trondheim, Norway

Felix Mannhardt
Eindhoven University of Technology, Eindhoven, the Netherlands

Pieter Jelle Toussaint
Norwegian University of Science and Technology, Trondheim, Norway



Copyright (C) 2023-now

Milad Naeimaei Aali, Norwegian University of Science and Technology, Trondheim, Norway  
Felix Mannhardt, Eindhoven University of Technology, Eindhoven, the Netherlands  
Pieter Jelle Toussaint, Norwegian University of Science and Technology, Trondheim, Norway  


## Description ##
This repository consists of adopted code for discovering care pathways (using event graph representation) for patients with multi-morbidity and involves Python script and Neo4j library.

## Publication ## 

    Under review


## Installation ## 
1. Installing Python  
2. Install packages in Python  
neo4j, pandas, time, os, CSV, graphviz  
3. Installation Neo4j
Install Neo4j Desktop 1.5.0 Neo4J Download Center  
4. Installation Graphviz  
For Ubuntu:  
sudo apt-get update -y  
sudo apt-get install -y graphviz  
5. Installation xdot  
For Ubuntu:  
sudo apt-get update -y  
sudo apt-get install -y dot  
## How to use ## 
1. Creating a project in Python consist all files in **CEKG_project** 
2. Creating a project in Neo4j Desktop
3. add Locl DBMS with version 4.4.11, recommended password: 1234; then, in the project setting, allocate enough memory to the database, such as DBMS.memory.heap.max_size=20G
4. start Neo4J DBMS
5. Update the **Neo4JImport** variable in **Step1..py to Step11...** in Python, the same as you created the DBMS import directory. To find which directory it is: Click three dots of your created DBMS, Then open the folder, and finally import
6. Run **Step1..py to Step11...** in python to create Clinical event knowledge graph
7. Open created DBMS Consule in Neo4j and Run Queries in **CEKG_Queries** Directory
