# ClinicalEventKnowledgeGraphs (CEKG) 
This repository contains scripts and datasets for building a clinical event knowledge graph locally. The work is related to the paper "Clinical Event Knowledge Graphs: Enriching Healthcare Event Data with Entities and Clinical Concepts," which was accepted at the 6th International Workshop on Process-Oriented Data Science for Healthcare (PODS4H23), held in conjunction with the 5th International Conference on Process Mining (ICPM 2023).


**Citation**
If you use or modify the scripts in this repository for your project, please cite the [Paper](https://doi.org/10.1007/978-3-031-56107-8_23).

**License**
This project is copyrighted by the authors (2023-now).


## Step 1: Start a Graph Database in Neo4j Desktop

To get started, you'll need to set up a Neo4j graph database. Follow these steps to install Neo4j Desktop and configure your local database.

- Go to the [Neo4j Download page](https://neo4j.com/download/) and download Neo4j Desktop. You may need to fill out a form to access the download link.
- After downloading, copy the **Neo4j Desktop Activation Key** when prompted.
- In this example, we used **Neo4j Desktop v1.6.0 for Linux**, but any version of Neo4j Desktop should work.

Once you have installed and opened Neo4j Desktop:

- Click on **"Add"** to create a new database.
- Select **"Local DBMS"** and provide the following details:
   - **Name**: Choose any name for your database.
   - **Password**: Set the password to `12345678`. *(Note: The scripts in this project are configured to use this password.)*
   - **Version**: Select **v5.23.0**. *(We recommend using this version, as other versions may require modifications to the scripts.)*

After entering the details, click **"Create"** and wait for the database to be set up. After the DBMS is created:

- Navigate to the **Plugins** section.
- Find **APOC** (a collection of useful Neo4j procedures and functions).
- Click on **Install** to add the plugin to your database.

Once the APOC plugin is installed:

- Select your newly created database.
- Click **"Start"** to run the database.



## Step 2: Create a Python Project

First, set up the Python project with a virtual environment. Once the virtual environment is activated, install the necessary packages using pip:
```bash
pip install pandas
pip install neo4j
pip install tqdm
pip install graphviz
```

## Step 3: Clone or Download the Repository and Organize the Files in the Python Project

First, download or clone the repository.
- If you are using Linux, download **Data** and **Script_for_Linux**.
- If you are using Windows, download **Data** and **Script_for_Windows**.

Copy all scripts from "Script_for_Linux" (if you are using Linux) or from "Script_for_Windows" (if you are using Windows) and paste them into the root of the project.

Additionally, copy the "Data" directory (not the files inside the "Data" directory) and paste it in the root of the project.

The tree-like structure of your file organization is shown in the following figure:

<img src="./README_resources/0_tree.png" alt="Alt text" width="300" height="100"/>

.
If you are using Linux, place the Linux-specific scripts in a subdirectory named linux_scripts.
If you are using Windows, place the Windows-specific scripts in a subdirectory named windows_scripts.

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
