# CS3223:   Database Systems Implementation

### 1. Project Description

Based on the given project template, our group has implemented the following features for the simple SPJ (Select, Project & Join) query engine:
1. Block Nested Loops Join
2. Sort Merge Join
3. K-way merge sorting
4. DISTINCT clause
5. GROUPBY clause
6. LIMIT clause
7. OFFSET clause
8. Randomized optimizer


### 2. Environment Setup and Compilation
Java 9 is used in this project

Mac OS : execute _build.sh_ file via 
``
./build.sh
``

Windows : execute _build.bat_ file


### 3. Testing 

After compilation, all the compiled classes are inside the directory `classes`. Move all the table definition _< tablename >.det_ to `classes`. Enter `classes` directory and execute the following commands:

``
cd classes
``

``
java RandomDB <tablename> <# of records>
``

``
java ConvertTxtToTbl <table name>
``

After all the testing data ready, we execute query by the following commands:

 ``
 java QueryMain <queryfilename> <resultfile> <pagesize> <numbuffer>
 ``
 
 The query result will be inside the `<resultfile>`
