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

After compilation, all the compiled classes are inside the directory `classes`. Move all the table definition _< tablename >.det_ to `classes`. Enter `classes` directory.


#### 4.1 Create test cases:

Enter _testcases_ directory.




After creating the schema, use _RandomDB_ class to generate serialized schema file _< tablename >.md_ and the data file in text format _< tablename >.txt_ via command:

``
java RandomDB <tablename> <# of records>
``

Once you had the database records in text format in _< table name >.txt_ file, convert the records into object format (_< table name >.tbl_):

``
java ConvertTxtToTbl <table name>
``

The above command also creates another file _< table name >.stat_ that gives the statistics of the table. i.e., number of distinct values for each column.  The format of this file is as below.

~~~~
<# of tuples>
<#of distinct values for col 1> <for col 2>
~~~~

#### 4.2 Test:

Eclipse / IntelliJ : 

- Copy the table schemas and test cases generated in previous step into the root folder.
Run the project with extra parameters _query.in_ _query.out_
 
Command line :

- Copy the table schemas and test cases generated in previous step into the _classes_ folder.
- Run the project
 ``
 java QueryMain _query.in_ _query.out_
 ``
