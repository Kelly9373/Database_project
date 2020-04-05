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


### 2. Environment Setup

1. Create a folder named _classes_ under the root folder which serves as the output directory for the project.

2. In project root folder, create a system variable COMPONENT pointing to the root directory:

    Linux / Mac : execute command 
 ``
 source queryenv
 ``
 
    Windows : follow the [guide](http://www.comp.nus.edu.sg/~tankl/cs3223/project/cs3223-proj-setup.htm)

    Eclipse (other IDEs such as IntelliJ are similar) : follow the [guide](http://www.comp.nus.edu.sg/~tankl/cs3223/project/cs3223-proj-setup.htm)

 
### 3. Compilation

Windows : execute _build.bat_ file

Linux / Mac : execute _build.sh_ file via 
``
./build.sh
``

### 4. Testing Procedure

Guide is taken from [official instruction](http://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm).


#### 4.1 Create test cases:

Enter _testcases_ directory.

Write Table Schema in customized file:  _< tablename >.det_

Format for schema:
~~~~
<# of columns>
<size of tuple>
<attribute name> <data type> <range> <key type> <column size>
~~~~

Explanation:

data type: 

- either INTEGER / STRING / REAL

range:
    
- For integers, and real, it considers the values between 0 and the range specified. 
- For primary keys, provide range value greater than or equal to number of records.
- For strings, just specify number of characters you string need to contain.           

Key type:

- May be PK/FK/NK respectively for primary key, foreign key, not a key.  
- Use only integer data type for primary key.
 
Attribute size:

- It is number of bytes required for the attribute. 
- In java, integer is 4 bytes, character is 2 bytes.

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
