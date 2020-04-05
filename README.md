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


Windows (for both Windows 7 and 10):    
    1. Go to "System Property" -> "Advanced"-> "Environment Variables" (alternatively, you may use the search function on the taskbar and type "Environment Variables", select "Edit the system environment variables" -> "Advanced" -> "Environment Variables") 
    2. In the upper panel "User variables for xxx" where xxx is your Windows username, click "New...". Variable Name: COMPONENT Variable Value: directory to your component (eg: D:\COMPONENT)
    3. In the upper panel "User variables for xxx", find the variable "CLASSPATH". If found, click "Edit..." otherwise click "New..." 
    4. Add/append the following to the variable "CLASSPATH" : %COMPONENT%;%COMPONENT%\classes;%COMPONENT%\lib 
    5. Save the change, and start a new command line window. You may test: echo %CLASSPATH% to see whether the setup takes effect or not. 

Eclipse : 
    1. "File" -> "Open Projects from File System".
    2. Set "Import source" to the path of the unarchived folder of COMPONENT, and press "Finish" button.
    3. Right-click the project (i.e., COMPONENT) from the Package Explorer, and choose "Properties".
    4. In the properties windows, press "Add Folder" to add "src" as a Source Folder from "Java Build Path" -> "Source".
    5. In the properties windows, press "Add External Class Folder" to add "lib" from "Java Build Path" -> "Libraries".
    6. Press "Apply and Close" on the properties windows. Now, you should see no more red underlines in the source code. You are ready to go. 

Intellij :
    1. Select Import Project from the COMPONENT folder (or any other name you used for the project) and locate your project folder.
    2. Select "Create project from existing sources" option and keep clicking "Next" until it finishes. (make sure the jdk version is not lower than 1.8).
    3. Run build.bat to build the project so that the essential folders can be created.
    4. Go to Project Structure > Modules:
        4.1 Mark src and testcases folder as Sources;
        4.2 Mark test folder as Tests;
        4.3 Mark classes folder and out folder as Excluded.


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
