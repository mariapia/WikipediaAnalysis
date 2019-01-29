# Wikipedia analysis through the use of Graphs

The project has been developed as exam for the Big Data course at University of Trento. The aim of the project is to process the Wikipedia's history dataset and anlyze the results through the use of graphs and queries.

The entire code has been written in Java, so to run it is necessary to have the last versiona of Java installed, that is the **Java 8.**
The project is built on top of **Apache Spark** and is executed with the comand spark ```spark-submit```, so the **Spark 2.2.1** version should run correctly on your computer.
The data processed by the App class are stored into HDFS and consequently the graphs generated and anlyzed are located on Neo4j local database. **Hadoop 2.8.3** sould be properly configured and also the **Neo4j community edition**.

The dataset, that can be found in the directory *dataset* has to be loaded on your local HDFS file system and the path has to be put on the *main.sh* file in replace of```<input-path>```. In your file system, there should be a directory that will contain the output. This path must end with the ```/``` symbol. The path must be replaced instead of ```<output-path>```.
In the *main.sh* replace <hadoop-bin-hdfs> path with your own path of the ```hadoop-2.8.3/bin/hdfs``` directory.

To execute the code on your computer, run the command ```./main.sh ```after editing correctly the properly fields. This script executes the pre-processing and generate the graph. To visualize the graphs open the url ``` http://localhost:7474``` on your browser and on the bar write the queries.




