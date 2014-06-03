Machine Learning **SPIKE**

This is a Mahout-Hadoop-Mongo-Camel-SQL machine learning structure for clustering customer data coming from different places.

This is tested to run only in Mac OSX

##Steps

- Make sure you have your **Hadoop** cluster running. [Follow this instructions](https://github.com/calo81/vagrant-hadoop-cluster)
- Make sure all virtual machines in the virtual cluster can communicate with each other using `ssh`. With the **root** user.
- Make sure you have **Hadoop 1.2.1** installed in `~/Programs/hadoop-1.2.1/bin/hadoop`
- Make sure you have **Mahout 0.9** installed in `~/Programs/mahout-distribution-0.9`
- Build this project (*clustering*) by running `mvn clean install` in the root path of the project.
- Make sure you configure the environment variables required:
  -`HADOOP_HOME=/Users/cscarion/Programs/hadoop-1.2.1`
  -`HADOOP_CONF_DIR=/Users/cscarion/projects/vagrant-hadoop-cluster/modules/hadoop/files/`
- Extract the data from the datasources
  - Use [Sqoop](http://sqoop.apache.org/) for extracting the data from your SQL database to HDFS. Make sure you download a **Sqoop** version compatible with the installed **Hadoop** version which is 1.2.1.
    - Copy the JDBC driver for your DB (in my case SQL Server) into the **lib** directory of the **sqoop** installation directory
    - `sqoop import --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --connect "jdbc:sqlserver://xxx:1433;username=xxx;password=xxx;databaseName=xx" --query 'select top 10 * from xxx a where a.x is not null and $CONDITIONS' --split-by  a._id --as-sequencefile --fetch-size 20 --delete-target-dir --target-dir importedCustomers -package-name "clustercustomers.sqoop" --null-string '' --fields-terminated-by ','`
    - Then copy the generated `QueryResults.java` (in the folder **clustercustomers.sqoop**) file to the **clustering** project under the correct package. This is needed as the Sequence File generated uses this class for the serialization/deserialization. So when we run the next map reduce tasks this file needs to exist.
    - Also copy the generated sequence files to the remote **Hadoop** cluster if you generated it with **Hadoop** locally as myself: `~/Programs/hadoop-1.2.1/bin/hadoop fs -put importedCustomers hdfs://192.168.1.10:9000/user/cscarion/aggregated_customers`
    - You can also generate a non SequenceFile file like: `sqoop import --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --connect "jdbc:sqlserver://bi02lon:1433;username=xxxx;password=xxxx;databaseName=IHubODS" --query 'xxxx where $CONDITIONS' --split-by  customer._id --fetch-size 20 --delete-target-dir --target-dir importedCustomersText --package-name "clustercustomers.sqoop" --null-string '' --null-string '' --fields-terminated-by '|'`. This one will be used for the aggregations at the end of the whole process, as it is easier to use from **Apache Pig**
    -Then of course copy this file as well to **HDFS**: `~/Programs/hadoop-1.2.1/bin/hadoop fs -put importedCustomersText hdfs://192.168.1.10:9000/user/cscarion/aggregated_customers_text`
    - For running both **sqoop** tasks, you must unset `HADOOP_CONF_DIR` as if not it will try to run the jobs in the Hadoop cluster and those virtual machines have no access to the Database. That is in my case.
    - HADOOP_CONF_DIR must also be unset for the running of the `hadoop fs -put` command.
  - Alternatively execute the program `clustercustomers.camel.Routing` which extracts the data from the MongoDB inserts it into HDFS.
  - This program can be run directly from the IDE or any means to run the main class.
- Aggregate the data (***IMPORTANT:*** Do this part only if you didn't run the **Scoop** version as that one gets the needed aggregated data with a join)
  - Execute the program `clustercustomers.hadoop.CustomerMapReduce` which takes the different customer files from HDFS, aggregates them and creates a new file in HDFS with all the aggregated data.
  - This execution currently generates the **Hadoop** directory `hdfs://192.168.1.10:9000//user/cscarion/aggregated_customers`
  - To execute this program you have to do: `~/Programs/hadoop-1.2.1/bin/hadoop jar target/cluster_customers-1.0-SNAPSHOT.jar clustercustomers.hadoop.CustomerMapReduce`
- Convert the file to a **Vectorized** `SequenceFile` which is the format that **Mahout** understands.
  - Execute the program `clustercustomers.hadoop.VectorCreationMapReduce`
  - To execute this program you have to configure the environment variable: `export HADOOP_CLASSPATH=/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar:/Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar`
  - Then tu run the job against the cluster you do: `~/Programs/hadoop-1.2.1/bin/hadoop jar target/cluster_customers-1.0-SNAPSHOT.jar clustercustomers.hadoop.VectorCreationMapReduce -libjars $PWD/target/lib/mahout-math-0.9.jar,$PWD/target/lib/mahout-core-0.9.jar,$PWD/target/lib/commons-lang3-3.1.jar,$PWD/target/lib/sqoop-1.4.4-hadoop100.jar`
  - In both previous steps `$PWD` is the **clustering** project root path.
- Cluster the data.
  - Again you have to set all the environment variables including: `export HADOOP_CLASSPATH=/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar:/Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar`
  - To cluster the customer data in the **Hadoop** cluster you run: `~/Programs/hadoop-1.2.1/bin/hadoop jar /Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar org.apache.mahout.clustering.kmeans.KMeansDriver -libjars /Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar,/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar -i /user/cscarion/vector_seq_file/part-m-00000 -c customer-clusters -o customer-kmeans -dm clustercustomers.mahout.CustomWeightedEuclideanDistanceMeasure -x 10 -k 3 -ow --clustering`
  - The previous command is run from the **Mahout** directory: `~/Programs/mahout-distribution-0.9`
  - You can also run a double step clustering. This is better as the centroids are not default ones. To do this you first run the **Canopy** clustering like:
    - `/Users/cscarion/Programs/hadoop-1.2.1/bin/hadoop jar /Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar org.apache.mahout.clustering.canopy.CanopyDriver -libjars /Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar,/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar -i /user/cscarion/vector_seq_file/part-m-00000 -o customer-centroids -dm clustercustomers.mahout.CustomWeightedEuclideanDistanceMeasure -t1 0.70 -t2 0.59`
  - Then you run the **kmeans** using those centroids:
    - `~/Programs/hadoop-1.2.1/bin/hadoop jar /Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar org.apache.mahout.clustering.kmeans.KMeansDriver -libjars /Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar,/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar -i /user/cscarion/vector_seq_file/part-m-00000 -c customer-centroids/clusters-0-final -o customer-kmeans -dm clustercustomers.mahout.CustomWeightedEuclideanDistanceMeasure -x 10 -ow --clustering`
- Check the created clusters visually
  - Run the command `bin/mahout clusterdump -o ~/graph.graphml  --input /user/cscarion/customer-kmeans/clusters-1-final --pointsDir /user/cscarion/customer-kmeans/clusteredPoints -of GRAPH_ML`
  - The previous command will generate a graph file that can be opened in a tool like **Graphviz**.
  - The previous command is run from the **Mahout** directory: `~/Programs/mahout-distribution-0.9`
  - For simple stdout outpu run the command like: `bin/mahout clusterdump --input /user/cscarion/customer-kmeans/clusters-1-final --pointsDir /user/cscarion/customer-kmeans/clusteredPoints`
- Generate and Get files with the cluster-customer_id mapping:
  - Run the **hadoop** job: `~/Programs/hadoop-1.2.1/bin/hadoop jar target/cluster_customers-1.0-SNAPSHOT.jar clustercustomers.hadoop.ClusterOutputIndividualFilesPerClusterMapReduce -libjars $PWD/target/lib/mahout-math-0.9.jar,$PWD/target/lib/mahout-core-0.9.jar,$PWD/target/lib/commons-lang3-3.1.jar`
  - That will give output to individual files per cluster as well as a file with all the clusters and the customers ids for each.
Generate individual Centroid files and one file with all Cluster centroids:
  - Run the **hadoop** job: `~/Programs/hadoop-1.2.1/bin/hadoop jar target/cluster_customers-1.0-SNAPSHOT.jar clustercustomers.hadoop.ClusterCentroidsToIndividualFilesMapReduce -libjars $PWD/target/lib/mahout-math-0.9.jar,$PWD/target/lib/mahout-core-0.9.jar,$PWD/target/lib/commons-lang3-3.1.jar`
  - That execution will output one file for each cluster centroid as well as a file with all the centroids with their cluster id.
- Adding some fake questions for the users to find the most common questions later: `~/Programs/hadoop-1.2.1/bin/hadoop jar target/cluster_customers-1.0-SNAPSHOT.jar clustercustomers.hadoop.fakes.AddingFakeQuestionsToUsersMapReduce -libjars $PWD/target/lib/mahout-math-0.9.jar,$PWD/target/lib/mahout-core-0.9.jar,$PWD/target/lib/commons-lang3-3.1.jar`  
- Running your aggregations with **Pig**
  - Download and install [Apache Pig](http://pig.apache.org/). I am using in my example **pig-0.12.1**.
  - Set the environment variables HADOOP_HOME and HADOOP_CONF_DIR as before, so **Pig** knows it will connect to the remote Hadoop.
  - Firts I'll show an example of running some **Pig** interactively with the **Grunt** shell:
    - Run the file `$PIG_HOME/bin/pig`
    - The next steps are run on the `grunt>` prompt.
    - Now load the aggregated customer data: `customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id);`
    - Next load the clustered data: `cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);`
    - Next do a grouping of the two by the customer id: `groupCluster = COGROUP customers BY id, cluster BY customerId INNER;`
    - Flatten the values into maneagble structure: `flattened = FOREACH groupCluster GENERATE FLATTEN(cluster), FLATTEN(customers);`
    - Get only the required fields for the calculation. This example shows using claims: `neededForAggrregate = FOREACH flattened GENERATE cluster::clusterId, cluster::customerId, customers::claims;`
    - Group by cluster id: `grouped = GROUP neededForAggrregate BY cluster::clusterId`
    - Get the claims: `claims = FOREACH grouped GENERATE group, AVG(neededForAggrregate.customers::claims);`
    - See the results: `DUMP claims;`
    - Now for the seeing the Premium average is similar. But this time we have to export the `quote` table information from `IHub` to `HDFS` as well. then combine it. Like this:
      - Run the corresponding `Sqoop` Something like the following but with the actual data: `sqoop import --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --connect "jdbc:sqlserver://xxx:1433;username=xxx;password=xxx;databaseName=IHubODS" --query "SELECT rfq, premium, insurer FROM quotes where \$CONDITIONS and premium is not null and insurer is not null and state='purchased'" --split-by  rfq --fetch-size 20 --delete-target-dir --target-dir importedQuotes --package-name "clustercustomers.sqoop.quotes" --null-string '' --fields-terminated-by '|'`  
      - Copy the files generated to the remote HDFS: `~/Programs/hadoop-1.2.1/bin/hadoop fs -put importedQuotes hdfs://192.168.1.10:9000/user/cscarion/imported_quotes`
      - Then some more **Pig**
      
```
premiums = LOAD '/user/cscarion/imported_quotes' USING PigStorage('|') AS(rfq_id, premium, insurer);
cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id);

withPremiums = JOIN premiums BY rfq_id, customers BY rfq_id;

rmf /user/cscarion/withPremiums;

store withPremiums into 'withPremiums' using PigStorage('|');

groupCluster2 = JOIN withPremiums BY customers::id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY cluster::clusterId;

premiumsAverage = FOREACH grouped2 GENERATE group, AVG(groupCluster2.withPremiums::premiums::premium);

rmf /user/cscarion/premiumsAverage;

STORE premiumsAverage into 'premiumsAverage' using PigStorage('|');

```

  - Now let's get the most common Insurer used with PIG:
  
```
quotes = LOAD '/user/cscarion/imported_quotes' USING PigStorage('|') AS(rfq_id, premium, insurer);
cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id);

withInsurers = JOIN quotes BY rfq_id, customers BY rfq_id;

groupCluster2 = JOIN withInsurers BY customers::id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY (cluster::clusterId, withInsurers::quotes::insurer);

countOfInsurer = FOREACH grouped2 GENERATE group, COUNT(groupCluster2.withInsurers::quotes::insurer) AS counted;

flattenedCount = FOREACH countOfInsurer GENERATE FLATTEN(group), counted;

STORE flattenedCount INTO 'flattenedCount' USING PigStorage('|');

flattenedCount = LOAD 'flattenedCount' USING PigStorage('|') AS (cluster, insurer, count:int);

commonInsurer = GROUP flattenedCount BY(cluster);

commonInsurerAggregate = FOREACH commonInsurer {
   elems = ORDER flattenedCount BY count DESC;
   one = LIMIT elems 1;
   GENERATE group, FLATTEN(one.insurer), FLATTEN(one.count);
 };

rmf /user/cscarion/commonInsurerAggregate

STORE commonInsurerAggregate into 'commonInsurerAggregate' using PigStorage('|');

```

  - Now let's get the most common Questions for each cluster with PIG:
 
```
cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text_questions' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id, question);

groupCluster2 = JOIN customers BY id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY (cluster::clusterId, customers::question);

countOfQuestions = FOREACH grouped2 GENERATE group, COUNT(groupCluster2.customers::question) AS counted;

flattenedCount = FOREACH countOfQuestions GENERATE FLATTEN(group), counted;

rmf /user/cscarion/flattenedCountOfQuestions

STORE flattenedCount INTO 'flattenedCountOfQuestions' USING PigStorage('|');

flattenedCount = LOAD 'flattenedCountOfQuestions' USING PigStorage('|') AS (cluster, question, count:int);

commonQuestion = GROUP flattenedCount BY(cluster);

commonQuestionAggregate = FOREACH commonQuestion {
   elems = ORDER flattenedCount BY count DESC;
   one = LIMIT elems 1;
   GENERATE group, FLATTEN(one.question), FLATTEN(one.count);
 };

rmf /user/cscarion/commonQuestionAggregate

STORE commonQuestionAggregate into 'commonQuestionAggregate' using PigStorage('|');

```
  - All Pig jobs have a script in the **pig** folder on the **clustering** project. They can be run like `$PIG_INSTALLATION_DIR/bin/pig pig/average_premium.pig`. Remember that both **HADOOP_HOME** and **HADOOP_CONF_DIR** must be set before tunning the command for them to be executed against the cluster.
  
##Utilities and notes. IMPORTANT STUFF

- Reading Sequence Files.
  - Most files that we generate in **Hadoop** are Sequence Files. You can read them using the program `clustercustomers.hadoop.SequenceFileReader`. Modify it as needed and run it normally. Take into account that you have to use the proper `Key` and `Value` classes when reading the sequence files. They need to match whatever the Sequence File has. You can read the SequenceFile with `hadoop dfs -cat` and see at the top what Key and value is expected. Then use those in the `SequenceFileReader` class.
- Some `hadoop` commands to be run in the master node:
  - Delete a directory or file: `./hadoop dfs -rmr /user/cscarion/vector_seq_file`
  - List contents of directory: `./hadoop dfs -ls /user/cscarion/vector_seq_file`
  - Read content of file: `./hadoop dfs -ls /user/cscarion/vector_seq_file/part-r-00000`
- In the URL **http://master:50030/jobtracker.jsp** you will see the running tasks of your cluster.
- The final cluster files are generated on `./hadoop dfs -ls /user/cscarion/customer-kmeans`
- Remember to have `HADOOP_CLASSPATH=/Users/cscarion/Programs/mahout-distribution-0.9/mahout-examples-0.9-job.jar:/Users/cscarion/projects/clustering/target/cluster_customers-1.0-SNAPSHOT.jar`
- If you shutdown or restart the **master** virtual machine, remember to rerun the hadoop format command on it. If not the **namenode** daemon doesn't start:
  - `bin/hadoop namenode -format`
- If you copy the **jar** file *mahout-examples-0.9-job.jar* to the **hadoop** machines, in the *lib* directory of the **hadoop** installation you don't need to pass it in the **-libjars** parameter of the `hadoop` command. The branch *mahout_tests* of the **vagrant-hadoop-cluster** project copies this file to the *Hadoop* machines.
- The full **Sqoop** commands are in my **notes** file where I keep my stuff.