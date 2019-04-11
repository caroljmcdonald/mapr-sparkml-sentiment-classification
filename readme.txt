
This example runs on MapR 6.1 ,  Spark 2.3.1 and greater

Install and fire up the Sandbox using the instructions here: http://maprdocs.mapr.com/home/SandboxHadoop/c_sandbox_overview.html. 

____________________________________________________________________

Step 1: Log into Sandbox, create data directory, MapR Event Store Topic and MapR Database table:

Use an SSH client such as Putty (Windows) or Terminal (Mac) to login. See below for an example:
use userid: mapr and password: mapr.

For VMWare use:  $ ssh mapr@ipaddress 

For Virtualbox use:  $ ssh mapr@127.0.0.1 -p 2222 

after logging into the sandbox At the Sandbox unix command line:
 
Create a directory for the data for this project

hadoop fs -mkdir /user/mapr/data

____________________________________________________________________

Step 2: Copy the data file to the MapR sandbox or your MapR cluster

 
Copy the data file from the project data folder to the sandbox using scp to this directory /user/mapr/data/uber.csv on the sandbox:

For VMWare use:  $ scp  *.json  mapr@<ipaddress>:/mapr/demo.mapr.com/user/mapr/data/.
For Virtualbox use:  $ scp -P 2222 data/*.json  mapr@127.0.0.1:/mapr/demo.mapr.com/user/mapr/data/.

this will put the data file into the cluster directory: 
/mapr/<cluster-name>/user/mapr/data

____________________________________________________________________

Step 3: To run the code in the Spark Shell:
 
/opt/mapr/spark/spark-2.3.1/bin/spark-shell --master local[2]
 
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"


____________________________________________________________________

Step 4: To submit the code as a spark application: Build project, Copy the jar files

Build project with maven and/or load into your IDE and build. 
You can build this project with Maven using IDEs like Intellij, Eclipse, NetBeans, and then copy the JAR file to your MapR Sandbox, or you can install Maven on your sandbox and build from the Linux command line, 
for more information on maven, eclipse or netbeans use google search. 

This creates the following jar in the target directory.

sparkml-sentiment-1.0.jar

After building the project on your laptop, you can use scp to copy your JAR file from the project target folder to the MapR Sandbox:

From your laptop command line or with a scp tool :

use userid: mapr and password: mapr.

For VMWare use:  $ scp  nameoffile.jar  mapr@ipaddress:/mapr/demo.mapr.com/user/mapr/.

For Virtualbox use:  $ scp -P 2222 target/*.jar  mapr@127.0.0.1:/mapr/demo.mapr.com/user/mapr/.

this will put the jar file into the mapr user home directory: 
/user/mapr



____________________________________________________________________

 To run the application code for Machine Learning Classification


From the Sandbox command line :

/opt/mapr/spark/spark-2.3.1/bin/spark-submit --class machinelearning.Review --master local[2]  /user/mapr/sparkml-sentiment-1.0.jar

This will read  from the file /user/mapr/data/revsporttrain.json

You can optionally pass the file as an input parameter   (take a look at the code to see what it does)

To run the code on a cluster with yarn , you can use :

$SPARK_HOME/bin/spark-submit --class machinelearning.Review --master yarn --deploy-mode client --num-executors 4 --executor-memory 4g --executor-cores 2 /user/mapr/sentimentanalysis/sparkml-sentiment-1.0.jar


____________________________________________________________________

Structured Streaming with MapR Event Store and MapR Database :

use the mapr command line interface to create a stream, a topic, get info and create a table:

maprcli stream create -path /user/mapr/stream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/mapr/stream -topic reviews  

to get info on the  topic :
maprcli stream topic info -path /user/mapr/stream -topic reviews

Create the MapR-DB Table which will get written to

maprcli table create -path /user/mapr/reviewtable -tabletype json -defaultreadperm p -defaultwriteperm p

Run the Streaming code to publish events to the topic:

java -cp ./sparkml-sentiment-1.0.jar:`mapr classpath` streams.MsgProducer

This client will read lines from the file in "/mapr/demo.mapr.com/user/mapr/data/revsportstream.json" and publish them to the topic /user/mapr/stream:reviews. 
You can optionally pass the file and topic as input parameters <file topic> 

Optional: run the MapR Streams Java consumer to see what was published :

java -cp sparkml-sentiment-1.0.jar:`mapr classpath` streams.MsgConsumer 

_____________________________________________________________________________

Run the  the Spark Structured Streaming client to consume events enrich them and write them to MapR Database
(in separate consoles if you want to run at the same time)


/opt/mapr/spark/spark-2.2.1/bin/spark-submit --class stream.StructuredStreamingConsumer --master local[2] \
 sparkml-sentiment-1.0.jar

$SPARK_HOME/bin/spark-submit --class stream.StructuredStreamingConsumer --master yarn --deploy-mode client --num-executors 4 --executor-memory 4g --executor-cores 2 /public_data/sentimentanalysis/sparkml-sentiment-1.0.jar


This spark streaming client will consume from the topic /user/mapr/stream:reviews, enrich from the saved model at
/user/mapr/sentmodel/ and write to the table /user/mapr/reviewtable.

You can optionally pass the  input parameters <topic model table> 
 
You can use ctl-c to stop

In another window while the Streaming code is running, run the code to Query from MapR-DB 

/opt/mapr/spark/spark-2.3.1/bin/spark-submit --class sparkmaprdb.QueryReview --master local[2] \
 sparkml-sentiment-1.0.jar

$SPARK_HOME/bin/spark-submit --class sparkmaprdb.QueryReview --master yarn --deploy-mode client --num-executors 4 --executor-memory 4g --executor-cores 2 /public_data/sentimentanalysis/sparkml-sentiment-1.0.jar

 Use the Mapr-DB shell to query the data

start the hbase shell and scan to see results: 

$ /opt/mapr/bin/mapr dbshell

maprdb mapr:> jsonoptions --pretty true --withtags false

maprdb mapr:> find /user/mapr/reviewtable --limit 5

maprdb mapr:> find /user/mapr/reviewtable --where '{"$and":[{"$eq":{"overall":5.0}},{ "$like" : {"_id":"%B004TNWD40%"} }]}' --f _id,prediction,summary --limit 5

find /user/mapr/reviewtable --where '{"$and":[{"$eq":{"prediction":0.0}},{"$eq":{"label":0.0}} ]}' --f _id,summary, reviewText --limit 5

find /user/mapr/reviewtable --where '{"$and":[{"$eq":{"overall":5.0}},{ "$like" : {"_id":"%B004TNWD40%"} }]}' --f _id,prediction,summary --limit 5


