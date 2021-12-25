PARQUETIZER:
The project intends to provide a way to get the OpenStreetMap data available in a Big Data friendly format as Parquet.

Currently any PBF file is converted into three parquet files, one for each type of entity from the original PBF (Nodes, Ways and Relations).

In order to get started:

git clone https://github.com/adrianulbona/osm-parquetizer.git
cd osm-parquetizer
mvn clean package
java -jar target/osm-parquetizer-1.0.1-SNAPSHOT.jar path_to_your.pbf
For example, by running:

java -jar target/osm-parquetizer-1.0.1-SNAPSHOT.jar romania-latest.osm.pbf
In a few seconds (on a decent laptop) you should get the following files:

-rw-r--r--  1 adrianbona  adrianbona   145M Apr  3 19:57 romania-latest.osm.pbf
-rw-r--r--  1 adrianbona  adrianbona   372M Apr  3 19:58 romania-latest.osm.pbf.node.parquet
-rw-r--r--  1 adrianbona  adrianbona   1.1M Apr  3 19:58 romania-latest.osm.pbf.relation.parquet
-rw-r--r--  1 adrianbona  adrianbona   123M Apr  3 19:58 romania-latest.osm.pbf.way.parquet
The parquet files have the following schemas:

node
 |-- id: long
 |-- version: integer
 |-- timestamp: long
 |-- changeset: long
 |-- uid: integer
 |-- user_sid: string
 |-- tags: array
 |    |-- element: struct
 |    |    |-- key: string
 |    |    |-- value: string
 |-- latitude: double
 |-- longitude: double

way
 |-- id: long
 |-- version: integer
 |-- timestamp: long
 |-- changeset: long
 |-- uid: integer
 |-- user_sid: string
 |-- tags: array
 |    |-- element: struct
 |    |    |-- key: string
 |    |    |-- value: string
 |-- nodes: array
 |    |-- element: struct
 |    |    |-- index: integer
 |    |    |-- nodeId: long

relation
 |-- id: long
 |-- version: integer
 |-- timestamp: long
 |-- changeset: long
 |-- uid: integer
 |-- user_sid: string
 |-- tags: array
 |    |-- element: struct
 |    |    |-- key: string
 |    |    |-- value: string
 |-- members: array
 |    |-- element: struct
 |    |    |-- id: long
 |    |    |-- role: string
 |    |    |-- type: stringur.pbf


PYSPARK and SEDONA:
PYSPARK:
Before installing pySpark, you must have Python and Spark installed. I also encourage you to set up a virtualenv
To install Spark, make sure you have Java 8 or higher installed on your computer. Then, visit the Spark downloads page. Select the latest Spark release, a prebuilt package for Hadoop, and download it directly.

Unzip it and move it to your /opt folder:

$ tar -xzf spark-x.x.x-bin-hadoop2.4.tgz 	- replace x.x.x with your version

$ mv spark-1.2.0-bin-hadoop2.4 /opt/spark-x.x.x   

export SPARK_HOME=/opt/spark-x.x.x 
export PATH=$SPARK_HOME/bin:$PATH


Install Jupyter notebook: THIS IS NOT REQUIRED!

$ pip install jupyter
You can run a regular jupyter notebook by typing:

$ jupyter notebook

CHECK IF PYSPARK IS WORKING:
$ pyspark    		-- you should see following:
"""""""""""""""""""""""""""""""
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.1.0
      /_/
Using Python version 3.5.2 (default, Jul  2 2016 17:53:06)
SparkSession available as 'spark'.
""""""""""""""""""""""""""""""""

Configure to work with Jupyter Notebook:

$ pip install findspark
Launch a regular Jupyter Notebook:

$ jupyter notebook
Create a new Python [default] notebook and write the following script:

import findspark
findspark.init()

TEST IN JUPYTER NOTEBOOK:

import random
num_samples = 100000000

def inside(p):     
  x, y = random.random(), random.random()
  return x*x + y*y < 1

count = sc.parallelize(range(0, num_samples)).filter(inside).count()

pi = 4 * count / num_samples
print(pi)

sc.stop()

If it gives output, pyspark is working!

INSTALL SEDONA:

install pyspark with hadoop and run it 
after it's running successfully

git clone https://github.com/apache/incubator-sedona/
https://sedona.apache.org/setup/compile/ 	BELLOW STEPS ARE FROM THIS LINK 
in root directory of incubator-sedona run:

mvn clean install -DskipTests -Dgeotools					this will take a while
cp python-adapter/target/sedona-python-adapter-xxx.jar SPARK_HOME/jars/		replace-xxx with your version 
try to run initialization, if everything goes well, you're ready to go!

SET BUFFER MEMORY: 
setting it in the properties file with:
nano $SPARK_HOME/conf/spark-defaults.conf
spark.driver.memory              20g
spark.kryoserializer.buffer.max  1g


GET OSM .pbf files:
you can download those from planet.osm, geofabrick.de, using pyrosm package
for instllation of pyrosm, follow this:
https://pyrosm.readthedocs.io/en/latest/installation.html

HOW I INSTALLED IT:
conda create -n osm python=3.9
conda activate osm
conda install -c conda-forge pyrosm
pip install --ignore-installed cykhash==1.0.2





#
