# MR-Twitter
Repository for Fall CS6240 Assignment 1 (Hadoop)

Hadoop MapReduce Twitter Follower Count<br>
Fall 2018

Code author
-----------
Manish Yadav

Installation
------------
These components are installed:<br>
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:<br>
export JAVA_HOME=/usr/lib/jvm/java-8-oracle<br>
export HADOOP_HOME=/home/manish/tools/hadoop/hadoop-2.9.1<br>
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop<br>
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:<br>
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	1) Sufficient for standalone: hadoop.root, jar.name, local.input
	1) Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	1) make switch-standalone		-- set standalone Hadoop environment (execute once)
	1) make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	1) make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	1) make pseudo					-- first execution
	1) make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	1) make upload-input-aws		-- only before first execution
	1) make aws					-- check for successful execution with web interface (aws.amazon.com)
	1) download-output-aws			-- after successful execution & termination

