
**PAGE RANK** 
This repository contains an efficient implementation of the PageRank algorithm using Apache Spark. PageRank is a fundamental algorithm in web analytics and graph theory, originally developed by Google's co-founders Larry Page and Sergey Brin to rank web pages in their search engine results.

Author
-----------
- Pritish Arora

Installation
------------
These components need to be installed first:
- OpenJDK 11
- Python3 (required for AWS CLI installation with homebrew (might not be applicable if not using homebrew))
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

Environment
-----------
1) Depending upon what shell one is using you need to figure out the exact file where the enviroment variables will be set.
I personally used z-shell on my Mac system, therefore I pasted the following path to the file '~/.zshrc'

This is what my '~/.zshrc' file looked like after making all the installations and setting up the environment variables.

	```
	export JAVA_HOME=/opt/homebrew/opt/openjdk@11
	export PATH=$JAVA_HOME/bin:$PATH
	export MVN_HOME=~/apache-maven-3.6.3 
	export PATH=$MVN_HOME/bin:$PATH
	export PATH="/opt/homebrew/opt/scala@2.12/bin:$PATH"
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	export PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3:"$PATH
	export AWS_HOME=/opt/homebrew/opt/awscli@1
	export PATH=$AWS_HOME/bin:$PATH
	```

	Note:
	For JDK and AWS CLI, I have used homebrew to do the installations but for making sure that the right version was installed use commands 'brew search'
	to find the available versions. If the right version is available, be mindful of the location of the installation while setting the path variable.

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

	`export JAVA_HOME=/opt/homebrew/opt/openjdk@11`

Execution
---------
All of the build & execution commands are organized in the Makefile. 
(see note on makefile at the end)
1) Unzip project file or clone the git repository using your preferrable IDE.
2) Open command prompt/terminal.
3) Navigate to directory where project files unzipped/cloned.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws`					-- check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination to download output from s3
	- `download-log-aws`		-- after successful execution & termination to download the logs of the execution from s3
8) Graph size and page rank iteration paramters
	- k 						-- Number of nodes in graph
	- NumOfIterations			-- Number of iterations for PageRank

The Repository contains MapReduce programs to calculate number of triangles in the twitter dataset.

Navigate to src > main > scala > PR
This directory contains folders for all specific java programs namely:
1. PageRankMain -Page Rank implementation with dangling edges


Note - Makefile Configuration
In order to execute any one of the aforementioned java programs on local/AWS, The corresponding jar needs to be compiled.
This compilation part is taken care of within the makefile commands but to specify which java file to compile and upload as jar to AWS,
the path of that specific java file needs to mentioned on the "job.name" argument in the makefile.
(The Makefile also contains these paths already as comments, to execute just uncomment and follow execution steps provided above)

