# Co-purchase analysis

Project for the Scalable and Cloud Programming course at University of Bologna, year 2024-2025.  
A co-purchase analysis implemented with Spark+Scala and run on Google Cloud.  


## Files

*	`doc/` : documents, notes, report, project requirements etc.
*	`out/` : where local execution output will be saved by default
*	`scripts/` : scripts for execution, setup etc., divided in groups
	*	`0.*` : setup
	*	`1.*` : remote execution
	*	`2.*` : remote deletion
	*	`3.*` : remote utils
	*	`4.*` : local execution
*	`src/` :
	*	`main/scala/` : source code
	*	`main/resources/` : dataset(s)
		*	`order_products.csv` : (missing file) the dataset path used in testing
		*	`*` : reduced datasets


## Setup

### Google Cloud

*	create a project
*	enable billing
*	enable APIs (dataproc API, Cloud Resource Manager API)
*	your cluster will have to use a network with firewall rule `default-allow-internal` enabled: the default network, used by default, should be ok
*	it could also be necessary to set `Private Google Access` to `On`, for `default` subnet, in the cluster zone you're going to use (in this project the default is `west-central1`), for `default` VPC network


### sbt

`sbt` is used to compile the project into a Jar, specifically using the `sbt-assembly` plugin (as you can see in `./project/plugins.sbt`).  


## Execution

Everything is managed by scripts (explained hereinafter), which make use of the `gcloud` command line tool.  
They should all be executed from project's root directory (here).  


### Output

The output created by the script (both in local and remote execution) will be a `.csv` file named with a timestamp related to the time of execution.  
A local execution will create a Spark's directory in the `out/` directory with the timestamp name, and then the `out/TIMESTAMP.csv`file.  
A remote execution will create a `TIMESTAMP` Spark directory in the bucket, and the `out/TIMESTAMP.csv` file in the local directory, after the download.  


### Remote

This sequence of commands will manually create the necessary resources (to better manage them), run the job, download the output (as described [above](#output)), and then delete the resources.  

```bash
# If needed, setup the environment variables in `scripts/0.variables.sh`.  

# compile the Jar
./scripts/4.2.local-compile.sh

# create dataproc buckets and cluster
./scripts/1.1.create-buckets.sh
./scripts/1.2.create-dataproc-cluster.sh
# upload Jar and dataset
./scripts/1.3.upload.sh
# run the job
./scripts/1.4.run.sh
# download the output
./scripts/1.5.download.sh

# to avoid ongoing costs, stop the cluster
./scripts/2.2.stop-dataproc-cluster.sh
# if you don't need them anymore, delete the buckets and the cluster
./scripts/2.1.delete-buckets.sh
./scripts/2.3.delete-dataproc-cluster.sh
```

To check the content of the remote bucket, you can use the provided script:
```bash
./scripts/3.1.list.sh
```


### Local execution

You can also run the algorithm locally, after installing Spark (we tested Spark version 3.5.4).  
Here are the commands for the execution:
```bash
# If needed, setup the environment variables in `scripts/0.variables.sh`.

# compile the Jar
./scripts/4.2.local-compile.sh
# run the job
./scripts/4.3.local-run.sh
```

The run scripts will make sure that Spark is running with master and workers (`start-master.sh`, `start-worker.sh` and `start-history-server.sh` for logging), then run the job (`spark-submit`); finally, will create the `.csv` output file from the Spark output files (as described [above](#output)).


### Running with a split dataset (or a different one)

There's also a script to split the original dataset (`src/main/resources/order_products.csv`) in a fraction of it:
```bash
# This will create a dataset with 1/3 of the original size, taking the first 1/3 of the lines.
# The created file will have the same path, and name followed by `_<N>`, with N being the fraction.
./scripts/4.1.split-dataset.sh 3
# This will instead take 1/3 random lines.
# The created file will have the same path, and name followed by `_<N>_random`, with N being the fraction.
./scripts/4.1.split-dataset.sh -r 3
```

Splitting the dataset could be useful to run faster tests, or to measure the weak scalability of the algorithm.  

Then, in order to use the newly created dataset, you need to set it in the `scripts/0.variables.sh` file, in the `PATH_SRC_DATASET` variable; e.g.:
```bash
PATH_SRC_DATASET="${PWD}/src/main/resources/order_products_50.csv"
```


## Implementation

### Other versions of the algorithm

The produced Jar allows to select the version of the algorithm used (among those developed during the project), through its number: the currently selcted one is the last (`10`).  
All the `MapPairsReduce<N>` functions use a similiar implementation, based on `map`, `groupBy`, `reduce`; among the others tested, there are also some returning non-RDD (like the `MapPairsAggregate*`), but they are not scalable, so can't be used with big datasets, like the current one.  


### Execution time

Two execution times will be printed during the execution (either local or remote) (both immediately and at the end of the job): the second one refers to the actual time required for running a Spark action.  


## Extra

### Default Google Cloud resources

Here are the default resources used used for executing the project on Google Cloud. Considering the limit of 8 vCPUs we had, besides the limited choice of machine types, we opted for the `n2-highmem-2`, for both master and workers: this allows up to 3 workers (plus the master) - thus `2x4=8` vCPUs. These machines also have 16GB of memory, which is needed not to give any JVM out-of-memory errors - especially when running with in single-worker, thus in local mode.  
Possible alternatives are `n1-standard-1` (1 vCPU, 3.75GB) and `n2-standard-2` (2 vCPUs, 8GB).  

