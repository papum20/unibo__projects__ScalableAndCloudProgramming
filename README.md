# Tutorial Google Cloud



## Steps

Enable APIs for projects:
*	Cloud Resource Manager API
	*	```
		WARNING: Failed to validate permissions required for default service account: <ACCOUNT>.
		Cluster creation could still be successful if required permissions have been granted to the respective service accounts
		mentioned in the document https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#dataproc_service_accounts_2.
		This could be due to Cloud Resource Manager API hasn't been enabled in your project '<PROJECT_NUMBER>' before or it is disabled.
		Enable it by visiting 'https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=<PROJECT_NUMBER>'.
		```
*	dataproc API

Use default network, which has firewall rule `default-allow-internal` enabled.  

### create dataproc buckets

Create manually, so can manage them.  

### create dataproc cluster

set `Private Google Access` to `On`, for `default` subnet in `west-central1` for `default` VPC network.  

*	CPU limit: 8;
*	usage of n2-highmem-2 is 2 vCPUs, 16GB
*	current usage: 1 master 1x2vCPUs + 3 workers 3x2vCPUs = 8 vCPUs
*	alternative: n1-standard-1 (1 vCPU, 3.75GB)

## TODO

tests:
* main2 w/ full dataset (ca. 0h20)
* mine w/ new cpus, full dataset (just time, no matter google fs errors) (12h45-)
* fix mine accessing google storage fs (delete if exists not working)
* compare time main/main2
* could be sc/sparkSession
