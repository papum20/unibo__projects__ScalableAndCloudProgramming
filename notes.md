# NOTES

## Evaluation

Strong scalability : given an input, times reduce when adding more resources  
*	e.g.:
	*	start with 1 worker, measure wall clock time
	*	try with 2 workers : if time halved, strong scalability at max, if time the same, nothing, usually in the middle
	*	increase... (max 4, with google cloud plan)

weak scalability : while adding resources, also increase work load (i.e. increase dataset size)  
*	e.g.: double resources, double input size
	*	measure : expect time to be the same
		*	usually takes (a little) longer

wall clock time : physical time from start to end of a task  


## code

e.g.: PageRank sol :
*	`val conf = ` ... `.setMaster("local[*]")` : run in parallel on local machine, using all available cores  
*	`.setMaster("local[2]")` : use 2 cores  

