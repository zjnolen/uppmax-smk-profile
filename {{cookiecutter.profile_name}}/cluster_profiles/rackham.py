from math import ceil
from snakemake.logging import logger

def resource_conversion(arg_dict):
	"""
	Adjusts sbatch arguments to be compatible with Rackham, where memory 
	arguments don't allocate memory, rather memory is allocated through 
	CPUs
	"""

	partition = arg_dict.get("partition", "core")
	constraint = arg_dict.get("constraint", None)
	ncpus = int(arg_dict.get("cpus-per-task", 1))
	mem = int(arg_dict.get("mem", 6400*ncpus))

	# Cap memory at 1TB, the max Rackham can take.
	if mem > 1000000:
		logger.info(
			f"WARNING: Requested memory ({mem}MB) exceeds maximum possible on "
			"Rackham. Capping memory to 1000GB. If this was likely an "
			"erroneous resource allocation, and your job will need "
			"considerably less memory, please consider editing the rule "
			"resources in the profile configuration to more appropriate "
			"values."
		)
		mem = int(1000000)
	
	# See if job can fit on the core partition
	if mem <= 128000:
		partition = "core"
		# Adjust cores if needed to meet memory requirements
		if mem > 6400*ncpus:
			logger.info(
				f"NOTE: Memory ({mem}MB) exceeds allotment given by Rackham "
				"for requested threads (6.4GB/CPU). Increasing CPU request to "
				"reserve requested memory."
			)
			ncpus = int(ceil(mem/6400))
		mem = int(ncpus*6400)
	# If not, see if it fits on mem256GB
	elif mem <= 256000:
		logger.info(
			f"NOTE: Job will use {mem}MB of memory, constraining to nodes "
			"with 256GB of memory."
		)
		partition = "node"
		constraint = "mem256GB"
		mem = int(256000)
	# If not there, give it to mem1TB
	elif mem > 256000:
		logger.info(
			f"NOTE: Job will use {mem}MB of memory, constraining to nodes "
			"with 1TB of memory."
		)
		partition = "node"
		constraint = "mem1TB"
		mem = int(1000000)
	
	# Check if job is going to exceed the max CPUs on any single node
	if ncpus > 20:
		logger.info(
			"WARNING: Greater than 20 threads requested, capping at 20. "
			"Rackham does not permit greater than 20 cores to be requested "
			"in a shared memory process. Likely, if you need more threads "
			"then this is expecting or should be set as an MPI job, however "
			"that is not supported in this Rackham specific profile currently."
		)
		ncpus = 20	
	
	adjusted_args = {}
	adjusted_args["partition"] = partition
	if constraint:
		adjusted_args["constraint"] = constraint
	adjusted_args["cpus-per-task"] = ncpus
	adjusted_args["mem"] = 0	# Why 0? This should be shorthand for maxing 
								# out the memory that the node/cpus allow.
	arg_dict.update(adjusted_args)
	return arg_dict