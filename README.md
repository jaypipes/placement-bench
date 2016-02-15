# Scheduler Benchmarking Framework

This repository contains a framework for testing out various scheduler designs
and placement strategies.

## Installation

First, clone this repo and install the dependencies in a virtualenv:

```
$ git clone git@github.com:jaypipes/placement-bench
$ cd placement-bench
$ virtualenv --no-site-packages venv
$ source venv/bin/activate
(venv)$ pip install -r requirements.txt
```

## Running

The command for running the benchmarks is well-documented:

```
(venv)$ python benchmark.py --help
usage: benchmark.py [-h] [--verbose] [--results-file RESULTS_FILE]
                    [--schema {placement,legacy}]
                    [--filter-strategy {db,python}] [--do-claim-in-scheduler]
                    [--placement-strategy {pack,spread,random}]
                    [--partition-strategy {modulo,none}] [--rows ROWS]
                    [--racks RACKS] [--nodes NODES] [--shared-storage-per-row]
                    [--node-ram-mb NODE_RAM_MB]
                    [--node-ram-mb-reserved NODE_RAM_MB_RESERVED]
                    [--node-cpu-cores NODE_CPU_CORES]
                    [--node-ram-allocation-ratio NODE_RAM_ALLOCATION_RATIO]
                    [--node-cpu-allocation-ratio NODE_CPU_ALLOCATION_RATIO]
                    [--workers WORKERS] [--in-requests-file IN_REQUESTS_FILE]
                    [--out-requests-file OUT_REQUESTS_FILE]

optional arguments:
  -h, --help            show this help message and exit
  --verbose             Show more output during run.
  --results-file RESULTS_FILE
                        If set, output results to supplied file instead of
                        stdout. If file exists, will append to the file.
  --schema {placement,legacy}
                        The schema to use for database layout.
  --filter-strategy {db,python}
                        The filter strategy to use for scheduler.
  --do-claim-in-scheduler
                        Perform the claim operation in the scheduler process
                        instead of on the compute node.
  --placement-strategy {pack,spread,random}
                        The placement strategy to use.
  --partition-strategy {modulo,none}
                        The partitioning strategy to use when selecting nodes
                        in the scheduler process.
  --rows ROWS           Number of rows in datacenter.
  --racks RACKS         Number of racks in each row.
  --nodes NODES         Number of nodes in each rack.
  --shared-storage-per-row
                        Should a shared storage pool be created for each row?
  --node-ram-mb NODE_RAM_MB
                        Amount of RAM in MB per node.
  --node-ram-mb-reserved NODE_RAM_MB_RESERVED
                        Amount of RAM in MB per node reserved for the
                        operating system.
  --node-cpu-cores NODE_CPU_CORES
                        Amount of physical CPU cores per node.
  --node-ram-allocation-ratio NODE_RAM_ALLOCATION_RATIO
                        The RAM allocation ratio for each node.
  --node-cpu-allocation-ratio NODE_CPU_ALLOCATION_RATIO
                        The CPU allocation ratio for each node.
  --workers WORKERS     Number of worker processes to simulate scheduler
                        processes.
  --in-requests-file IN_REQUESTS_FILE
                        If set, the set of instance requests is read from the
                        specified YAML file instead of created anew for the
                        run.
  --out-requests-file OUT_REQUESTS_FILE
                        If set, the set of instance requests is written out to
                        a file in YAML format. This can then be read in with
                        the --in-requests-file option when you want to run
                        different scenarios using identical requests sets.
```

# To Do

There are a number of items I need to complete work on:

1. Correct an issue where the `random` placement strategy execution stops
   on the first attempt at placement.

2. Add support for the legacy Nova database schema

3. Emulate claims occurring on the distributed compute nodes instead of in
   the scheduler decision-making process.
