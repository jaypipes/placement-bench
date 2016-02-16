import argparse
import yaml
import logging
import math
import multiprocessing
import random
import sys
import time
import uuid

import six

import const
import db
import placement

LOG = logging.getLogger('main')

SCHEMA_CHOICES = ('placement', 'legacy')
FILTER_STRATEGY_CHOICES = ('db', 'python')
PLACEMENT_STRATEGY_CHOICES = ('pack', 'spread', 'random', 'random-pack', 'random-spread')
PARTITION_STRATEGY_CHOICES = ('modulo', 'none')

# Defaults here model a typical 256GB dual-socket i7 with 12 hardware threads
DEFAULT_RAM_MB_PER_NODE = 256 * 1024
DEFAULT_RAM_MB_RESERVED_PER_NODE = 1024
DEFAULT_CPU_CORES_PER_NODE = 24
DEFAULT_RAM_ALLOCATION_RATIO = 1.5
DEFAULT_CPU_ALLOCATION_RATIO = 16.0


def calc_num_nodes_from_args(args):
    rows = args.rows
    racks = args.racks
    nodes = args.nodes
    return (rows * racks * nodes)


def queue_instance_requests(args, request_queue):
    """
    Returns a list of tuples containing a new instance UUID and a pointer to a
    resource template to use for the instance. We pre-create the list of UUIDs
    and randomized resource templates in order to have a semi-realistic
    randomized resource template selection but re-play the exact same set of
    randomized resource selection requests to the various test scenarios.

    If the user has specified an input requests file to use, that file is
    read and the contents placed into the queue.

    If the user has specified an output requests file, the queued instance
    requests are JSON-ified and written to this file after being placed into
    the request queue.
    """
    if args.in_requests_file is not None:
        requests = yaml.load(open(args.in_requests_file, 'r').read())
        for r in requests:
            request_queue.put(r)
        LOG.info("Read instance requests from input file %s." %
                 args.in_requests_file)
        return len(requests)

    save_requests = args.out_requests_file is not None
    rows = args.rows
    racks = args.racks
    nodes = args.nodes
    ram_mb = args.node_ram_mb
    ram_mb_reserved = args.node_ram_mb_reserved
    ram_allocation_ratio = args.node_ram_allocation_ratio
    total_ram_per = (ram_mb - ram_mb_reserved) * ram_allocation_ratio
    num_nodes = calc_num_nodes_from_args(args)
    total_ram = int(math.floor(total_ram_per * num_nodes))
    # Add some fudging factor to ensure we always have more requests
    # than could possibly fit on all nodes.
    total_ram += (1024 * 100 * num_nodes * args.workers)

    num_requests = 0
    requests = []
    while total_ram > 0:
        res_tpl = random.choice(const.RESOURCE_TEMPLATES)
        total_ram -= res_tpl[const.RAM_MB]
        if total_ram > 0:
            r = (uuid.uuid4().hex, res_tpl)
            request_queue.put(r)
            if save_requests:
                requests.append(r)
            num_requests += 1

    if save_requests:
        with open(args.out_requests_file, 'w') as outfile:
            outfile.write(yaml.dump(requests))
        LOG.info("Wrote instance requests to output file %s." %
                 args.out_requests_file)

    LOG.info("Placed %d instance requests into queue." % num_requests)


def print_compiled_results(args, results, total_wallclock_time):
    """
    Aggregate the results from each worker process and display.
    """
    if args.results_file is not None:
        outfile = open(args.results_file, 'a+')
    else:
        outfile = sys.stdout

    do_claim_in_scheduler = 'Yes' if args.do_claim_in_scheduler else 'No'

    placement_total_query_time = sum(r.placement_total_query_time for r in results)
    requests_processed_count = sum(r.requests_processed_count for r in results)
    placement_query_count = sum(r.placement_query_count for r in results)
    placement_found_provider_count = sum(r.placement_found_provider_count for r in results)
    placement_no_found_provider_count = sum(r.placement_no_found_provider_count for r in results)
    placement_random_no_found_retry_count = sum(r.placement_random_no_found_retry_count for r in results)
    placement_avg_query_time = placement_total_query_time / placement_query_count
    placement_min_query_time = min(r.placement_min_query_time for r in results)
    placement_max_query_time = max(r.placement_max_query_time for r in results)
    claim_trx_rollback_count = sum(r.claim_trx_rollback_count for r in results)
    claim_deadlock_count = sum(r.claim_deadlock_count for r in results)
    claim_trx_count = sum(r.claim_trx_count for r in results)
    claim_success_count = sum(r.claim_success_count for r in results)
    claim_total_trx_time = sum(r.claim_total_trx_time for r in results)
    claim_avg_trx_time = claim_total_trx_time / claim_trx_count
    claim_min_trx_time = min(r.claim_min_trx_time for r in results)
    claim_max_trx_time = max(r.claim_max_trx_time for r in results)
    claim_total_deadlock_sleep_time = sum(r.claim_total_deadlock_sleep_time for r in results)

    outfile.write("==============================================================\n")
    outfile.write("                          Results\n")
    outfile.write("==============================================================\n")
    outfile.write("  Number of compute nodes:            %d\n" % calc_num_nodes_from_args(args))
    outfile.write("  Number of scheduler processes:      %d\n" % len(results))
    outfile.write("  Number of requests processed:       %d\n" % requests_processed_count)
    outfile.write("  Schema:                             %s\n" % args.schema)
    outfile.write("  Filter strategy:                    %s\n" % args.filter_strategy)
    outfile.write("  Placement strategy:                 %s\n" % args.placement_strategy)
    outfile.write("  Partition strategy:                 %s\n" % args.partition_strategy)
    outfile.write("  Do claim in scheduler?              %s\n" % do_claim_in_scheduler)
    outfile.write("  Total wallclock time:               %7.5f\n" % total_wallclock_time)
    outfile.write("--------------------------------------------------------------\n")
    outfile.write(" Placement operations\n")
    outfile.write("--------------------------------------------------------------\n")
    outfile.write("  Count placement queries:            %d\n" % placement_query_count)
    outfile.write("  Count found provider:               %d\n" % placement_found_provider_count)
    outfile.write("  Count not found provider:           %d\n" % placement_no_found_provider_count)
    outfile.write("  Count random no found retries:      %d\n" % placement_random_no_found_retry_count)
    outfile.write("  Total time filtering:               %7.5f\n" % placement_total_query_time)
    outfile.write("  Avg time to filter:                 %7.5f\n" % placement_avg_query_time)
    outfile.write("  Min time to filter:                 %7.5f\n" % placement_min_query_time)
    outfile.write("  Max time to filter:                 %7.5f\n" % placement_max_query_time)
    outfile.write("--------------------------------------------------------------\n")
    outfile.write(" Claim operations\n")
    outfile.write("--------------------------------------------------------------\n")
    outfile.write("  Count claim transactions:           %d\n" % claim_trx_count)
    outfile.write("  Count successful claims:            %d\n" % claim_success_count)
    outfile.write("  Count transaction rollbacks:        %d\n" % claim_trx_rollback_count)
    outfile.write("  Count claim deadlocks:              %d\n" % claim_deadlock_count)
    outfile.write("  Total time deadlock sleeping:       %7.5f\n" % claim_total_deadlock_sleep_time)
    outfile.write("  Total time in claim transactions:   %7.5f\n" % claim_total_trx_time)
    outfile.write("  Avg time in claim transaction:      %7.5f\n" % claim_avg_trx_time)
    outfile.write("  Min time in claim transaction:      %7.5f\n" % claim_min_trx_time)
    outfile.write("  Max time in claim transaction:      %7.5f\n" % claim_max_trx_time)
    outfile.write("==============================================================\n")
    outfile.flush()

    if args.results_file is not None:
        outfile.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', action="store_true",
                        default=False,
                        help="Show more output during run.")
    parser.add_argument('--results-file',
                        help="If set, output results to supplied file "
                             "instead of stdout. If file exists, will "
                             "append to the file.")
    parser.add_argument('--schema', choices=SCHEMA_CHOICES,
                        help="The schema to use for database layout.")
    parser.add_argument('--filter-strategy',
                        choices=FILTER_STRATEGY_CHOICES,
                        help="The filter strategy to use for scheduler.")
    parser.add_argument('--do-claim-in-scheduler', action="store_true",
                        default=True,
                        help="Perform the claim operation in the scheduler "
                             "process instead of on the compute node.")
    parser.add_argument('--placement-strategy',
                        choices=PLACEMENT_STRATEGY_CHOICES,
                        help="The placement strategy to use.")
    parser.add_argument('--partition-strategy',
                        choices=PARTITION_STRATEGY_CHOICES,
                        help="The partitioning strategy to use when selecting "
                             "nodes in the scheduler process.")
    parser.add_argument('--rows', type=int,
                        default=1,
                        help="Number of rows in datacenter.")
    parser.add_argument('--racks', type=int,
                        default=5,
                        help="Number of racks in each row.")
    parser.add_argument('--nodes', type=int,
                        default=20,
                        help="Number of nodes in each rack.")
    parser.add_argument('--shared-storage-per-row', action="store_true",
                        default=True,
                        help="Should a shared storage pool be created "
                             "for each row?")
    parser.add_argument('--node-ram-mb', type=int,
                        default=DEFAULT_RAM_MB_PER_NODE,
                        help="Amount of RAM in MB per node.")
    parser.add_argument('--node-ram-mb-reserved', type=int,
                        default=DEFAULT_RAM_MB_RESERVED_PER_NODE,
                        help="Amount of RAM in MB per node reserved "
                             "for the operating system.")
    parser.add_argument('--node-cpu-cores', type=int,
                        default=DEFAULT_CPU_CORES_PER_NODE,
                        help="Amount of physical CPU cores per node.")
    parser.add_argument('--node-ram-allocation-ratio', type=float,
                        default=DEFAULT_RAM_ALLOCATION_RATIO,
                        help="The RAM allocation ratio for each node.")
    parser.add_argument('--node-cpu-allocation-ratio', type=float,
                        default=DEFAULT_CPU_ALLOCATION_RATIO,
                        help="The CPU allocation ratio for each node.")
    parser.add_argument('--workers', type=int,
                        default=min(1, multiprocessing.cpu_count() - 2),
                        help="Number of worker processes to simulate "
                             "scheduler processes.")
    parser.add_argument('--in-requests-file',
                        help="If set, the set of instance requests is "
                             "read from the specified YAML file instead of "
                             "created anew for the run.")
    parser.add_argument('--out-requests-file',
                        help="If set, the set of instance requests is "
                             "written out to a file in YAML format. This "
                             "can then be read in with the --in-requests-file "
                             "option when you want to run different scenarios "
                             "using identical requests sets.")
    args = parser.parse_args()

    schema = args.schema
    if schema not in SCHEMA_CHOICES:
        print "Please select a schema to use."
        parser.print_help()
        sys.exit(1)

    filter_strategy = args.filter_strategy
    if filter_strategy not in FILTER_STRATEGY_CHOICES:
        print "Please select a filter strategy to use."
        parser.print_help()
        sys.exit(1)

    placement_strategy = args.placement_strategy
    if placement_strategy not in PLACEMENT_STRATEGY_CHOICES:
        print "Please select a placement strategy to use."
        parser.print_help()
        sys.exit(1)

    partition_strategy = args.partition_strategy
    if partition_strategy not in PARTITION_STRATEGY_CHOICES:
        print "Please select a partition strategy to use."
        parser.print_help()
        sys.exit(1)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_fmt = '%(levelname)7s: (%(process)d) %(message)s'
    logging.basicConfig(format=log_fmt, level=log_level)
    if schema == 'placement':
        db.placement_create_schema(args)
        db.placement_populate(args)

    # Workers grab their requests from here
    mp_manager = multiprocessing.Manager()
    request_queue = mp_manager.Queue()

    # Workers send their results here
    result_queue = mp_manager.Queue()

    LOG.debug("Queuing instance requests.")
    queue_instance_requests(args, request_queue)

    LOG.debug("Adding an empty task for each worker.")
    for x in six.moves.xrange(args.workers):
        request_queue.put(None)

    workers = []
    worker_done_events = []
    wallclock_start_time = time.time()
    for x in six.moves.xrange(args.workers):
        e = multiprocessing.Event()
        p = multiprocessing.Process(
                target=placement.run,
                args=(args, request_queue, result_queue, e))
        workers.append(p)
        worker_done_events.append(e)
        p.start()

    LOG.info("Created %d scheduler worker(s)." % len(workers))

    for x in six.moves.xrange(args.workers):
        e = worker_done_events[x]
        LOG.debug("Waiting for worker %d to set its done event." % x)
        e.wait()

    tot_wallclock_time = time.time() - wallclock_start_time

    # Wait for and collate our results. There should be one result entry
    # for each worker in the process pool.
    results = []
    for x in six.moves.xrange(args.workers):
        entry = result_queue.get()
        results.append(entry)

    LOG.info("Got %d results from worker processes." % len(results))

    print_compiled_results(args, results, tot_wallclock_time)


if __name__ == '__main__':
    main()
