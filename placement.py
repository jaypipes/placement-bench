import datetime
import logging
import multiprocessing
import random
import sys
import time

import sqlalchemy as sa
from sqlalchemy import sql

import const
import db
import result

LOG = logging.getLogger('placement')


def run(args, request_queue, result_queue, done_event):
    (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl) = db.placement_get_tables()
    engine = db.placement_get_engine()
    conn = engine.connect()

    worker_number = multiprocessing.current_process().name.split('-')[1]
    res = result.Result()
    res.process = worker_number

    def send_results():
        """
        Collates the results, pushes the results onto the result queue.
        """
        LOG.debug("Sending results to result queue.")
        result_queue.put(res)
        LOG.debug("Setting done event.")
        done_event.set()
        conn.close()
        engine.dispose()

    def get_selected_provider():
        """
        Return a resource provider that matches the requested resource
        amounts.
        """
        if args.schema == 'placement':
            select = db.placement_get_select(args, res_template, worker_number)
        
        records = conn.execute(select)

        if args.filter_strategy == 'db':
            return records.fetchone()
        if args.filter_strategy == 'python':
            filtered = []
            for row in records:
                # Filter out by RAM
                ram_available = (row['ram_total'] - row['ram_reserved']) * row['ram_allocation_ratio']
                ram_used = float(row['ram_used'] or 0)
                if row['ram_min_unit'] > res_template[const.RAM_MB]:
                    continue
                if row['ram_max_unit'] < res_template[const.RAM_MB]:
                    continue
                if (ram_available - ram_used) < res_template[const.RAM_MB]:
                    continue
                # Filter out by CPU
                cpu_available = (row['cpu_total'] - row['cpu_reserved']) * row['cpu_allocation_ratio']
                cpu_used = float(row['cpu_used'] or 0)
                if row['cpu_min_unit'] > res_template[const.VCPU]:
                    continue
                if row['cpu_max_unit'] < res_template[const.VCPU]:
                    continue
                if (cpu_available - cpu_used) < res_template[const.VCPU]:
                    continue
                filtered.append(row)
            if filtered:
                if args.placement_strategy == 'pack':
                    filtered.sort(key=lambda t: (t['ram_used'], t['cpu_used'], -t['id']), reverse=True)
                if args.placement_strategy == 'spread':
                    filtered.sort(key=lambda t: (t['ram_used'], t['cpu_used'], t['id']))
                if args.placement_strategy == 'random':
                    return random.choice(filtered)
                else:
                    return filtered[0]

    while True:
        # Grab an entry from the request queue. Entries are a tuple of the form
        # (uuid, resource_template).
        entry = request_queue.get()
        if entry is None:
            LOG.info("No more entries in request queue after processing "
                     "%d requests. Sending results." % res.requests_processed_count)
            send_results()
            return

        res.requests_processed_count += 1

        instance_uuid = entry[0]
        res_template = entry[1]
        LOG.debug("Attempting to place instance %s request for %dMB RAM "
                  "and %d vCPUs." %
                  (instance_uuid,
                   res_template[const.RAM_MB],
                   res_template[const.VCPU]))

        start_placement_query_time = time.time()
        selected = get_selected_provider()
        res.add_placement_query_time(time.time() - start_placement_query_time)
        res.placement_query_count += 1

        if not selected:
            LOG.info("No available space in datacenter for request for %d RAM and %d CPU" %
                     (res_template[const.RAM_MB], res_template[const.VCPU]))
            res.placement_no_found_provider_count += 1
            send_results()
            return

        res.placement_found_provider_count += 1

        provider_id = selected['id']
        generation = selected['generation']

        start_trx_time = time.time()
        trans = conn.begin()
        try:
            created_on = datetime.datetime.utcnow()
            # Allocate the RAM
            ins = alloc_tbl.insert().values(resource_provider_id=provider_id,
                                            resource_class_id=const.RAM_MB,
                                            consumer_uuid=instance_uuid,
                                            used=res_template[const.RAM_MB],
                                            created_at=created_on)
            conn.execute(ins)
            # Allocate the CPU
            ins = alloc_tbl.insert().values(resource_provider_id=provider_id,
                                            resource_class_id=const.VCPU,
                                            consumer_uuid=instance_uuid,
                                            used=res_template[const.VCPU],
                                            created_at=created_on)
            conn.execute(ins)

            upd = rp_tbl.update().where(
                    sql.and_(rp_tbl.c.id == provider_id,
                             rp_tbl.c.generation == generation)).values(generation=generation+1)
            upd_res = conn.execute(upd)
            res.claim_trx_count += 1
            
            # Check to see if another thread executed a concurrent update on
            # the same target resource provider
            rc = upd_res.rowcount
            if rc != 1:
                res.claim_deadlock_count += 1
                trx_time = time.time() - start_trx_time
                res.add_claim_trx_time(trx_time)
                sleep_time = random.uniform(0.01, 0.10)
                res.total_deadlock_sleep_time += sleep_time
                time.sleep(sleep_time)
                raise Exception("deadlocked, retrying transaction")

            trans.commit()

            res.claim_success_count += 1
            trx_time = time.time() - start_trx_time
            res.add_claim_trx_time(trx_time)

            LOG.debug("allocated instance %s to compute node %d. %06d RAM and %02d CPU." %
                      (instance_uuid, provider_id, res_template[const.RAM_MB], res_template[const.VCPU]))
        except Exception as e:
            res.claim_trx_rollback_count += 1
            trans.rollback()
