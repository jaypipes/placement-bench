import logging
import random
import uuid
import warnings

import six
import sqlalchemy as sa
from sqlalchemy import sql

import const

LOG = logging.getLogger('db')


def placement_get_engine():
    return sa.create_engine('mysql://root@localhost/placement')


def placement_get_tables():
    """Returns a tuple of table objects, loaded from the placement schema."""
    engine = placement_get_engine()
    metadata = sa.MetaData(bind=engine)

    rp_tbl = sa.Table('resource_providers', metadata, autoload=True)
    agg_tbl = sa.Table('aggregates', metadata, autoload=True)
    rp_agg_tbl = sa.Table('resource_provider_aggregates', metadata, autoload=True)
    inv_tbl = sa.Table('inventories', metadata, autoload=True)
    alloc_tbl = sa.Table('allocations', metadata, autoload=True)
    engine.dispose()
    return (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl)


def placement_create_schema(args):
    """
    Creates the new placement schema with resource-providers framework tables.
    """
    engine = sa.create_engine('mysql://root@localhost')
    conn = engine.connect()
    sql = "DROP SCHEMA IF EXISTS placement"
    conn.execute(sql)
    sql = "CREATE SCHEMA placement"
    conn.execute(sql)
    conn.close()
    engine.dispose()
    LOG.debug('Dropped and created placement database.')

    engine = placement_get_engine()
    conn = engine.connect()
    sql = """
CREATE TABLE resource_providers (
  id INT NOT NULL,
  uuid CHAR(36) NOT NULL,
  name VARCHAR(200) NULL,
  can_host INT NOT NULL,
  generation INT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (uuid)
)
"""
    conn.execute(sql)
    LOG.debug('Created resource_provider table in placement schema.')

    sql = """
CREATE TABLE aggregates (
  id INT NOT NULL,
  name VARCHAR(200) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (name)
)
"""
    conn.execute(sql)
    LOG.debug('Created aggregates table in placement schema.')

    sql = """
CREATE TABLE resource_provider_aggregates (
  resource_provider_id INT NOT NULL,
  aggregate_id INT NOT NULL,
  PRIMARY KEY (resource_provider_id, aggregate_id),
  INDEX (aggregate_id)
)
"""
    conn.execute(sql)
    LOG.debug('Created resource_provider_aggregates table in placement '
              'schema.')

    sql = """
CREATE TABLE inventories (
  resource_provider_id INT NOT NULL,
  resource_class_id INT NOT NULL,
  total INT NOT NULL,
  reserved INT NOT NULL,
  min_unit INT NOT NULL,
  max_unit INT NOT NULL,
  step_size INT NOT NULL,
  allocation_ratio FLOAT NOT NULL,
  generation INT NOT NULL,
  PRIMARY KEY (resource_provider_id, resource_class_id),
  INDEX (resource_class_id)
)
"""
    conn.execute(sql)
    LOG.debug('Created inventories table in placement schema.')

    sql = """
CREATE TABLE IF NOT EXISTS allocations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  resource_provider_id INT NOT NULL,
  resource_class_id INT NOT NULL,
  consumer_uuid CHAR(36) NOT NULL,
  used INT NOT NULL,
  created_at DATETIME NOT NULL,
  INDEX (resource_provider_id, resource_class_id),
  INDEX (consumer_uuid),
  INDEX (resource_class_id, resource_provider_id, used)
)
"""
    conn.execute(sql)
    conn.close()
    engine.dispose()
    LOG.debug('Created allocations table in placement schema.')
    LOG.info('Created placement schema.')


def create_schema_legacy(args):
    """
    Creates the legacy Nova schema for compute nodes.
    """
    pass


def placement_populate(args):
    """
    Creates resource provider, inventory, and aggregates in the new placement
    schema based on a row/rack/node layout provided by arguments.
    """
    num_rows = args.rows
    num_racks_per_row = args.racks
    num_nodes_per_rack = args.nodes
    create_row_storage = args.shared_storage_per_row
    ram_mb = args.node_ram_mb
    ram_mb_reserved = args.node_ram_mb_reserved
    cpu_cores = args.node_cpu_cores
    ram_allocation_ratio = args.node_ram_allocation_ratio
    cpu_allocation_ratio = args.node_cpu_allocation_ratio

    min_ram_mb = min(*[rt[const.RAM_MB] for rt in const.RESOURCE_TEMPLATES])
    max_ram_mb = max(*[rt[const.RAM_MB] for rt in const.RESOURCE_TEMPLATES])

    LOG.debug('Populating placement schema. %d rows with %d racks in each row '
              'and %d nodes in each rack.' %
              (num_rows, num_racks_per_row, num_nodes_per_rack))
    LOG.debug('Using shared storage pool per row: %s' %
              ('Yes' if create_row_storage else 'No'))

    # We have 2000 compute nodes in our datacenter.
    #
    # There are 10 aggregates representing 10 rows in the datacenter. Each row has
    # 10 racks of compute nodes. Each rack contains 20 compute nodes. Each compute
    # node has 256GB RAM and 24 physical CPU cores and no local disk for use by
    # instances. Each compute host reserves 1GB of RAM for the host and has a 1.5X
    # overcommit ratio for RAM and 16X overcommit ratio on vCPUs.
    #
    # Each aggregate is served by an NFS attached block storage system that is
    # used to store instance disk images.

    engine = placement_get_engine()
    conn = engine.connect()
    
    # SQLAlchemy warns me that generation field has no default value...
    warnings.filterwarnings("ignore", '.*generation.*',)

    (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl) = placement_get_tables()
    provider_id = 1
    num_computes = 0
    num_aggs = 0
    for row in six.moves.xrange(num_rows):
        # Create an aggregate for the row...
        row_name = 'row ' + str(row)
        agg_ins = agg_tbl.insert().values(id=row, name=row_name)
        conn.execute(agg_ins)

        num_aggs += 1

        if create_row_storage:
            # Create a resource provider for the shared storage pool
            # on the rack
            store_name = 'NFS share for row ' + str(row)
            store_ins = rp_tbl.insert().values(id=provider_id,
                                               uuid=uuid.uuid4().hex,
                                               name=store_name,
                                               can_host=0,
                                               generation=1)
            conn.execute(store_ins)

            # Associate the shared storage pool with the aggregate
            # representing the row
            map_ins = rp_agg_tbl.insert().values(
                    resource_provider_id=provider_id,
                    aggregate_id=row)
            conn.execute(map_ins)

            # Set the total capacity of the shared storage pool
            inv_ins = inv_tbl.insert().values(
                    resource_provider_id=provider_id,
                    resource_class_id=const.DISK_GB,
                    total=1024*100,  # 100TB
                    reserved=100,
                    min_unit=10,
                    max_unit=1024, # 1TB
                    step_size=50,
                    allocation_ratio=1.0)
            conn.execute(inv_ins)

            provider_id += 1
                                             
        for rack in six.moves.xrange(num_racks_per_row):
            for node in six.moves.xrange(num_nodes_per_rack):
                # Create a resource provider for each compute host
                node_name = 'row%02drack%02dnode%02d' % (row, rack, node)
                node_ins = rp_tbl.insert().values(
                        id=provider_id,
                        uuid=uuid.uuid4().hex,
                        name=node_name,
                        can_host=1,
                        generation=1)
                conn.execute(node_ins)

                # Associate the compute node with the aggregate
                # representing the row
                map_ins = rp_agg_tbl.insert().values(
                        resource_provider_id=provider_id,
                        aggregate_id=row)
                conn.execute(map_ins)

                # Set the RAM capacity of the compute node
                inv_ins = inv_tbl.insert().values(
                        resource_provider_id=provider_id,
                        resource_class_id=const.RAM_MB,
                        total=ram_mb,  # 256GB
                        reserved=ram_mb_reserved,
                        min_unit=min_ram_mb,
                        max_unit=max_ram_mb, # 64GB
                        step_size=min_ram_mb,
                        allocation_ratio=ram_allocation_ratio)
                conn.execute(inv_ins)

                # Set the CPU capacity of the compute node
                inv_ins = inv_tbl.insert().values(
                        resource_provider_id=provider_id,
                        resource_class_id=const.VCPU,
                        total=cpu_cores,
                        reserved=0,
                        min_unit=1,
                        max_unit=cpu_cores,
                        step_size=2,
                        allocation_ratio=16.0)
                conn.execute(inv_ins)

                provider_id += 1
                num_computes += 1

    conn.close()
    LOG.info('Populated placement schema with %d resource_providers '
             '(%d compute nodes / %d aggregates).' %
             ((provider_id - 1), num_computes, num_aggs))


def placement_get_select(args, res_template, worker_number):
    """
    Returns a sqlalchemy.Select object representing the query against the
    resource_providers schema tables, using derived table queries against the
    allocations and inventories tables. The SQL generated from this looks like
    the following. (If the filter_strategy is not 'db', then the WHERE clause
    is not appended to the SQL):

        SELECT cn.id, cn.generation 
        FROM resource_providers AS cn
        INNER JOIN inventories AS ram_filtered
        ON cn.id = ram_filtered.resource_provider_id
        AND ram_filtered.resource_class_id = 2
        LEFT OUTER JOIN (
          SELECT 
            allocations.resource_provider_id AS resource_provider_id,
            sum(allocations.used) AS used 
          FROM allocations 
          WHERE allocations.resource_class_id = 2
          GROUP BY allocations.resource_provider_id
        ) AS ram_usage
        ON ram_filtered.resource_provider_id = ram_usage.resource_provider_id
        INNER JOIN inventories AS cpu_filtered
        ON ram_filtered.resource_provider_id = cpu_filtered.resource_provider_id
        AND cpu_filtered.resource_class_id = 1
        LEFT OUTER JOIN (
          SELECT
            allocations.resource_provider_id AS resource_provider_id,
            sum(allocations.used) AS used
          FROM allocations 
          WHERE allocations.resource_class_id = 1
          GROUP BY allocations.resource_provider_id
        ) AS cpu_usage
        ON cpu_filtered.resource_provider_id = cpu_usage.resource_provider_id
        WHERE
        ram_filtered.min_unit <= 64
        AND ram_filtered.max_unit >= 64
        AND floor((ram_filtered.total - ram_filtered.reserved) * ram_filtered.allocation_ratio) - ifnull(ram_usage.used, 0) >= 64
        AND cpu_filtered.min_unit <= 1
        AND cpu_filtered.max_unit >= 1
        AND floor((cpu_filtered.total - cpu_filtered.reserved) * cpu_filtered.allocation_ratio) - ifnull(cpu_usage.used, 0) >= 1
        LIMIT 1

    Depending on the partition and placement strategy, there may be additional
    WHERE clauses that look like the following:

        # 'modulo' partition strategy
        AND (cn.id + $NUM_WORKERS) % NUM_WORKERS == 0

        or:

        # 'random' placement strategy
        AND cn.id >= $RANDOM_COMPUTE_NODE_ID

    The ORDER BY clause will depend on the placement strategy and may look like
    the following:

        # 'pack' placement strategy
        ORDER BY IFNULL(ram_usage.used, 0) ASC, cn.id ASC

        or:

        # 'spread' placement strategy
        ORDER BY IFNULL(ram_usage.used, 0) DESC, cn.id ASC
    """
    (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl) = placement_get_tables()
    cn_tbl = sa.alias(rp_tbl, name='cn')
    ram_filtered = sa.alias(inv_tbl, name='ram_filtered')
    cpu_filtered = sa.alias(inv_tbl, name='cpu_filtered')

    ram_usage = sa.select([alloc_tbl.c.resource_provider_id,
                           sql.func.sum(alloc_tbl.c.used).label('used')])
    ram_usage = ram_usage.where(alloc_tbl.c.resource_class_id == const.RAM_MB)
    ram_usage = ram_usage.group_by(alloc_tbl.c.resource_provider_id)
    ram_usage = sa.alias(ram_usage, name='ram_usage')

    cpu_usage = sa.select([alloc_tbl.c.resource_provider_id,
                           sql.func.sum(alloc_tbl.c.used).label('used')])
    cpu_usage = cpu_usage.where(alloc_tbl.c.resource_class_id == const.VCPU)
    cpu_usage = cpu_usage.group_by(alloc_tbl.c.resource_provider_id)
    cpu_usage = sa.alias(cpu_usage, name='cpu_usage')

    ram_inv_join = sql.join(cn_tbl, ram_filtered,
                            sql.and_(
                                cn_tbl.c.id == ram_filtered.c.resource_provider_id,
                                ram_filtered.c.resource_class_id == const.RAM_MB))
    ram_join = sql.outerjoin(ram_inv_join, ram_usage,
                             ram_filtered.c.resource_provider_id == ram_usage.c.resource_provider_id)
    cpu_inv_join = sql.join(ram_join, cpu_filtered,
                            sql.and_(
                                ram_filtered.c.resource_provider_id == cpu_filtered.c.resource_provider_id,
                                cpu_filtered.c.resource_class_id == const.VCPU))
    cpu_join = sql.outerjoin(cpu_inv_join, cpu_usage,
                             cpu_filtered.c.resource_provider_id == cpu_usage.c.resource_provider_id)

    cols_in_output = [cn_tbl.c.id, cn_tbl.c.generation]
    if args.filter_strategy == 'python':
        # When we don't do stuff on the DB side, we need to pass back
        # a whole lot more columns since we have Python code loops
        # that need to process these data fields.
        cols_in_output.extend([
           ram_filtered.c.total.label('ram_total'),
           ram_filtered.c.reserved.label('ram_reserved'),
           ram_filtered.c.min_unit.label('ram_min_unit'),
           ram_filtered.c.max_unit.label('ram_max_unit'),
           ram_filtered.c.allocation_ratio.label('ram_allocation_ratio'),
           ram_usage.c.used.label('ram_used'),
           cpu_filtered.c.total.label('cpu_total'),
           cpu_filtered.c.reserved.label('cpu_reserved'),
           cpu_filtered.c.min_unit.label('cpu_min_unit'),
           cpu_filtered.c.max_unit.label('cpu_max_unit'),
           cpu_filtered.c.allocation_ratio.label('cpu_allocation_ratio'),
           cpu_usage.c.used.label('cpu_used'),
        ])

    select = sa.select(cols_in_output).select_from(cpu_join)

    if args.filter_strategy == 'db':
        where_conds = (
            ram_filtered.c.min_unit <= res_template[const.RAM_MB],
            ram_filtered.c.max_unit >= res_template[const.RAM_MB],
            sql.func.floor((ram_filtered.c.total - ram_filtered.c.reserved) * ram_filtered.c.allocation_ratio)
            - sql.func.ifnull(ram_usage.c.used, 0) >= res_template[const.VCPU],
            cpu_filtered.c.min_unit <= res_template[const.VCPU],
            cpu_filtered.c.max_unit >= res_template[const.VCPU],
            sql.func.floor((cpu_filtered.c.total - cpu_filtered.c.reserved) * cpu_filtered.c.allocation_ratio)
            - sql.func.ifnull(cpu_usage.c.used, 0) >= res_template[const.VCPU]
        )
        if args.partition_strategy == 'modulo':
            where_conds += ((cn_tbl.c.id % args.workers) == worker_number,)

        if args.placement_strategy == 'pack':
            select = select.order_by(sql.func.ifnull(ram_usage.c.used, 0), cn_tbl.c.id)
        if args.placement_strategy == 'spread':
            select = select.order_by(sql.func.ifnull(ram_usage.c.used, 0).desc(), cn_tbl.c.id)
        if args.placement_strategy == 'random':
            # The scheduler could keep a cache of the number of compute
            # nodes in the system. But here, we emulate that cache by
            # simply picking a random compute node ID by selecting a
            # random ID since we know the compute nodes are
            # sequentially ordered and a fixed number.
            num_compute_nodes = (args.rows * args.racks * args.nodes)
            num_shared_resource_pools = args.rows * (1 if args.shared_storage_per_row else 0)
            random_compute_node_id = random.randint(1 + num_shared_resource_pools,
                                                    num_compute_nodes + num_shared_resource_pools)
            where_conds += (cn_tbl.c.id >= random_compute_node_id,)

        select = select.where(sql.and_(*where_conds))
        select = select.limit(1)

    return select
