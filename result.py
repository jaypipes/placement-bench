import sys


class Result(object):

    def __init__(self):
        self.process = None
        self.total_trx_time = 0
        self.total_query_time = 0
        self.total_deadlock_sleep_time = 0
        self.placement_count = 0
        self.deadlock_count = 0
        self.rollback_count = 0
        self.trx_count = 0
        self.query_count = 0
        self.min_query_time = sys.maxint
        self.max_query_time = 0
        self.min_trx_time = sys.maxint
        self.max_trx_time = 0

    @property
    def avg_trx_time(self):
        return self.tot_trx_time / float(self.trx_count)

    @property
    def avg_query_time(self):
        return self.tot_query_time / float(self.query_count)
