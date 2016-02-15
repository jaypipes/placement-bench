import sys


class Result(object):

    def __init__(self):
        self.process = None
        self.requests_processed_count = 0
        self.retry_requests_processed_count = 0

        # Placement query stats
        self.placement_query_count = 0
        self.placement_found_provider_count = 0
        self.placement_no_found_provider_count = 0
        self.placement_retry_partition_count = 0
        self.placement_total_query_time = 0
        self.placement_min_query_time = sys.maxint
        self.placement_max_query_time = 0

        # Claim transaction stats
        self.claim_trx_count = 0
        self.claim_success_count = 0
        self.claim_deadlock_count = 0
        self.claim_trx_rollback_count = 0
        self.claim_total_deadlock_sleep_time = 0
        self.claim_total_trx_time = 0
        self.claim_min_trx_time = sys.maxint
        self.claim_max_trx_time = 0

    def add_placement_query_time(self, time):
        self.placement_min_query_time = min(self.placement_min_query_time, time)
        self.placement_max_query_time = max(self.placement_max_query_time, time)
        self.placement_total_query_time += time

    def add_claim_trx_time(self, time):
        self.claim_min_trx_time = min(self.claim_min_trx_time, time)
        self.claim_max_trx_time = max(self.claim_max_trx_time, time)
        self.claim_total_trx_time += time

    @property
    def claim_avg_trx_time(self):
        return self.claim_total_trx_time / float(self.claim_trx_count)

    @property
    def placement_avg_query_time(self):
        return self.placement_total_query_time / float(self.placement_query_count)
