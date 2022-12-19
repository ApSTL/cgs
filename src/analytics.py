#!/usr/bin/env python3
import sys
from statistics import mean, stdev


class Analytics:
	def __init__(self, warm_up=0, cool_down=sys.maxsize):
		self.warm_up = warm_up
		self.cool_down = cool_down
		self.requests = {}
		self.requests_submitted = 0
		self.requests_failed = 0

		# The number of submitted requests already handled by existing tasks
		self.requests_duplicated = 0

		self.tasks = []
		self.tasks_processed = 0
		self.tasks_failed = 0
		self.tasks_redundant = 0
		self.tasks_renewed = 0

		self.bundles = []
		self.bundles_acquired = 0
		self.bundles_forwarded = 0
		self.bundles_delivered = 0
		self.bundles_dropped = 0
		self.bundles_rerouted = 0

		self.latencies = []

	@property
	def latency_ave(self):
		return mean(self.latencies)

	@property
	def latency_stdev(self):
		return stdev(self.latencies)

	def submit_request(self, r):
		self.requests[r.uid] = r
		self.requests_submitted += 1

	def fail_request(self):
		self.requests_failed += 1

	def duplicated_request(self):
		self.requests_duplicated += 1

	def add_task(self, t):
		self.tasks.append(t)
		self.tasks_processed += 1

	def fail_task(self):
		self.tasks_failed += 1

	def redundant_task(self):
		self.tasks_redundant += 1

	def renew_task(self):
		"""
		If a redundant task is replaced by a new task, this method is triggered
		"""
		self.tasks_renewed += 1

	def add_bundle(self, b):
		self.bundles.append(b)
		self.bundles_acquired += 1

	def forward_bundle(self):
		self.bundles_forwarded += 1

	def deliver_bundle(self, b, t_now):
		self.latencies.append(t_now - b.created_at)
		self.bundles_delivered += 1

	def drop_bundle(self):
		self.bundles_dropped += 1

	def reroute_bundle(self):
		"""
		Method invoked any time a bundle is assigned to a route that differs from the
		one along which it was most recently assigned. E.g. if a Bundle was due to
		traverse the route 3->4->6, but doesn't make it over contact 3 and therefore
		gets reassigned to the route 5->7, this would constitute a "reroute" event
		"""
		self.bundles_rerouted += 1

