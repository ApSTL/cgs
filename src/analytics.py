#!/usr/bin/env python3
import sys
from statistics import mean, stdev


class Analytics:
	def __init__(self, sim_time, ignore_start=0, ignore_end=0):
		self.start = ignore_start
		self.end = sim_time - ignore_end
		self.requests = {}
		self.requests_failed_count = 0

		# The number of submitted requests already handled by existing tasks
		self.requests_duplicated_count = 0

		self.tasks = {}
		self.tasks_failed_count = 0
		self.tasks_redundant_count = 0
		self.tasks_renewed_count = 0

		self.bundles = []
		self.bundles_delivered = []
		self.bundles_failed = []

		self.bundles_acquired_count = 0
		self.bundles_forwarded_count = 0
		self.bundles_delivered_count = 0
		self.bundles_dropped_count = 0
		self.bundles_rerouted_count = 0

	def get_bundles_delivered_in_active_period(self):
		return [
			b for b in self.bundles_delivered if
			self.start <= self.requests[self.tasks[b.task_id].request_ids[0]].time_created
			and b.delivered_at <= self.end
		]

	def get_bundles_failed_in_active_period(self):
		return [
			b for b in self.bundles_failed if
			self.start <= self.requests[self.tasks[b.task_id].request_ids[0]].time_created
			and b.dropped_at <= self.end
		]

	@property
	def pickup_latencies(self):
		# List of all times between request submission and bundle creation for ALL
		# bundles deemed "valid" for analysis
		return [
			b.created_at - self.requests[self.tasks[b.task_id].request_ids[
				0]].time_created
			for b in self.get_bundles_delivered_in_active_period() + self.get_bundles_failed_in_active_period()
		]

	@property
	def pickup_latency_ave(self):
		return mean(self.pickup_latencies)

	@property
	def pickup_latency_stdev(self):
		return stdev(self.pickup_latencies)

	@property
	def delivery_latencies(self):
		# List of times from bundle creation and bundle delivery
		return [
			b.delivered_at - b.created_at
			for b in self.get_bundles_delivered_in_active_period()
		]

	@property
	def delivery_latency_ave(self):
		return mean(self.delivery_latencies)

	@property
	def delivery_latency_stdev(self):
		return stdev(self.delivery_latencies)

	@property
	def request_latencies(self):
		# List of times between bundle delivery and request submission
		return [
			b.delivered_at - self.requests[self.tasks[b.task_id].request_ids[0]].time_created
			for b in self.get_bundles_delivered_in_active_period()
		]

	@property
	def request_latency_ave(self):
		return mean(self.request_latencies)

	@property
	def request_latency_stdev(self):
		return stdev(self.request_latencies)

	def get_requests_in_active_period(self):
		bundles = self.get_bundles_delivered_in_active_period() + self.get_bundles_failed_in_active_period()
		return [
			r for r in [
				self.requests[self.tasks[b.task_id].request_ids[0]] for b in bundles
			]
		]

	def submit_request(self, r):
		self.requests[r.uid] = r

	@property
	def requests_submitted_count(self):
		return len(self.requests)

	def fail_request(self):
		self.requests_failed_count += 1

	def duplicated_request(self):
		self.requests_duplicated_count += 1

	@property
	def tasks_processed_count(self):
		return len(self.tasks)

	def add_task(self, t):
		self.tasks[t.uid] = t

	def fail_task(self):
		self.tasks_failed_count += 1

	def redundant_task(self):
		self.tasks_redundant_count += 1

	def renew_task(self):
		"""
		If a redundant task is replaced by a new task, this method is triggered
		"""
		self.tasks_renewed_count += 1

	def add_bundle(self, b):
		self.bundles.append(b)
		self.bundles_acquired_count += 1

	def forward_bundle(self):
		self.bundles_forwarded_count += 1

	def deliver_bundle(self, b, t_now):
		self.bundles_delivered.append(b)
		self.bundles_delivered_count += 1

	def drop_bundle(self, bundle):
		self.bundles_failed.append(bundle)
		self.bundles_dropped_count += 1

	def reroute_bundle(self):
		"""
		Method invoked any time a bundle is assigned to a route that differs from the
		one along which it was most recently assigned. E.g. if a Bundle was due to
		traverse the route 3->4->6, but doesn't make it over contact 3 and therefore
		gets reassigned to the route 5->7, this would constitute a "reroute" event
		"""
		self.bundles_rerouted_count += 1

