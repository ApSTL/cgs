#!/usr/bin/env python3
import sys
from statistics import mean, stdev


class Analytics:
	def __init__(self, sim_time, ignore_start=0, ignore_end=0):
		self.start = ignore_start
		self.end = sim_time - ignore_end
		self.requests = {}
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

	def get_all_bundles_in_active_period(self):
		"""
		Return list of all bundles originating from requests in active period
		"""
		return [
			b for b in self.bundles if
			self.start <= b.task.requests[0].time_created <= self.end
		]

	def get_bundles_delivered_in_active_period(self):
		"""
		Return list of delivered bundles originating from requests in active period
		"""
		return [
			b for b in self.bundles_delivered if
			self.start <= b.task.requests[0].time_created <= self.end
		]

	def get_bundles_failed_in_active_period(self):
		"""
		Return list of dropped bundles originating from requests in active period
		"""
		return [
			b for b in self.bundles_failed if
			self.start <= b.task.requests[0].time_created <= self.end
		]

	@property
	def pickup_latencies(self):
		"""List of times between request submission and bundle creation for all bundles

		"""
		return [
			b.created_at - b.task.requests[0].time_created
			for b in self.get_all_bundles_in_active_period()
		]

	@property
	def pickup_latency_ave(self):
		return mean(self.pickup_latencies)

	@property
	def pickup_latency_stdev(self):
		return stdev(self.pickup_latencies)

	@property
	def delivery_latencies(self):
		"""List of times from bundle creation and bundle delivery.

		The "delivery latency" for dropped bundle is set to be the full time to live
		"""
		return [
			b.delivered_at - b.created_at
			for b in self.get_bundles_delivered_in_active_period()
		] + [
			b.deadline - b.created_at
			for b in self.get_bundles_failed_in_active_period()
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
			x[0] + x[1] for x in zip(self.pickup_latencies, self.delivery_latencies)
		]

	@property
	def request_latency_ave(self):
		return mean(self.request_latencies)

	@property
	def request_latency_stdev(self):
		return stdev(self.request_latencies)

	def get_all_requests_in_active_period(self):
		return [
			r for r in self.requests.values()
			if self.start <= r.time_created <= self.end
		]

	def get_delivered_requests_in_active_period(self):
		return [
			r for r in self.requests.values()
			if self.start <= r.time_created <= self.end
			and r.status == "delivered"
		]

	def get_failed_requests_in_active_period(self):
		return [
			r for r in self.requests.values()
			if self.start <= r.time_created <= self.end
			and r.status == "failed"
		]

	def submit_request(self, r):
		self.requests[r.uid] = r

	@property
	def requests_submitted_count(self):
		return len(self.get_all_requests_in_active_period())

	@property
	def requests_delivered_count(self):
		return len(self.get_delivered_requests_in_active_period())

	@property
	def requests_failed_count(self):
		return len(self.get_failed_requests_in_active_period())

	# FIXME update this
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

