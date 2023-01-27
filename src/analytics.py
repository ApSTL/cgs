#!/usr/bin/env python3
import sys
from statistics import mean, stdev
from pubsub import pub


class Analytics:
	def __init__(self, sim_time, ignore_start=0, ignore_end=0, inputs=None):
		self.start = ignore_start
		self.end = sim_time - ignore_end
		self.requests = {}
		self.requests_duplicated_count = 0

		self.tasks = {}

		self.bundles = []
		self.bundles_delivered = []
		self.bundles_failed = []

		self._traffic_load = None

		self.inputs = inputs

	# *************************** CRUD operations *************************
	def submit_request(self, r):
		self.requests[r.uid] = r

	def add_task(self, t):
		self.tasks[t.uid] = t

	def reschedule_task(self, task, t, node):
		self.tasks[task].status = "rescheduled"
		self.tasks[task].rescheduled_at = t
		self.tasks[task].rescheduled_by = node

	def fail_task(self, task, t, on):
		# If this task has already been fulfilled elsewhere, don't set to failed
		if self.tasks[task].status == "delivered":
			return
		self.tasks[task].failed(t, on)
		self.requests[self.tasks[task].request_ids[0]].status = "failed"

	def acquire_bundle(self, b):
		self.bundles.append(b)
		# There's a chance the task to which this bundle relates, has already been
		# "acquired", so check first and only update if it's the first time
		if self.tasks[b.task_id].status != "acquired":
			self.requests[b.task.request_ids[0]].status = "acquired"
			self.tasks[b.task_id].acquired(b.created_at, b.src)

	def deliver_bundle(self, b):
		# If the request has already been delivered, skip
		if self.requests[b.task.request_ids[0]].status == "delivered":
			return
		self.bundles_delivered.append(b)
		self.requests[b.task.request_ids[0]].status = "delivered"
		self.tasks[b.task.uid].delivered(b.delivered_at, b.previous_node, b.current)

	def drop_bundle(self, b):
		self.bundles_failed.append(b)
		# There's a chance the task to which this bundle relates, has already been
		# "delivered", so check first and only update if it's the first time
		if self.tasks[b.task_id].status != "failed":
			self.fail_task(b.task.uid, b.dropped_at, b.current)

	# *************************** LATENCIES *******************************
	@property
	def pickup_latencies(self):
		"""List of times between request submission and bundle creation for dlvrd bundles.
		"""
		return [
			b.created_at - b.task.requests[0].time_created
			for b in self.get_all_bundles_in_active_period()
		]

	@property
	def pickup_latencies_delivered(self):
		"""List of times between request submission and bundle creation for dlvrd bundles.
		"""
		return [
			b.created_at - b.task.requests[0].time_created
			for b in self.get_bundles_delivered_in_active_period()
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
		# ] + [
		# 	b.deadline - b.created_at
		# 	for b in self.get_bundles_failed_in_active_period()
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
			x[0] + x[1] for x in zip(
				self.pickup_latencies_delivered,
				self.delivery_latencies
			)
		]

	@property
	def request_latency_ave(self):
		return mean(self.request_latencies)

	@property
	def request_latency_stdev(self):
		return stdev(self.request_latencies)

	# ****************************** HOP-COUNTS ********************************
	@property
	def hop_count_average_all(self):
		bundles = self.get_all_bundles_in_active_period()
		return mean([b.hop_count for b in bundles])

	@property
	def hop_count_average_delivered(self):
		bundles = self.get_bundles_delivered_in_active_period()
		return mean([b.hop_count for b in bundles])

	# ****************************** REQUESTS ********************************
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

	@property
	def requests_submitted_count(self):
		return len(self.get_all_requests_in_active_period())

	@property
	def requests_rejected_count(self):
		# The number of requests that were not converted into tasks
		return self.requests_submitted_count - self.tasks_processed_count

	@property
	def requests_delivered_count(self):
		return len(self.get_delivered_requests_in_active_period())

	@property
	def requests_failed_count(self):
		# the number of requests that were converted into tasks, but not delivered
		# return len(self.get_failed_requests_in_active_period())
		return self.tasks_processed_count - self.requests_delivered_count

	@property
	def request_delivery_ratio(self):
		"""
		The fraction of "submitted" requests for which a bundle was delivered
		"""
		return self.requests_delivered_count / self.requests_submitted_count

	@property
	def request_drop_ratio(self):
		return self.requests_failed_count / self.requests_submitted_count

	# ************************ TASKS ****************************
	def get_tasks_generated_in_active_period(self):
		return [
			t for t in self.tasks.values()
			if self.start <= t.requests[0].time_created <= self.end
		]

	def get_tasks_acquired_in_active_period(self):
		return [
			t for t in self.get_tasks_generated_in_active_period()
			if t.status == "acquired"
		]

	def get_tasks_delivered_in_active_period(self):
		return [
			t for t in self.get_tasks_generated_in_active_period()
			if t.status == "delivered"
		]

	def get_tasks_failed_in_active_period(self):
		return [
			t for t in self.get_tasks_generated_in_active_period()
			if t.status == "failed"
		]

	def get_tasks_rescheduled_in_active_period(self):
		return [
			t for t in self.get_tasks_generated_in_active_period()
			if t.status == "rescheduled"
		]

	def get_tasks_rescheduled_post_pickup_in_active_period(self):
		return [
			b.task for b in self.get_bundles_failed_in_active_period()
			if b.task.status == "rescheduled"
		]

	def get_tasks_rescheduled_pre_pickup_in_active_period(self):
		return [
			t for t in self.get_tasks_rescheduled_in_active_period()
			if t not in self.get_tasks_rescheduled_post_pickup_in_active_period()
		]

	@property
	def tasks_processed_count(self):
		return len(self.get_tasks_generated_in_active_period())

	@property
	def tasks_acquired_count(self):
		return len(self.get_tasks_acquired_in_active_period())

	@property
	def tasks_delivered_count(self):
		return len(self.get_tasks_delivered_in_active_period())

	@property
	def tasks_failed_count(self):
		return len(self.get_tasks_failed_in_active_period())

	@property
	def tasks_rescheduled_count(self):
		return len(self.get_tasks_rescheduled_in_active_period())

	@property
	def tasks_rescheduled_pre_pickup_count(self):
		return len(self.get_tasks_rescheduled_pre_pickup_in_active_period())

	@property
	def tasks_rescheduled_post_pickup_count(self):
		return len(self.get_tasks_rescheduled_post_pickup_in_active_period())

	@property
	def task_delivery_ratio(self):
		"""
		The fraction of "accepted" requests, i.e. those for which a task was created,
		for which a bundle was delivered.
		"""
		return self.requests_delivered_count / self.tasks_processed_count

	# *************************** BUNDLES *************************
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
	def bundles_acquired_count(self):
		return len(self.get_all_bundles_in_active_period())

	@property
	def bundles_delivered_count(self):
		return len(self.get_bundles_delivered_in_active_period())

	@property
	def bundles_dropped_count(self):
		return len(self.get_bundles_failed_in_active_period())

	@property
	def bundle_delivery_ratio(self):
		return self.bundles_delivered_count / self.bundles_acquired_count

	@property
	def bundle_drop_ratio(self):
		return 1 - self.bundle_delivery_ratio

	@property
	def traffic_load(self):
		return self._traffic_load

	@traffic_load.setter
	def traffic_load(self, v):
		self._traffic_load = v


def init_analytics(duration, ignore_start=0, ignore_end=0, inputs=None):
	"""The analytics module tracks events that occur during the simulation.

	This includes keeping a log of every request, task and bundle object, and counting
	the number of times a specific movement is made (e.g. forwarding, dropping,
	state transition etc).
	"""
	a = Analytics(duration, ignore_start, ignore_end, inputs)

	pub.subscribe(a.submit_request, "request_submit")

	pub.subscribe(a.add_task, "task_add")
	pub.subscribe(a.reschedule_task, "task_reschedule")
	pub.subscribe(a.fail_task, "task_failed")

	pub.subscribe(a.acquire_bundle, "bundle_acquired")
	pub.subscribe(a.deliver_bundle, "bundle_delivered")
	pub.subscribe(a.drop_bundle, "bundle_dropped")

	return a
