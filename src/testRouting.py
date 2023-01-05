import sys
import unittest
from copy import deepcopy

import simpy
from pubsub import pub

from main import init_analytics
from node import Node
from scheduling import Scheduler, Request
from routing import Contact, cgr_yens
from misc import id_generator


def init_contact_plan(n0, n1, n2):
	cp = [
		Contact(n0, n1, n1, 0, 2),
		Contact(n0, n2, n2, 2, 4),
		Contact(n1, n2, n2, 13, 15),
		Contact(n2, n1, n1, 13, 15),
		Contact(n1, n0, n0, 20, 21),
		Contact(n2, n0, n0, 25, 28)
	]
	return cp


def init_contact_plan_targets(n1, n2):
	cp = [
		Contact(n2, 20, 5, 5),
		Contact(n1, 10, 10, 10),
	]
	return cp


def init_requests(env, scheduler):
	wait_times = [0, 2]
	requests = [
		Request(10, deadline_deliver=22, time_created=0, destination=scheduler.uid),
		Request(20, deadline_deliver=30, time_created=2, destination=scheduler.uid)
	]
	while wait_times:
		wait = wait_times.pop(0)
		yield env.timeout(wait)
		request = requests.pop(0)
		print(f'Request submitted for pickup from Target {request.target_id} at time'
		      f' {env.now}')
		scheduler.request_received(request, env.now)


class MyTestCase(unittest.TestCase):
	def setUp(self) -> None:
		scheduler = Node(0, Scheduler())
		node1 = Node(1)
		node2 = Node(2)
		self.nodes = [scheduler, node1, node2]
		self.analytics = init_analytics()

		cp = init_contact_plan(scheduler.uid, node1.uid, node2.uid)
		cpt = init_contact_plan_targets(node1.uid, node2.uid)
		for node in self.nodes:
			node.update_contact_plan(deepcopy(cp), deepcopy(cpt))
			node.outbound_queue = {x.uid: [] for x in self.nodes if x.uid != node.uid}
			pub.subscribe(node.bundle_receive, str(node.uid) + "bundle")
			for n_ in [x for x in [scheduler.uid, node1.uid, node2.uid] if x != node.uid]:
				node.route_table[n_] = cgr_yens(
					node.uid, n_, node.contact_plan, 0, sys.maxsize)

	def tearDown(self) -> None:
		pub.unsubAll()

	def test_show_msr_delivers_both_bundles(self):
		"""Validate benefit of MSR over CGR in a simple example

		The scenario here is:
		- Request 1 arrives at time 0, which gets processed into a task for acquisition
			at time 10 and delivery at time 20. The delivery deadline is 21.
		- Request 2 arrives at time 2, which gets processed into a task for acquisition
			at time 5 and delivery at time 25.
		It is possible for the bundle for R2 to be delivered earlier, however because of
		the capacity constraints on the earlier delivery, which has already been
		assigned to R1's bundle, it is deliberately delayed. In the CGR case, however,
		this is not done, since R2's bundle is "older" and therefore should have priority.

		"""
		print("starting MSR test")
		for node in self.nodes:
			node.msr = True
		env = simpy.Environment()
		env.process(init_requests(env, self.nodes[0]))
		for node in self.nodes:
			env.process(node.bundle_assignment_controller(env))
			env.process(node.contact_controller(env))  # Generator that initiates contacts
		env.run(until=100)

		print(self.analytics.bundles_delivered_count)
		print(self.analytics.bundles_forwarded_count)
		print(self.analytics.bundles_dropped_count)
		print(self.analytics.delivery_latency_ave)

	def test_show_cgr_drops_the_high_priority_bundle(self):
		print("starting CGR test")
		for node in self.nodes:
			node.msr = False
		env = simpy.Environment()
		env.process(init_requests(env, self.nodes[0]))
		for node in self.nodes:
			env.process(node.bundle_assignment_controller(env))
			env.process(node.contact_controller(env))  # Generator that initiates contacts
		env.run(until=100)
		print(self.analytics.bundles_delivered_count)
		print(self.analytics.bundles_forwarded_count)
		print(self.analytics.bundles_dropped_count)
		print(self.analytics.delivery_latency_ave)
		del env


# def network_setup(msr):


if __name__ == '__main__':
	unittest.main()
