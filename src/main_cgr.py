import sys
from copy import deepcopy

import simpy
from pubsub import pub

from main import create_route_tables
from node import Node
from routing import Contact, cgr_yens
from bundles import Bundle, Buffer
from misc import cp_load


SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100000
NUM_BUNDLES = [5, 10]
BUNDLE_ARRIVAL_RATE = 0.2  # Mean number of bundles to be generated per unit time
BUNDLE_TTL = 25  # Time to live for a
CONGESTION = 0.5
TARGET_UID = 999

SCHEDULER_ID = 0
TARGET_ID_BASE = 3000
SATELLITE_ID_BASE = 2000
GATEWAY_ID_BASE = 1000


def init_nodes(nodes, cp):
	"""Create a Node object for each node

	"""
	node_list = []
	for n_uid in nodes:
		n = Node(
			n_uid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queue={x: [] for x in range(1, len(nodes) + 1)},
			contact_plan=deepcopy(cp),
		)
		# Subscribe to any published messages that indicate a bundle has been sent to
		# this node. This will execute the bundle_receive() method on board the
		# receiving node at the time when the FULL bundle has been received, including
		# any delay incurred through travel (OWLT)
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")

		node_list.append(n)

	return node_list


def init_bundles(node):
	bundles = [
		Bundle(src=1, dst=5, size=2, deadline=50, priority=0),
		Bundle(src=1, dst=5, size=4, deadline=50, priority=1),
		Bundle(src=1, dst=5, size=2, deadline=50, priority=0),
	]
	for b in bundles:
		node.buffer.append(b)


if __name__ == "__main__":
	contact_plan = cp_load('contact_plans/cgr_tutorial.txt', 5000)
	node_ids = set([c.frm for c in contact_plan] + [c.to for c in contact_plan])
	nodes = init_nodes(node_ids, contact_plan)
	for n in nodes:
		for n_ in [x for x in nodes if x.uid != n.uid]:
			n.route_table[n_.uid] = cgr_yens(n.uid, n_.uid, n.contact_plan, 0, sys.maxsize)

	# Add some bundles on to node #1
	init_bundles([n for n in nodes if n.uid == 1][0])
	env = simpy.Environment()

	for node in nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts

	env.run(until=100)
	print('')
