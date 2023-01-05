#!/usr/bin/env python3

import random
import sys
import json
import cProfile

from copy import deepcopy
import simpy
from pubsub import pub

from node import Node
from routing import Contact, cgr_yens
from scheduling import Scheduler, Request
from bundles import Buffer, Bundle
from spaceNetwork import setup_satellites, setup_ground_nodes, GroundNode
from spaceMobility import review_contacts
from analytics import Analytics


SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100000
NUM_BUNDLES = [5, 10]
BUNDLE_ARRIVAL_RATE = 0.2  # Mean number of bundles to be generated per unit time
BUNDLE_SIZE = [1, 5]
BUNDLE_TTL = 25  # Time to live for a

SCHEDULER_ID = 0
ENDPOINT_ID = 999999  # EID for "destinations"
TARGET_ID_BASE = 3000
SATELLITE_ID_BASE = 2000
GATEWAY_ID_BASE = 1000


def get_request_inter_arrival_time(sim_time, outflow, congestion, size) -> int:
	"""Returns the mean time between request arrivals based on congestion target.

	Given a certain amount of delivery capacity (i.e. the long-term average rate of
	delivery per unit time), some target level of congestion in the network (ratio of
	inflow to outflow) and the size of each bundle (i.e. the package generated in
	response to a request), return the mean time to wait between request arrivals.
	"""
	return (sim_time * size) / (outflow * congestion)


def requests_generator(env, sources, sinks, moc, inter_arrival_time, size, priority, ttl):
	"""
	Generate requests that get submitted to a scheduler where they are processed into
	tasks, added to a task table, and distributed through the network for execution by
	nodes.
	"""
	num_fails = 0
	while True:
		yield env.timeout(random.expovariate(1 / inter_arrival_time))
		sources_tried = set()
		while len(sources_tried) < len(sources):
			# Keep trying different sources (targets) at random until one of them
			# results in a successful task creation
			source = random.choice(
				[s for s in sources.values() if s.uid not in sources_tried])
			sources_tried.add(source.uid)
			request = Request(
				source.uid,
				destination=random.choice(sinks),
				data_volume=size,
				priority=priority,
				bundle_lifetime=ttl,
				time_created=env.now,
			)
			moc.request_received(request)
			request = moc.request_queue.pop(0)
			success = moc.process_request(request, env.now)
			if success:
				pub.sendMessage("request_submit", r=request)
				break
			if len(sources_tried) == len(sources):
				num_fails += 1
				print(f"Number of fully failed requests is {num_fails}")


def bundle_generator(env, sources, destinations):
	"""
	Process that generates bundles on nodes according to some probability for the
	duration of the simulation
	"""
	while True:
		yield env.timeout(random.expovariate(BUNDLE_ARRIVAL_RATE))
		source = random.choice(sources)
		dests = [x for x in destinations if x.uid != source.uid]
		destination = random.choice(dests)
		size = random.randint(*BUNDLE_SIZE)
		deadline = env.now + BUNDLE_TTL
		print(
			f"bundle generated on node {source.uid} at time {env.now} for destination"
			f" {destination.uid}")
		b = Bundle(
			src=source.uid, dst=destination.uid, target_id=source.uid, size=size,
			deadline=deadline, created_at=env.now, current=source.uid)
		source.buffer.append(b)
		pub.sendMessage("bundle_acquired", b=b)


def init_space_nodes(nodes, cp, cpwt, msr=True):
	node_ids = [x for x in nodes]
	# TODO more generalised way to do this??
	node_ids.append(SCHEDULER_ID)
	node_list = []
	for n_uid, n in nodes.items():
		# TODO this is a bit of a hack to get all of the Gateways sharing the same
		#  endpoint ID so that they can all be the "destination". This should be more
		#  flexible, so that we can group nodes together in bespoke ways
		eid = ENDPOINT_ID if isinstance(n, GroundNode) else n_uid
		n = Node(
			n_uid,
			eid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queue={x: [] for x in node_ids},
			contact_plan=deepcopy(cp),
			contact_plan_targets=deepcopy(cpwt),
			msr=msr
		)
		# n._targets = targets
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")
		node_list.append(n)
	print(f"Nodes created, with MSR = {msr}")
	return node_list


def create_route_tables(nodes, destinations, t_now=0, end_time=sys.maxsize) -> None:
	"""
	Route Table creation - Invokes Yen's CGR algorithm to discover routes between
	node-pairs, stores them in a dictionary and updates the route table on each node
	"""
	for n in nodes:
		for d in [x for x in destinations if x != n.uid]:
			n.route_table[d] = cgr_yens(
				n.uid,
				d,
				n.contact_plan,
				t_now,
				end_time,
			)


def init_analytics(ignore_start=0, ignore_end=0):
	"""The analytics module tracks events that occur during the simulation.

	This includes keeping a log of every request, task and bundle object, and counting
	the number of times a specific movement is made (e.g. forwarding, dropping,
	state transition etc).
	"""
	a = Analytics(inputs["simulation"]["duration"], ignore_start, ignore_end)

	pub.subscribe(a.submit_request, "request_submit")
	pub.subscribe(a.fail_request, "request_fail")
	pub.subscribe(a.duplicated_request, "request_duplicated")

	pub.subscribe(a.add_task, "task_add")
	pub.subscribe(a.redundant_task, "task_redundant")  # TODO
	pub.subscribe(a.fail_task, "task_failed")  # TODO
	pub.subscribe(a.renew_task, "task_renew")  # TODO

	pub.subscribe(a.add_bundle, "bundle_acquired")
	pub.subscribe(a.deliver_bundle, "bundle_delivered")
	pub.subscribe(a.forward_bundle, "bundle_forwarded")
	pub.subscribe(a.drop_bundle, "bundle_dropped")
	pub.subscribe(a.reroute_bundle, "bundle_reroute")  # TODO

	return a


def init_space_network(epoch, duration, step_size, targets_, satellites_, gateways_):
	targets = setup_ground_nodes(
		epoch,
		duration,
		step_size,
		targets_,
		is_source=True,
		id_counter=TARGET_ID_BASE
	)

	satellites = setup_satellites(
		epoch,
		duration,
		step_size,
		satellites_,
		counter=SATELLITE_ID_BASE
	)

	gateways = setup_ground_nodes(
		epoch,
		duration,
		step_size,
		gateways_,
		id_counter=GATEWAY_ID_BASE
	)

	return targets, satellites, gateways


def get_download_capacity(contact_plan, sinks, sats):
	"""Return the total delivery capacity from satellites to gateway nodes

	The total download capacity is the sum of the data transfer capacity from all
	possible download opportunities (i.e. from satellite to gateway)
	"""
	# TODO This does not consider any overlap restrictions that may exist
	total = 0
	for contact in contact_plan:
		if contact.frm in sats and contact.to in sinks:
			total += contact.volume
	return total


def get_data_rate_pairs(sats, gws, s2s, s2g, g2s):
	nodes = [*satellites, *gateways]
	rate_pairs = {}
	for n1 in nodes:
		rate_pairs[n1] = {}
		for n2 in [x for x in nodes if x != n1]:
			if n1 in sats:
				if n2 in sats:
					rate = s2s
				else:
					rate = s2g
			elif n1 in gws:
				if n2 in sats:
					rate = g2s
				else:
					rate = sys.maxsize
			rate_pairs[n1][n2] = rate
	return rate_pairs


def update_contact_endpoints(cp, gateways):
	"""For all contacts with a gateway as the receiving node, update the Contact's EID
	to be the destination EID.
	"""
	for contact in cp:
		if contact.to in gateways:
			contact.to_eid = ENDPOINT_ID


if __name__ == "__main__":
	"""
	Contact Graph Scheduling implementation
	
	Requests are submitted to a central Scheduler node, which process requests into Tasks 
	that are distributed through a delay-tolerant network so that nodes can execute 
	pick-ups according to their assignation (i.e. bundle acquisition). Acquired bundles 
	are routed through the network using either CGR or MSR, as specified.
	"""
	random.seed(0)

	# ****************** SPACE NETWORK SETUP ******************
	# set up the space network nodes (satellites and gateways, and if known in advance,
	# the targets)
	filename = "input_files//walker_delta_16.json"
	with open(filename, "r") as read_content:
		inputs = json.load(read_content)

	sim_epoch = inputs["simulation"]["date_start"]
	sim_duration = inputs["simulation"]["duration"]
	sim_step_size = inputs["simulation"]["step_size"]
	times = [x for x in range(0, sim_duration, sim_step_size)]
	# FIXME This won't work if we have multiple types of bundles with different sizes
	bundle_size = inputs["traffic"]["size"]

	targets, satellites, gateways = init_space_network(
		sim_epoch, sim_duration, sim_step_size, inputs["targets"], inputs["satellites"],
		inputs["gateways"]
	)
	print("Node propagation complete")

	rates = get_data_rate_pairs(
		[*satellites],
		[*gateways],
		inputs["satellites"]["rate_isl"],
		inputs["satellites"]["rate_s2g"],
		inputs["gateways"]["rate"]
	)

	# Get Contact Plan from the relative mobility between satellites, targets (sources)
	# and gateways (sinks)
	cp = review_contacts(
		times,
		{**satellites, **targets, **gateways},
		satellites,
		gateways,
		targets,
		rates
	)
	print("Contact Plans built")

	# ****************** SCHEDULING SPECIFIC PREPARATION ******************
	# Create a contact plan that ONLY has contacts with target nodes and a contact plan
	# that ONLY has contacts NOT with target nodes. The target CP will be used to
	# extend the non-target one during request processing, but since target nodes don't
	# participate in routing, they slow down the route discovery process if considered.
	cp_with_targets = [c for c in cp if c.to in [t for t in targets]]
	cp = [c for c in cp if c.to not in [t for t in targets]]

	# Add a permanent contact between the MOC and the Gateways so that they can always
	# be up-to-date in terms of the Task Table
	for g_uid, g in gateways.items():
		# TODO Fix how we're defining the EIDs here, hardcoding isn't good
		cp.insert(0, Contact(SCHEDULER_ID, g_uid, ENDPOINT_ID, 0, sim_duration, sys.maxsize))
		cp.insert(0, Contact(g_uid, SCHEDULER_ID, SCHEDULER_ID, 0, sim_duration, sys.maxsize))

	# Instantiate the Mission Operations Center, i.e. the Node at which requests arrive
	# and then set up each of the remote nodes (including both satellites and gateways).
	moc = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=cp,
		contact_plan_targets=cp_with_targets,
		scheduler=Scheduler(),
		outbound_queue={x: [] for x in {**satellites, **gateways}},
		request_duplication=False
	)
	moc.scheduler.parent = moc
	pub.subscribe(moc.bundle_receive, str(SCHEDULER_ID) + "bundle")

	download_capacity = get_download_capacity(
		cp,
		[*gateways],
		[*satellites]
	)

	# TODO while this request wait time is based on the download capacity and
	#  congestion values, this doesn't ensure we actually DO all of these. Indeed,
	#  given a limited time horizon and TTL, many of these won't get completed,
	#  such that we're going to be way under our congestion-level. I don't think
	#  there's an easy way to do this, in terms of request arrival being the driver,
	#  since it could be the case whereby ALL of the requests that come in are for
	#  targets that don't have a feasible solution. We can't just keep adding requests
	#  as we'll never reach our preferred level of congestion. If we just increase the
	#  TTL and make sure that the whole target set is serviced on a fairly regular
	#  basis, we should be able to ensure execution.
	request_arrival_wait_time = get_request_inter_arrival_time(
			sim_duration,
			download_capacity,
			inputs["traffic"]["congestion"],
			bundle_size
		)

	# FIXME Urghhh
	update_contact_endpoints(cp, [*gateways])

	nodes = init_space_nodes(
		{**satellites,  **gateways},
		cp,
		cp_with_targets,
		inputs["traffic"]["msr"]
	)

	# TODO Replace this with locally invoked Route Discovery or central route discovery
	#  and realistic deployment of the tables through the network
	create_route_tables(
		nodes=nodes,
		destinations=[ENDPOINT_ID],
		end_time=inputs["traffic"]["lifetime"]
	)
	print("Route tables constructed")

	analytics = init_analytics(1000, 1000)

	# ************************ BEGIN THE SIMULATION PROCESS ************************
	# Initiate the simpy environment, which keeps track of the event queue and triggers
	# the next discrete event to take place
	env = simpy.Environment()
	env.process(requests_generator(
		env,
		targets,
		[inputs["targets"]["destination"]],
		moc,
		request_arrival_wait_time,
		bundle_size,
		inputs["traffic"]["priority"],
		inputs["traffic"]["lifetime"]
	))

	# Set up the Simpy Processes on each of the Nodes. These are effectively the
	# generators that iterate continuously throughout the simulation, allowing us to
	# jump ahead to whatever the next event is, be that bundle assignment, handling a
	# contact or discovering more routes downstream
	for node in [moc] + nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generator that does regular route discovery. This
		#  will effectively be something that runs every so often and makes sure we
		#  have a sufficient number of routes in our route tables with enough capacity.
		#  We could actually have something that watches our Route Tables and triggers
		#  the Route Discovery whenever we drop below a certain number of good options

	# env.run(until=sim_duration)
	cProfile.run('env.run(until=sim_duration)')

	print(f"Total download capacity was {download_capacity} units")
	print("*** REQUEST DATA ***")
	print(f"{analytics.requests_submitted_count} Requests were submitted")
	print(f"{analytics.requests_failed_count} Requests could not be fulfilled")
	print(f"{analytics.requests_duplicated_count} Requests already handled by existing tasks\n")
	print("*** TASK DATA ***")
	print(f"{analytics.tasks_processed_count} Tasks were created")
	print(f"{analytics.tasks_failed_count} Tasks were unsuccessful\n")
	print("*** BUNDLE DATA ***")
	print(f"{analytics.bundles_acquired_count} Bundles were acquired")
	print(f"{analytics.bundles_forwarded_count} Bundles were forwarded")
	print(f"{analytics.bundles_delivered_count} Bundles were delivered")
	print(f"{analytics.bundles_dropped_count} Bundles were dropped\n")
	print("*** PERFORMANCE DATA ***")
	print(f"The average bundle DELIVERY latency is {analytics.delivery_latency_ave}")
	print(f"The bundle DELIVERY latency Std. Dev. is {analytics.delivery_latency_stdev}")
	print(f"The average bundle REQUEST latency is {analytics.request_latency_ave}")
	print(f"The bundle REQUEST latency Std. Dev. is {analytics.request_latency_stdev}")

	print('')

