#!/usr/bin/env python3

import random
import sys
import json
import cProfile
import pickle
from types import SimpleNamespace
from typing import List

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


def requests_generator(
		env, sources, sinks, moc, inter_arrival_time, size, priority,
		acquire_time, deliver_time
):
	"""
	Generate requests that get submitted to a scheduler where they are processed into
	tasks, added to a task table, and distributed through the network for execution by
	nodes.
	"""
	# num_fails = 0
	while True:
		yield env.timeout(random.expovariate(1 / inter_arrival_time))
		# sources_tried = set()
		# while len(sources_tried) < len(sources):
			# Keep trying different sources (targets) at random until one of them
			# results in a successful task creation
			# source = random.choice(
			# 	[s for s in sources.values() if s.uid not in sources_tried])
			# sources_tried.add(source.uid)
		source = random.choice([s for s in sources.values()])
		acquire_deadline = env.now + acquire_time if acquire_time else sys.maxsize

		request = Request(
			source.uid,
			destination=random.choice(sinks),
			data_volume=size,
			priority=priority,
			deadline_acquire=acquire_deadline,
			bundle_lifetime=deliver_time,
			time_created=env.now,
		)
		moc.request_received(request)
		request = moc.request_queue.pop(0)
		success = moc.process_request(request, env.now)
			# if success:
			# 	break
			# if len(sources_tried) == len(sources):
			# 	num_fails += 1
			# 	print(f"Number of fully failed requests is {num_fails}")


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


def init_space_nodes(
		nodes, cp, cpwt, msr=True, uncertainty: float = 1.0, scheduler=0,
		scheme: List = None
) -> List[Node]:
	if scheme is None:
		scheme = [True, True, True, True, True]
	node_ids = [x for x in nodes]
	# TODO more generalised way to do this??
	node_ids.append(SCHEDULER_ID)
	node_list = []
	for n_uid, n in nodes.items():
		# TODO this is a bit of a hack to get all of the Gateways sharing the same
		#  endpoint ID so that they can all be the "destination". This should be more
		#  flexible, so that we can group nodes together in bespoke ways
		eid = ENDPOINT_ID if isinstance(n, GroundNode) else n_uid

		# Define the scheduler (if it has one)
		sched = Scheduler(
				valid_pickup=scheme[0],
				define_pickup=scheme[1],
				valid_delivery=scheme[2],
				resource_aware=scheme[3],
				define_delivery=scheme[4]
			) if scheduler else None

		n = Node(
			n_uid,
			eid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queue={x: [] for x in node_ids},
			contact_plan=deepcopy(cp),
			contact_plan_targets=deepcopy(cpwt),
			msr=msr,
			uncertainty=uncertainty,
			scheduler=sched
		)
		n.scheduler.parent = n
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")
		pub.subscribe(n.task_table_receive, str(n_uid) + "task_table")
		node_list.append(n)
	print(f"Nodes created, with MSR = {msr}")
	return node_list


def create_route_tables(nodes, destinations, t_now=0) -> None:
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
			)


def init_analytics(duration, ignore_start=0, ignore_end=0, inputs=None):
	"""The analytics module tracks events that occur during the simulation.

	This includes keeping a log of every request, task and bundle object, and counting
	the number of times a specific movement is made (e.g. forwarding, dropping,
	state transition etc).
	"""
	a = Analytics(duration, ignore_start, ignore_end, inputs)

	pub.subscribe(a.submit_request, "request_submit")

	pub.subscribe(a.add_task, "task_add")
	pub.subscribe(a.fail_task, "task_failed")

	pub.subscribe(a.acquire_bundle, "bundle_acquired")
	pub.subscribe(a.deliver_bundle, "bundle_delivered")
	pub.subscribe(a.drop_bundle, "bundle_dropped")

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
	nodes = [*sats, *gws]
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

	return cp


def build_contact_plan(ins, duration, times, sats, gws, tgts):
	rates = get_data_rate_pairs(
		[*sats],
		[*gws],
		ins.satellites.rate_isl,
		ins.satellites.rate_s2g,
		ins.gateways.rate
	)

	# Get Contact Plan from the relative mobility between satellites, targets (sources)
	# and gateways (sinks)
	cp = review_contacts(times, {**sats, **tgts, **gws}, sats, gws, tgts, rates)

	# Add a permanent contact between the MOC and the Gateways so that they can always
	# be up-to-date in terms of the Task Table
	for g_uid, g in gws.items():
		# TODO Fix how we're defining the EIDs here, hardcoding isn't good
		cp.insert(
			0,
			Contact(
				SCHEDULER_ID, g_uid, ENDPOINT_ID, 0, duration,
				sys.maxsize
			)
		)

		cp.insert(
			0,
			Contact(
				g_uid, SCHEDULER_ID, SCHEDULER_ID, 0, duration,
				sys.maxsize
			)
		)

	# FIXME Urghhh
	cp = update_contact_endpoints(cp, [*gws])

	return cp


def build_moc(cp, cpt, sats, gws, scheme: List = None):
	# Instantiate the Mission Operations Center, i.e. the Node at which requests arrive
	# and then set up each of the remote nodes (including both satellites and gateways).
	if scheme is None:
		scheme = [True, True, True, True, True]
	moc = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=deepcopy(cp),
		contact_plan_targets=deepcopy(cpt),
		scheduler=Scheduler(
			valid_pickup=scheme[0],
			define_pickup=scheme[1],
			valid_delivery=scheme[2],
			resource_aware=scheme[3],
			define_delivery=scheme[4]
		),
		outbound_queue={x: [] for x in {**sats, **gws}},
		request_duplication=False
	)
	moc.scheduler.parent = moc
	pub.subscribe(moc.bundle_receive, str(SCHEDULER_ID) + "bundle")

	return moc


def main(inputs_, scheme: List = None, uncertainty: float = 1.0, scheduler: int = 0):
	"""

	"""
	pub.unsubAll()
	random.seed(0)

	# Time for the clean network to reach a steady state
	warm_up = 10800

	# Time after which we ignore any new requests in the analysis
	cool_down = 2 * (
			inputs_.traffic.max_time_to_acquire + inputs_.traffic.max_time_to_deliver)

	full_duration = inputs_.simulation.duration + warm_up + cool_down

	times = [x for x in range(0, full_duration, inputs_.simulation.step_size)]

	targets, satellites, gateways = init_space_network(
		inputs_.simulation.date_start,
		full_duration,
		inputs_.simulation.step_size,
		inputs_.targets,
		inputs_.satellites,
		inputs_.gateways
	)
	print("Node propagation complete")

	contact_plan_base = build_contact_plan(
		inputs_, full_duration, times, satellites, gateways, targets)
	cp_wo_targets = [c for c in contact_plan_base if c.to not in [t for t in targets]]
	cp_only_targets = [c for c in contact_plan_base if c.to in [t for t in targets]]
	print("Contact plans built")

	download_capacity = get_download_capacity(
		cp_wo_targets,
		[*gateways],
		[*satellites]
	)

	request_arrival_wait_time = get_request_inter_arrival_time(
		full_duration,
		download_capacity,
		inputs_.traffic.congestion,
		inputs_.traffic.size
	)

	moc = build_moc(
		cp_wo_targets,
		cp_only_targets,
		satellites,
		gateways,
		scheme
	)

	nodes = init_space_nodes(
		{**satellites, **gateways},
		cp_wo_targets,
		cp_only_targets,
		inputs_.traffic.msr,
		uncertainty,
		scheduler,
		scheme
	)

	create_route_tables(
		nodes=nodes,
		destinations=[ENDPOINT_ID],
	)
	print("Route tables constructed")

	# Set up the analytics module.
	analytics_ = init_analytics(full_duration, warm_up, cool_down, inputs_)

	# ************************ BEGIN THE SIMULATION PROCESS ************************
	# Initiate the simpy environment, which keeps track of the event queue and triggers
	# the next discrete event to take place
	env = simpy.Environment()
	env.process(requests_generator(
		env,
		targets,
		[inputs_.targets.destination],
		moc,
		request_arrival_wait_time,
		inputs_.traffic.size,
		inputs_.traffic.priority,
		inputs_.traffic.max_time_to_acquire,
		inputs_.traffic.max_time_to_deliver
	))

	# Set up the Simpy Processes on each of the Nodes. These are effectively the
	# generators that iterate continuously throughout the simulation, allowing us to
	# jump ahead to whatever the next event is, be that bundle assignment, handling a
	# contact or discovering more routes downstream
	for node in [moc] + nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))

	end_sim = full_duration - (cool_down / 2)
	env.run(until=end_sim)
	# cProfile.run('env.run(until=end_sim)')

	adjusted_download_capacity = download_capacity * (
				inputs_.simulation.duration / full_duration)

	print(f"Total download capacity was {adjusted_download_capacity} units")
	analytics_.traffic_load = analytics_.bundles_acquired_count * inputs_.traffic.size / adjusted_download_capacity

	return analytics_


if __name__ == "__main__":
	"""
	Contact Graph Scheduling implementation
	
	Requests are submitted to a central Scheduler node, which process requests into Tasks 
	that are distributed through a delay-tolerant network so that nodes can execute 
	pick-ups according to their assignation (i.e. bundle acquisition). Acquired bundles 
	are routed through the network using either CGR or MSR, as specified.
	"""
	filename = "input_files//walker_delta_16.json"
	with open(filename, "rb") as read_content:
		inputs = json.load(read_content, object_hook=lambda d: SimpleNamespace(**d))

	analytics_ = main(inputs)

	with open(f"results//results", "wb") as file:
		pickle.dump(analytics_, file)

	print(f"Actual congestion, after considering rejected requests, was {analytics_.traffic_load}")

	print("*** REQUEST DATA ***")
	print(f"{analytics_.requests_submitted_count} Requests were submitted")
	print(f"{analytics_.requests_failed_count} Requests could not be fulfilled")
	print(f"{analytics_.requests_delivered_count} Requests were delivered\n")

	print("*** TASK DATA ***")
	print(f"{analytics_.tasks_processed_count} Tasks were created")
	print(f"{analytics_.tasks_acquired_count} Tasks remain in an 'acquired' state")
	print(f"{analytics_.tasks_delivered_count} Tasks were delivered")
	print(f"{analytics_.tasks_failed_count} Tasks were unsuccessful\n")

	print("*** BUNDLE DATA ***")

	print(f"{analytics_.bundles_acquired_count} Bundles were acquired")
	print(f"{analytics_.bundles_delivered_count} Bundles were delivered")
	print(f"{analytics_.bundles_dropped_count} Bundles were dropped\n")

	print("*** PERFORMANCE DATA ***")
	print(f"The average bundle PICKUP latency is {analytics_.pickup_latency_ave}")
	print(f"The bundle PICKUP latency Std. Dev. is {analytics_.pickup_latency_stdev}")
	print(f"The average bundle DELIVERY latency is {analytics_.delivery_latency_ave}")
	print(f"The bundle DELIVERY latency Std. Dev. is {analytics_.delivery_latency_stdev}")
	print(f"The average bundle REQUEST latency is {analytics_.request_latency_ave}")
	print(f"The bundle REQUEST latency Std. Dev. is {analytics_.request_latency_stdev}")
	print(f"The average HOPS PER DELIVERED BUNDLE is {analytics_.hop_count_average}")
