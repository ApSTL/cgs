#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List, Dict, Set
from copy import deepcopy

from pubsub import pub

from scheduling import Scheduler, Request, Task
from bundles import Buffer, Bundle
from routing import candidate_routes
from misc import id_generator


OUTBOUND_QUEUE_INTERVAL = 1
BUNDLE_ASSIGN_REPEAT_TIME = 1
DEBUG = True


@dataclass
class Node:
    """
    A Node object is a network element that can participate, in some way, to the data
    scheduling, generation, routing and/or delivery process.

    Args:
        request_duplication: A flag to indicate whether (True) or not (False) new
            requests can be appended to existing tasks, should that task technically
            already fulfil the request demand.
        msr: Flag indicating use of Moderate Source Routing, if possible
    """
    uid: int
    scheduler: Scheduler = None
    buffer: Buffer = field(default_factory=lambda: Buffer())
    outbound_queues: Dict = field(default_factory=dict)
    contact_plan: List = field(default_factory=list)
    contact_plan_targets: List = field(default_factory=list)
    request_duplication: bool = False
    msr: bool = True

    _bundle_assign_repeat: int = field(init=False, default=BUNDLE_ASSIGN_REPEAT_TIME)
    _outbound_repeat_interval: int = field(init=False, default=OUTBOUND_QUEUE_INTERVAL)
    route_table: Dict = field(init=False, default_factory=dict)
    request_queue: List = field(init=False, default_factory=list)
    handled_requests: List = field(init=False, default_factory=list)
    task_table: Dict = field(init=False, default_factory=dict)
    drop_list: List = field(init=False, default_factory=list)
    delivered_bundles: List = field(init=False, default_factory=list)
    _task_table_updated: bool = field(init=False, default=False)
    _targets: Set = field(init=False, default_factory=set)
    _contact_plan_self: List = field(init=False, default_factory=list)
    _contact_plan_dict: Dict = field(init=False, default_factory=dict)
    _eid: str = field(init=False, default_factory=lambda: id_generator())

    def __post_init__(self) -> None:
        self.update_contact_plan(self.contact_plan)
        if self.scheduler:
            self.scheduler.parent = self

    def update_contact_plan(self, cp=None, cp_targets=None):
        if cp:
            self.contact_plan = cp
            self._contact_plan_self = [c for c in cp if c.frm == self.uid]
            # Create a dict versions of the contact plan to ease resource modification.
            # This allows us to update the resources directly of the contacts to which a
            # bundle is assigned, rather than having to search through the whole list
            # for a matching ID
            self._contact_plan_dict = {c.uid: c for c in cp}

        if cp_targets:
            self.contact_plan_targets = cp_targets
            self._contact_plan_self.extend(
                [c for c in self.contact_plan_targets if c.frm == self.uid]
            )
            self._targets = set([c.to for c in cp_targets])

        self._contact_plan_self.sort()

    # *** REQUEST HANDLING (I.E. SCHEDULING) ***
    def request_received(self, request, t_now):
        """
        When a request is received, it gets added to the request queue
        """
        self.request_queue.append(request)

        # TODO this will trigger the request processing immediately having received a
        #  request, however we may want to set this process to be periodic
        self._process_requests(t_now)

    def _process_requests(self, curr_time):
        """
        Process each request in the queue, by identifying the assignee-target contact
        that will collect the payload, creating a Task for this and adding it to the table
        :return:
        """
        while self.request_queue:
            request = self.request_queue.pop(0)
            self.handled_requests.append(request)

            # Check to see if any existing tasks exist that could service this request.
            if self.request_duplication:
                task_ = self._task_already_servicing_request(request)
                if task_:
                    task_.request_ids.append(request.uid)
                    # TODO Note that this won't necessarily be shared throughout the
                    #  network, since it's not really an "update to the task. Tbh,
                    #  it won't matter that much, since the remote node doesn't need to
                    #  know details about the request(s) its servicing, but could be
                    #  good to ensure it's shared
                    pub.sendMessage("request_duplicated")
                    continue

            task = self.scheduler.schedule_task(
                request,
                curr_time,
                self.contact_plan,
                self.contact_plan_targets
            )

            # If a task has been created (i.e. there is a feasible acquisition and
            # delivery opportunity), add the task to the table. Else, that request
            # cannot be fulfilled so log something to that effect
            if task:
                self.task_table[task.uid] = task
                self._task_table_updated = True

    def _task_already_servicing_request(self, request: Request) -> Task | None:
        """Returns True if the request is already handled by an existing Task.

        Check to see if any of the existing tasks would satisfy the request. I.e. the
        target ID is the same and the (ideal) time of acquisition is at, or after,
        the request arrival time. Effectively, this bundle could be delivered in
        response to this request.

        Args:
            request: A Request object

        Returns:
            A boolean indicating whether (True) or not (False) the request is already
            being handled by an existing task
        """
        for task in self.task_table.values():
            if task.target == request.target_id and task.pickup_time >= \
                    request.time_created:
                return task

    # *** CONTACT HANDLING ***
    def contact_controller(self, env):
        """Generator that iterates over every contact in which this node is the sender.

        Iterates over the contacts in which self is the sending node and invokes the
        contact procedure so that, if appropriate, bundles are forwarded to the current
        neighbour. Once the contact has concluded, we exit from this contact, but until
        that point the contact procedure remains live so that any bundles/Task Table
        updates that arrive during the contact can be shared (if applicable)
        """
        while self._contact_plan_self:
            next_contact = self._contact_plan_self.pop(0)
            time_to_contact_start = next_contact.start - env.now

            # Delay until the contact starts and then resume
            yield env.timeout(time_to_contact_start)
            if next_contact.to in self._targets:
                self._target_contact_procedure(env.now, next_contact.to)
            else:
                env.process(self._node_contact_procedure(env, next_contact))

    def _target_contact_procedure(self, t_now, target):
        """
        Procedure to follow if we're in contact w/ a Target node
        """
        for task_id, task in self.task_table.items():
            if task.pickup_time == t_now and task.target == target:
                # If there's insufficient buffer capacity to complete the task,
                # set the task to "redundant" so it can be rescheduled
                if self.buffer.capacity_remaining < task.size:
                    task.status = "redundant"
                    continue
                self._acquire_bundle(t_now, task)

    def _acquire_bundle(self, t_now, task):
        bundle_lifetime = min(task.deadline_delivery, t_now + task.lifetime)
        bundle = Bundle(
            src=self.uid,
            dst=task.destination,
            target_id=task.target,
            size=task.size,
            deadline=bundle_lifetime,
            created_at=t_now,
            priority=task.priority,
            task_id=task.uid,
            obey_route=self.msr
        )
        self.buffer.append(bundle)
        if DEBUG:
            print(f"^^^ Bundle acquired on node {self.uid} at time {t_now} from target {task.target}")
        if task.del_path and self.msr:
            bundle.route = task.del_path
        pub.sendMessage("bundle_acquired", b=bundle)
        task.status = "acquired"

    def _node_contact_procedure(self, env, contact):
        """
        Carry out the contact with a neighbouring node. This involves a handshake (if
        applicable/possible), sending of data and closing down contact
        """
        if DEBUG:
            print(f"contact started on {self.uid} with {contact.to} at {env.now}")
        self._handshake(env, contact.to, contact.owlt)
        while env.now < contact.end:
            # If the task table has been updated while we've been in this contact,
            # send that before sharing any more bundles as it may be of value to the
            # neighbour
            if self._task_table_updated:
                env.process(self._task_table_send(
                        env,
                        contact.to,
                        contact.owlt,
                    )
                )
                # FIXME This will "switch off" the task table update flag for everyone,
                #  so, e.g. if we're in contact with two nodes and one of them sends
                #  through an update such that this flag goes true, if we then
                #  immediately respond to that node with the updated TT, we'll not get
                #  the trigger to send to the other neighbour.
                self._task_table_updated = False
                yield env.timeout(0)
                continue

            # If we don't have any bundles waiting in the current neighbour's outbound
            # queue, we can just wait a bit and try again later
            if not self.outbound_queues[contact.to]:
                yield env.timeout(self._outbound_repeat_interval)
                continue

            bundle = self.outbound_queues[contact.to].pop(0)
            send_time = bundle.size / contact.rate
            # Check that there's a sufficient amount of time remaining in the contact
            if contact.end - env.now < send_time:
                self._return_bundle_to_buffer(bundle)
                continue

            # If, for some reason, the bundle does not have an assigned route,
            # return to the buffer so that it can be reassigned (or dropped)
            if not bundle.route:
                self._return_bundle_to_buffer(bundle)
                continue

            next_hop = self._contact_plan_dict[bundle.route[0]]
            # If the next hop in the bundle's route is NOT the current neighbour, skip
            if next_hop.to != contact.to:
                self._return_bundle_to_buffer(bundle)
                continue

            # If the bundle is restricted to it's assigned route ONLY, and the next hop
            # in the bundle's route is not this current contact, skip
            if bundle.obey_route and bundle.route[0] != contact.uid:
                self._return_bundle_to_buffer(bundle)
                continue

            # If we've reached this point, we're good to send the bundle
            env.process(
                self._bundle_send(env, bundle, contact.to, contact.owlt+send_time)
            )

            if contact.to == bundle.dst and self.task_table:
                self.task_table[bundle.task_id].status = "delivered"
                self._task_table_updated = True

            # Wait until the bundle has been sent (note it may not have
            # been fully received at this time, due to the OWLT, but that's
            # fine as we can start sending the next already)
            yield env.timeout(send_time)

        if DEBUG:
            print(f"contact between {self.uid} and {contact.to} ended at {env.now}")
        # Add any bundles that couldn't fit across the contact back in to the
        #  buffer so that they can be assigned to another outbound queue.
        self._return_outbound_queue_to_buffer(contact.to)

    def _handshake(self, env, to, delay):
        """
        Carry out the handshake at the beginning of the contact,
        """
        env.process(self._task_table_send(env, to, delay))

    def _task_table_send(self, env, to, delay):
        while True:
            yield env.timeout(delay)
            # Wait until the whole message has arrived and then invoke the "receive"
            # method on the receiving node
            pub.sendMessage(
                str(to) + "bundle",
                t_now=env.now, bundle=deepcopy(self.task_table), is_task_table=True
            )
            break

    def _bundle_send(self, env, bundle, to_node, delay):
        """
        Send bundle to current neighbour.

        Args:
            env: Simpy Environment object
            bundle: Bundle object
            to_node: ID of neighbouring node to whom the bundle is being sent
            delay: duration for the bundle to fully arrive at the neighbour (includes
                time to send plus the time to traverse the contact (one-way-light-time)
        """
        while True:
            if DEBUG:
                print(f">>> Bundle sent from {self.uid} to {to_node} at time {env.now} "
                      f"size {bundle.size}, total delay {delay:.1f}")

            # Wait until the whole message has *arrived* and then invoke the "receive"
            # method on the receiving node. This is the earliest time at which the
            # receiving node can do anything with this bundle
            bundle.previous_node = self.uid
            bundle.update_age(env.now)
            bundle.route.pop(0)
            yield env.timeout(delay)
            pub.sendMessage(
                str(to_node) + "bundle",
                t_now=env.now, bundle=bundle, is_task_table=False
            )

            return

    def bundle_receive(self, t_now, bundle, is_task_table=False):
        """
        Receive bundle from neighbouring node. This also includes the receiving of Task
        Tables, as indicated by the flag in the args.

        If the bundle is too large to be accommodated, reject, else accept
        """
        if is_task_table:
            self._merge_task_tables(bundle)
            return

        if self.buffer.capacity_remaining < bundle.size:
            # TODO Handle the case where a bundle is too large to be accommodated
            pass
            return

        bundle.hop_count += 1

        if bundle.dst == self.uid:
            if DEBUG:
                print(f"*** Bundle delivered to {self.uid} from {bundle.previous_node} at"
                      f" {t_now:.1f}")
            pub.sendMessage("bundle_delivered", b=bundle, t_now=t_now)
            self.delivered_bundles.append(bundle)
            if self.task_table:
                self.task_table[bundle.task_id].status = "delivered"
                self._task_table_updated = True
            return

        if DEBUG:
            print(f"<<< Bundle received on {self.uid} from {bundle.previous_node} at"
                  f" {t_now:.1f}")

        pub.sendMessage("bundle_forwarded")
        self.buffer.append(bundle)

    # *** ROUTE SELECTION, BUNDLE ENQUEUEING AND RESOURCE CONSIDERATION ***
    def bundle_assignment_controller(self, env):
        """Repeating process that kicks off the bundle assignment procedure.
        """
        while True:
            self._bundle_assignment(env.now)
            yield env.timeout(self._bundle_assign_repeat)

    def _bundle_assignment(self, t_now):
        """Select routes, and enqueue (for transmission) bundles residing in the buffer.

        For each bundle in the buffer, identify the route over which it should
        be sent and reduce the resources along each Contact in that route accordingly.
        Stop once all bundles have been assigned a route. Bundles for which a
        feasible route (i.e. one that ensures delivery before the bundle's expiry) is
        not available shall be dropped from the buffer.
        :return:
        """
        while not self.buffer.is_empty():
            assigned = False
            b = self.buffer.extract()

            # If the use of Moderate Source Routing is encouraged, then we should check
            # to see if a nominal (and feasible) route exists on the bundle. If it
            # does, use it, else remove the existing (infeasible) route if exists and
            # assign based on routes in the route table.
            if b.route and b.obey_route:
                next_hop = self._contact_plan_dict[b.route[0]]
                # If the next hop in the bundle's intended journey has not yet
                # finished, add it to that next node's outbound queue. Otherwise,
                # remove the route and use CGR.
                if next_hop.end > t_now and next_hop.frm == self.uid:
                    # TODO there's a chance that this route won't be feasible in
                    #  terms of resources, such that we reduce them to below zero.
                    #  How to handle this...
                    self.outbound_queues[next_hop.to].append(b)
                    hops = []
                    for hop in b.route:
                        hops.append(self._contact_plan_dict[hop])
                    self._contact_resource_update(hops, b.size)
                    continue
                else:
                    if DEBUG:
                        print(f"Bundle not able to traverse its MSR route on {self.uid}"
                              f"at {t_now}")
                    b.route = []
                    b.obey_route = False

            candidates = candidate_routes(
                t_now, self.uid, self.contact_plan, b, self.route_table[b.dst], []
            )

            for route in candidates:
                # If any of the nodes along this route are in the "excluded nodes"
                # list, then we shouldn't assign it along this route
                # TODO in CGR, this simply looks at the "next node" rather than the
                #  receiving node in all hops, but why send the bundle along a route that
                #  includes a node it shouldn't be routed via??
                # if any(hop.to in b.excluded_nodes for hop in route.hops):
                #     continue

                # if the route is not of higher value than the current best
                # route, break from for loop as none of the others will be better
                # TODO change this if converting to generic value rather than arrival time
                if route.best_delivery_time > b.deadline:
                    continue

                # Check each of the hops and make sure the bundle can actually traverse
                # that hop based on the current time and the end time of the hop
                # TODO this should really take into account the backlog over each
                #  contact and the first & last byte transmission times for this
                #  bundle. Currently, we assume that we can traverse the contact IF it
                #  ends after the current time, however in reality there's more to it
                #  than this
                for hop in route.hops:
                    if hop.end <= t_now:
                        continue

                # If this route cannot accommodate the bundle, skip
                if route.volume < b.size:
                    continue

                assigned = True
                # b.base_route = [int(x.uid) for x in route.hops]

                # Add the bundle to the outbound queue for the bundle's "next node"
                self.outbound_queues[route.hops[0].to].append(b)

                # Update the resources on the selected route
                self._contact_resource_update(route.hops, b.size)

                # Update the "assigned route" argument on the bundle object
                b.route = [contact.uid for contact in route.hops]
                break

            if not assigned:
                self.drop_list.append(b)
                if DEBUG:
                    print(f"XXX Bundle dropped from network at {t_now} on node"
                          f" {self.uid}")
                pub.sendMessage("bundle_dropped")

    def _return_outbound_queue_to_buffer(self, to):
        """Return the contents of the outbound queue to the buffer.

        This process will also result in resources that were originally assigned for
        the movement of this bundle, to be replenished so that they are not double-counted
        """
        while self.outbound_queues[to]:
            bundle = self.outbound_queues[to].pop()
            self._return_bundle_to_buffer(bundle)

    def _return_bundle_to_buffer(self, bundle):
        # TODO maybe better to use MAV here, since we know the priority
        if bundle.route:
            hops = []
            for hop in bundle.route:
                hops.append(self._contact_plan_dict[hop])
            self._contact_resource_update(hops, -bundle.size)
        self.buffer.append(bundle)
        if DEBUG:
            print(f"returned bundle to Buffer on {self.uid}")

    @staticmethod
    def _contact_resource_update(contacts: list, data_size: int | float) -> None:
        """Consume or replenish resources on a Contact.

        Contact volume is reduced (if data is being sent) or increased (if data is no
        longer being sent) according to traffic flow

        Args:
            contact: ID of the contact on which resources should be updated
            data_size: Volume of the data being transferred over the contact
        """
        for contact in contacts:
            contact.volume -= data_size

    def _merge_task_tables(self, tt_other):
        """
        Compare two task tables and return one with the most up to dat information
        """
        # Extract the IDs of the tasks present on both tables
        shared_tasks = self.task_table.keys() & tt_other.keys()

        # For each item in the task table we're comparing against, if the task is
        # either not shared, or is "greater than", replace the one in our table
        for task_id, task in tt_other.items():
            if task_id in shared_tasks:
                if not self.task_table[task_id] < task:
                    continue
            self.task_table[task_id] = deepcopy(task)
            self._task_table_updated = True
