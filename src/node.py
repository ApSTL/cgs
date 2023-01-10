#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List, Dict, Set
from copy import deepcopy

from pubsub import pub

from scheduling import Scheduler, Request, Task
from bundles import Buffer, Bundle
from routing import candidate_routes, cgr_yens
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
    eid: int = None
    scheduler: Scheduler = None
    buffer: Buffer = field(default_factory=lambda: Buffer())
    outbound_queue: Dict = field(default_factory=dict)
    contact_plan: List = field(default_factory=list)
    contact_plan_targets: List = field(default_factory=list)
    request_duplication: bool = False
    msr: bool = True

    _bundle_assign_repeat: int = field(init=False, default=BUNDLE_ASSIGN_REPEAT_TIME)
    _outbound_repeat_interval: int = field(init=False, default=OUTBOUND_QUEUE_INTERVAL)
    route_table: Dict = field(init=False, default_factory=dict)
    request_queue: List = field(init=False, default_factory=list)
    handled_requests: List = field(init=False, default_factory=list)
    failed_requests: List = field(init=False, default_factory=list)
    task_table: Dict = field(init=False, default_factory=dict)
    drop_list: List = field(init=False, default_factory=list)
    delivered_bundles: List = field(init=False, default_factory=list)
    _task_table_updates: Dict = field(init=False, default_factory=dict)
    _targets: Set = field(init=False, default_factory=set)
    _contact_plan_self: List = field(init=False, default_factory=list)
    _contact_plan_dict: Dict = field(init=False, default_factory=dict)
    _eid: str = field(init=False, default_factory=lambda: id_generator())
    _outbound_queue_all: List = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if not self.eid:
            self.eid = self.uid

        self.update_contact_plan(self.contact_plan, self.contact_plan_targets)
        if self.scheduler:
            self.scheduler.parent = self

        # TODO If the OBQ gets updated after initiation, this will get missed.
        self._task_table_updates = {n: [] for n in self.outbound_queue}

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
    def request_received(self, request):
        """
        When a request is received, it gets added to the request queue.
        """
        self.request_queue.append(request)

    def process_all_requests(self, curr_time):
        """Process each request in the queue, by earliest-arrival first.

        By identifying the assignee-target contact that will collect the payload,
        creating a Task for this and adding it to the table
        :return:
        """
        while self.request_queue:
            request = self.request_queue.pop(0)
            self.process_request(request, curr_time)

    def process_request(self, request: Request, curr_time: int | float):
        """Process a single request resulting in a Task being added to the task table.

        In the event that a request cannot be fulfilled, it gets added to the failed list
        Args:
            :param request: Request object
            :param curr_time: Current time
        """
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
                return True

        task = self.scheduler.schedule_task(
            request,
            curr_time,
            self.contact_plan,
            self.contact_plan_targets
        )

        # If a task has been created (i.e. the request can be fulfilled), add the task to
        # the table. Else, that request cannot be fulfilled
        if task:
            request.status = "scheduled"
            self.task_table[task.uid] = task
            self._update_task_change_tracker(task.uid, [])
            return True

        # If no assignee has been identified, then it means there's no feasible way
        # the data can be acquired and delivered that fulfills the requirements.
        # TODO add in some exception that handles a lack of feasible acquisition
        # print(f"No task was created for request {request.uid} as either acquisition "
        #    f"or delivery wasn't feasible")
        pub.sendMessage("request_fail")
        request.status = "failed"
        self.failed_requests.append(request)
        return False

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
        """Procedure to follow if we're in contact w/ a Target node.

        Essentially, we need to check whether there's a "pending" task for pickup from
        the target with whom we're in contact and, if there's an assignee (and by
        association a pick-up time) the ID matches ours
        """
        for task_id, task in self.task_table.items():

            # If the task's target is not the node we're in contact with, skip
            if task.target != target:
                continue

            # If the task is not needing to be executed
            if task.status != "pending":
                continue

            # If the task has been assigned to different node, skip
            if task.assignee and task.assignee != self.uid:
                continue

            # If the task has a LATER scheduled pickup time, skip
            if task.pickup_time and task.pickup_time > t_now:
                continue

            # If there's insufficient buffer capacity to complete the task, and the
            # task has been scheduled to be acquired by us, at this time, set the task
            # to "redundant" so it can be rescheduled. Otherwise, just skip so that it
            # can perhaps be handled later, if still pending
            # TODO Implement re-scheduling so that we can reassign this task to the
            #  next best opportunity. Otherwise, we'll just wait until a viable
            #  opportunity and complete it then.
            if self.buffer.capacity_remaining < task.size:
                # if task.assignee and task.assignee == self.uid and task.pickup_time and\
                #         task.pickup_time == t_now:
                #     task.status = "redundant"
                continue

            # Otherwise, pick up the bundle :)
            self._acquire_bundle(t_now, task)
            task.acquired(t_now, self.uid)

    def _acquire_bundle(self, t_now, task):
        bundle_deadline = t_now + task.lifetime
        bundle = Bundle(
            src=self.uid,
            dst=task.destination,
            target_id=task.target,
            size=task.size,
            deadline=bundle_deadline,
            created_at=t_now,
            priority=task.priority,
            task_id=task.uid,
            task=task,
            obey_route=self.msr,
            current=self.uid

        )
        self.buffer.append(bundle)
        if DEBUG:
            print(f"^^^ Bundle acquired on node {self.uid} at time {t_now} from target {task.target}")
        if task.del_path and self.msr:
            bundle.route = task.del_path
        pub.sendMessage("bundle_acquired", b=bundle)

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
            if self._task_table_updates[contact.to]:
                env.process(self._task_table_send(
                        env,
                        contact.to,
                        contact.owlt,
                        [t for t in self.task_table.values()
                         if t.uid in self._task_table_updates[contact.to]]
                    )
                )
                self._task_table_updates[contact.to] = []
                yield env.timeout(0)
                continue

            # If we don't have any bundles waiting in the current neighbour's outbound
            # queue, we can just wait a bit and try again later
            if not self.outbound_queue[contact.to]:
                yield env.timeout(self._outbound_repeat_interval)
                continue

            bundle = self._pop_from_outbound_queue(contact.to)
            send_time = bundle.size / contact.rate
            # Check that there's a sufficient amount of time remaining in the contact
            if contact.end - env.now < send_time:
                self._return_bundle_to_buffer(bundle)
                continue

            # If, for some reason, the bundle does not have an assigned route,
            # return to the buffer so that it can be assigned (or dropped)
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
                self.task_table[bundle.task_id].delivered(env.now, self.uid, contact.to)
                self._update_task_change_tracker(bundle.task_id, [])

            # Wait until the bundle has been sent (note it may not have
            # been fully received at this time, due to the OWLT, but that's
            # fine as we can start sending the next already)
            yield env.timeout(send_time)

        # Add any bundles that couldn't fit across the contact back in to the
        #  buffer so that they can be assigned to another outbound queue.
        self._return_outbound_queue_to_buffer(contact.to)

        if DEBUG:
            print(f"contact between {self.uid} and {contact.to} ended at {env.now}")

    def _handshake(self, env, to, delay):
        """
        Carry out the handshake at the beginning of the contact,
        """
        env.process(self._task_table_send(
            env,
            to,
            delay,
            [t for t in self.task_table.values() if t.uid in self._task_table_updates[to]]
        ))
        self._task_table_updates[to] = []

    def _update_task_change_tracker(self, task_id: str, excluded: List[int]):
        """Updates dict that tracks tasks that may have changed for each other node.

        This method appends the task ID to each node in the dict to indicate something
        has changed with this task such that it should be shared in case an update is
        required on the other node.
        """
        for node, tasks in self._task_table_updates.items():
            if node in excluded:
                continue
            tasks.append(task_id)

    def _task_table_send(self, env, to, delay, updated_tasks):
        while True:
            yield env.timeout(delay)
            # Wait until the whole message has arrived and then invoke the "receive"
            # method on the receiving node
            pub.sendMessage(
                str(to) + "task_table",
                task_table={t.uid: t for t in updated_tasks},
                frm=self.uid
            )
            break

    def task_table_receive(self, task_table, frm):
        self._merge_task_tables(task_table, frm)

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
                t_now=env.now, bundle=bundle
            )

            return

    def bundle_receive(self, t_now, bundle):
        """
        Receive bundle from neighbouring node.

        If the bundle is too large to be accommodated, reject, else accept
        """
        if self.buffer.capacity_remaining < bundle.size:
            # TODO Handle the case where a bundle is too large to be accommodated
            pass
            return

        bundle.hop_count += 1
        bundle.current = self.uid

        if bundle.dst == self.eid:
            if DEBUG:
                print(f"*** Bundle delivered to {self.uid} from {bundle.previous_node} at"
                      f" {t_now:.1f}")
            bundle.delivered_at = t_now
            pub.sendMessage("bundle_delivered", b=bundle, t_now=t_now)
            self.delivered_bundles.append(bundle)
            if self.task_table:
                self.task_table[bundle.task_id].delivered(
                    t_now, bundle.previous_node, self.uid)
                bundle.task.requests[0].status = "delivered"
                self._update_task_change_tracker(bundle.task_id, [])
            return

        if DEBUG:
            print(f"<<< Bundle received on {self.uid} from {bundle.previous_node} at"
                  f" {t_now:.1f}")

        pub.sendMessage("bundle_forwarded")
        self.buffer.append(bundle)
        # TODO it may be good to invoke the bundle assignment here, because otherwise
        #  we're perhaps waiting until the next time step, since this event muight
        #  happen after this node has invoked its own bundle assignment for this
        #  time-step. However, that is a realistic scenario if, indeed BA is only
        #  triggered at those regular intervals.

    # *** ROUTE SELECTION, BUNDLE ENQUEUEING AND RESOURCE CONSIDERATION ***
    def route_table_eval(self, t_now):
        """Review the Route Tables and, if deemed necessary, refresh them.

        Each route table stores potential routes to a specific destination endpoint. If
        the number of routes available, or the minimum route volume is below some
        threshold, regenerate this route table.
        """
        for dest in self.route_table:

            # TODO Make the Route Table a class and update these things as necessary,
            #  so that we don't need to do it on the fly each time
            # Remove any route that has already passed
            self.route_table[dest] = [r for r in self.route_table[dest] if
                                      r.hops[0].end > t_now]

            self.contact_plan = [c for c in self.contact_plan if c.end > t_now]
            self.contact_plan_targets = [c for c in self.contact_plan_targets if c.end > t_now]

            # Get the time at which the very last route currently ends
            if self.route_table[dest]:
                max_delivery_time = max([r.hops[-1].end for r in self.route_table[dest]])
            else:
                max_delivery_time = 0

            # TODO need a better approach to this
            # if max_delivery_time < self.buffer.final_deadline_for_destination(dest):
            if max_delivery_time < t_now + 5000:
                self.route_table[dest] = self._route_discovery(
                    dest,
                    t_now,
                    t_now + 10000,  # TODO
                )

    def _route_discovery(self, destination, from_time, end_time):
        return cgr_yens(self.uid, destination, self.contact_plan, from_time, end_time)

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
        new_bundles_assigned = False

        if not self.buffer.is_empty():
            self.route_table_eval(t_now)

        while not self.buffer.is_empty():
            new_bundles_assigned = True
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
                    # FIXME there's a chance that this route won't be feasible in
                    #  terms of resources, such that we reduce them to below zero.
                    #  How to handle this...
                    self._append_to_outbound_queue(b, next_hop.to)
                    hops = []
                    for hop in b.route:
                        hops.append(self._contact_plan_dict[hop])
                    self._contact_resource_update(hops, b.size, b.priority)
                    continue
                else:
                    if DEBUG:
                        print(f"Bundle not able to traverse its MSR route on {self.uid}"
                              f" at {t_now}")
                    b.route = []
                    b.obey_route = False

            candidates = candidate_routes(
                t_now, self.uid, self.contact_plan, b, self.route_table[b.dst], [],
                self.outbound_queue
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
                self._append_to_outbound_queue(b, route.hops[0].to)

                # Update the resources on the selected route
                self._contact_resource_update(route.hops, b.size, b.priority)

                # Update the "assigned route" argument on the bundle object
                b.route = [contact.uid for contact in route.hops]
                break

            if not assigned:
                b.dropped_at = t_now
                self.drop_list.append(b)
                if DEBUG:
                    print(f"XXX Bundle dropped from network at {t_now} on node"
                          f" {self.uid}")
                pub.sendMessage("bundle_dropped", bundle=b)

        # Check for any over-booking of contacts and, if required, carry out the bundle
        # assignment again for any bundles that have been put back into the Buffer
        if new_bundles_assigned:
            self._contact_over_booking()
            if not self.buffer.is_empty():
                self._bundle_assignment(t_now)

    def _return_outbound_queue_to_buffer(self, to):
        """Return the contents of the outbound queue to the buffer.

        This process will also result in resources that were originally assigned for
        the movement of this bundle, to be replenished so that they are not double-counted
        """
        while self.outbound_queue[to]:
            bundle = self._pop_from_outbound_queue(to)
            self._return_bundle_to_buffer(bundle)

    def _append_to_outbound_queue(self, bundle: Bundle, to: int) -> None:
        """Add a bundle to an outbound queue.

        Args:
            bundle: Bundle object to be added to OBQ
            to: Node to which this bundle is to be sent
        """
        self.outbound_queue[to].append(bundle)
        self._outbound_queue_all.append(bundle)

    def _pop_from_outbound_queue(self, to: int) -> Bundle:
        """Extract a bundle from the Outbound Queue.

        Args:
            to: Node to which this bundle is destined for transmission
        """
        bundle = self.outbound_queue[to].pop(0)
        self._outbound_queue_all.remove(bundle)
        return bundle

    def _return_bundle_to_buffer(self, bundle):
        if bundle.route:
            hops = []
            for hop in bundle.route:
                hops.append(self._contact_plan_dict[hop])
            self._contact_resource_update(hops, -bundle.size, bundle.priority)
        self.buffer.append(bundle)
        if DEBUG:
            print(f"returned bundle to Buffer on {self.uid}")

    @staticmethod
    def _contact_resource_update(contacts: list, size: int | float, priority: int = 0) -> None:
        """Consume or replenish resources on a Contact.

        Contact volume is reduced (if data is being sent) or increased (if data is no
        longer being sent) according to traffic flow

        Args:
            contacts: IDs of the contacts on which resources should be updated
            size: Volume of the data being transferred over the contact
        """
        if priority not in [0, 1, 2]:
            raise ValueError("Bundle priority not defined in valid range")

        for contact in contacts:
            for p in range(priority+1):
                contact.mav[p] -= size

    def _contact_over_booking(self) -> None:
        """Return bundles to the buffer until no over-booked contacts.

        While we're over-booked on at least one contact, pop bundles from the list of Bs
        that have been assigned already and, if they use at least one of the
        over-booked contacts, add them back into the Buffer. This will replenish
        resources on each of the contacts to which the bundle was assigned. Once no
        over-booked contacts exist, add the bundles that were popped, but not returned
        to the buffer, back in to the assigned list.
        """
        overbooked_contacts = []
        for contact in self.contact_plan:
            if min(contact.mav) < 0 and contact not in overbooked_contacts:
                overbooked_contacts.append(contact)
        if not overbooked_contacts:
            return

        return_to_obq = []
        self._outbound_queue_all.sort()
        while any([min(c.mav) < 0 for c in overbooked_contacts]):
            bundle = self._outbound_queue_all.pop()
            if set(bundle.route) & set([x.uid for x in overbooked_contacts]):
                self.outbound_queue[self._contact_plan_dict[bundle.route[0]].to].remove(bundle)
                bundle.obey_route = False
                self._return_bundle_to_buffer(bundle)
            else:
                return_to_obq.append(bundle)
        self._outbound_queue_all.extend(return_to_obq)

    def _merge_task_tables(self, tt_other, frm):
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
            self._update_task_change_tracker(task_id, excluded=[frm])
