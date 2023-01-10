#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List

from pubsub import pub

from routing import Route, Contact, cgr_dijkstra
from misc import id_generator


@dataclass
class Request:
    target_id: int = None
    target_lat: float = None
    target_lon: float = None
    target_alt: float = None
    deadline_acquire: int = sys.maxsize
    # deadline_deliver: int = sys.maxsize
    bundle_lifetime: int = sys.maxsize
    priority: int = 0
    destination: int = 999
    data_volume: int = 1
    time_created: int = None
    __uid: str = field(init=False, default_factory=lambda: id_generator())
    status: str = "initiated"

    @property
    def uid(self):
        return self.__uid


@dataclass
class Task:
    """
    Args:
        status: Options include: "pending", "acquired", "redundant", "rescheduled",
            "delivered" or "failed"
    """
    deadline_acquire: int = sys.maxsize
    lifetime: int = sys.maxsize
    target: int = 0
    priority: int = 0
    destination: int = None
    size: int = 1
    assignee: int = None
    scheduled_at: int | float = None
    scheduled_by: int = None
    pickup_time: int | float = None  # Intended pick-up time
    delivery_time: int | float = None  # Intended delivery time
    acq_path: List = field(default_factory=list)
    del_path: List = field(default_factory=list)
    request_ids: List = field(default_factory=list)
    requests: List[Request] = field(default_factory=list)

    acquired_at: int | float = field(init=False, default=None)
    acquired_by: int = field(init=False, default=None)
    delivered_at: int | float = field(init=False, default=None)
    delivered_by: int = field(init=False, default=None)
    delivered_to: int = field(init=False, default=None)
    failed_at: int | float = field(init=False, default=None)
    failed_on: int = field(init=False, default=None)
    status: str = field(init=False, default="pending")
    __uid: str = field(init=False, default_factory=lambda: id_generator())

    @property
    def uid(self):
        return self.__uid

    def acquired(self, t, by):
        self.status = "acquired"
        self.acquired_at = t
        self.acquired_by = by

    def delivered(self, t, by, to):
        self.status = "delivered"
        self.delivered_at = t
        self.delivered_by = by
        self.delivered_to = to

    def failed(self, t, node):
        self.status = "failed"
        self.failed_at = t
        self.failed_on = node

    def __lt__(self, other):
        """Order Tasks based on their status value

        Task ordering is required when merging Task Tables and identifying which of two
        tasks are the most "up-to-date", resulting in the other one being updated to match
        """
        if self.status == other.status:
            return False
        if self.status == "pending":
            return True
        if self.status == "acquired" and other.status != "pending":
            return True
        if self.status == "redundant" and \
                (other.status != "pending" and other.status != "acquired"):
            return True
        return False

    def __repr__(self):
        return "Task: ID %s | Target %d | Assignee %s | Status %s | pickup time %d" % (
            self.__uid, self.target, self.assignee, self.status, self.pickup_time
        )


@dataclass
class Scheduler:
    """The Scheduler is an object that enables a node to carry out Contact Graph
    Scheduling operations. I.e. it can schedule tasks in response to requests, based on
    a certain Contact Plan

    Args:
        parent: The Node object on which this Scheduler is located
        valid_pickup: If true, valid pickup (i.e. before deadline) must exist
        valid_delivery: If true, valid delivery (i.e. before deadline) must exist.
            Note: cannot be true valid_pickup is not True
        define_pickup: If true, pickup (acquisition) information is defined on the Task
        define_delivery: If true, delivery information (route) is defined on the Task
    """
    parent = None
    valid_pickup: bool = True
    valid_delivery: bool = True
    define_pickup: bool = True
    define_delivery: bool = True
    resource_aware: bool = True

    def __post_init__(self):
        # Need to make sure we're not defining a need to specify pickup or delivery
        # information if we're not required to check valid routes.
        if not self.valid_pickup:
            self.valid_delivery = False
            self.define_pickup = False
            self.resource_aware = False
        if not self.valid_delivery:
            self.define_delivery = False
            self.resource_aware = False
        if not self.define_delivery:
            self.resource_aware = False

    def schedule_task(self, request: Request, curr_time: int | float, contact_plan: list,
                      contact_plan_targets: list) -> Task | None:
        """
        Identify the contact, between a satellite & target, in which the request should
        be fulfilled and return a task that includes the necessary info. The process to do
        this is effectively two-executions of Dijkstra's algorithm, whereby routes to
        the acquisition opportunity are found, followed immediately by finding the
        shortest route to the destination from that point.

        As requests are handled, the main contact schedule is updated to account for
        the resources used, such that future requests are scheduled based on the
        network state having already processed previous requests.

        Args:
            request: Request object that is being processed into a Task
            curr_time: Current time
            contact_plan: List of Contact objects on which the scheduling will occur
            contact_plan_targets: List of Contact objects with target nodes

        Returns:
            task: a Task object (if possible), else None

        """
        # TODO This is fine while we have a contact plan with target contacts
        #  available, but in the more general case we're more likely to simply have a
        #  target location with which we need to evaluate (in real time) contact
        #  opportunities

        # If we're not checking for a valid pickup opportunity, then we can simply
        # create a task without any assignments
        if not self.valid_pickup:
            return self._create_task(request, curr_time)

        # Add target contacts to the Contact Plan
        contact_plan.extend([c for c in contact_plan_targets if c.to == request.target_id])

        # If we need to check for a valid pickup, but NOT for a valid delivery,
        # we can just do a Dijkstra search to the first pick-up opportunity
        if not self.valid_delivery:
            root = Contact(
                self.parent.uid, self.parent.uid, self.parent.eid, curr_time,
                sys.maxsize, sys.maxsize)
            root.arrival_time = curr_time
            acq_path = cgr_dijkstra(root, request.target_id, contact_plan, request.deadline_acquire)
            if not acq_path:
                return None

            assignee = acq_path.hops[-1].frm if self.define_pickup else None
            return self._create_task(request, curr_time, assignee)

        # If we've reached here, then we must need to check for both a valid
        # acquisition AND a valid delivery, so execute CGS to ensure this.
        acq_path, del_path = self._cgs_routing(self.parent.uid, request, curr_time,
                                               contact_plan)

        # Remove any contacts with this target before moving on, so that we don't clutter
        [
            contact_plan.remove(x) for x in [
                c for c in contact_plan_targets if c.to == request.target_id
            ]
        ]

        if acq_path and del_path:
            if self.resource_aware:
                for hop in del_path.hops:
                    hop.volume -= request.data_volume

            if not self.define_pickup:
                assignee = None
                pickup_time = None
                acq_path_ = None
            else:
                assignee = del_path.hops[0].frm
                pickup_time = acq_path.best_delivery_time
                acq_path_ = [x.uid for x in acq_path.hops]

            if not self.define_delivery:
                delivery_time = None
                del_path_ = None
            else:
                delivery_time = del_path.best_delivery_time
                del_path_ = [x.uid for x in del_path.hops]

            return self._create_task(
                request, curr_time, assignee, pickup_time, delivery_time, acq_path_,
                del_path_)


    def _cgs_routing(self, src: int, request: Request, curr_time: int, contact_plan
                     ) -> tuple:
        """
        Find the acquisition and delivery paths such that the data is delivered at the
        earliest opportunity.
        The general procedure is:
        ---------------------------------------------------------------------------------
        While a possible better acquisition opportunity exists:
        -- Find the route to the earliest acquisition opportunity
        -- If either no route is found, or the time of acquisition is greater than the
        earliest time of delivery:
        ---- break
        -- Remove all contacts between the most recent assignee and the target
        -- Find the shortest delivery route from the most recent acquisition opportunity
        -- If a delivery route is found:
        ---- add the acquisition-delivery route pair to the list of potential paths
        ---------------------------------------------------------------------------------
        :param src: ID of the starting Contact going from/to source node (self)
        :param request: Request object defining the target node ID, deadlines, etc
        :return:
        """
        path_acq_selected = None
        path_del_selected = None
        earliest_delivery_time = sys.maxsize

        # reset contacts
        for contact in contact_plan:
            contact.clear_dijkstra_area()
            contact.clear_management_area()

        # Root contact is the connection to self that acts as the source vertex in the
        # Contact Graph
        root = Contact(src, src, src, curr_time, sys.maxsize, sys.maxsize)
        root.arrival_time = curr_time

        while True:
            # Find the lowest cost acquisition path using Dijkstra
            path_acq = cgr_dijkstra(
                root, request.target_id, contact_plan, request.deadline_acquire)

            if not path_acq or path_acq.best_delivery_time >= earliest_delivery_time:
                break

            # suppress all acquisition opportunities from this node so that it's not
            # considered in any other searches (later acquisitions cannot be better)
            self._suppress_contacts_from_node(
                path_acq.hops[-1].frm,
                request.target_id,
                contact_plan
            )

            # Create a root contact from which we can find a delivery path
            # TODO a hack to reducing the risk of bundles being scheduled over contacts
            #  they may not be able to traverse
            # current_contacts = [
            #     c for c in contact_plan
            #     if c.frm == path_acq.hops[-1].frm and
            #        c.start < path_acq.best_delivery_time < c.end
            # ]
            #
            # if current_contacts:
            #     earliest_next_contact = max([c.end for c in current_contacts])
            # else:
            #     earliest_next_contact = path_acq.best_delivery_time
            #
            # root_delivery = Contact(
            #     path_acq.hops[-1].frm,
            #     path_acq.hops[-1].frm,
            #     path_acq.hops[-1].frm,
            #     earliest_next_contact,
            #     sys.maxsize,
            #     sys.maxsize
            # )
            # root_delivery.arrival_time = earliest_next_contact

            root_delivery = Contact(
                path_acq.hops[-1].frm,
                path_acq.hops[-1].frm,
                path_acq.hops[-1].frm,
                path_acq.best_delivery_time,
                sys.maxsize,
                sys.maxsize
            )
            root_delivery.arrival_time = path_acq.best_delivery_time

            # Identify best route to the destination from our current acquiring node
            path_del = cgr_dijkstra(
                root_delivery,
                request.destination,
                contact_plan,
                path_acq.best_delivery_time + request.bundle_lifetime,
                request.data_volume
            )

            # If there are no valid routes to the destination from this target
            # acquisition, skip
            if not path_del:
                continue

            # If this delivery route is better than our current "best", assign it
            # FIXME This really needs to be treated like the forwarding process,
            #  considering backlog as well as actual times at which bundles can
            #  feasibly reach the destination. This currently just goes some way to
            #  making sure we're not getting delivered before we've even acquired... If
            #  our delivery path was multiple hops, and our bundle was large, it would
            #  take time for each hop to be completed, even if they're all connected
            #  from the time it begins.
            current_bdt = max(
                path_del.best_delivery_time,
                root_delivery.arrival_time + sum(
                    [c.owlt + c.rate * request.data_volume for c in path_del.hops]
                )
            )

            if current_bdt < earliest_delivery_time:
                earliest_delivery_time = current_bdt
                path_acq_selected = path_acq
                path_del_selected = path_del

        return path_acq_selected, path_del_selected

    def _create_task(self, request, t_now, assignee=None, pickup_time=None,
                     delivery_time=None, acq_path_=None, del_path_=None) -> Task:
        task = Task(
            deadline_acquire=request.deadline_acquire,
            lifetime=request.bundle_lifetime,
            target=request.target_id,
            priority=request.priority,
            destination=request.destination,
            size=request.data_volume,
            assignee=assignee,
            scheduled_at=t_now,
            scheduled_by=self.parent.uid,
            pickup_time=pickup_time,
            delivery_time=delivery_time,
            acq_path=acq_path_,
            del_path=del_path_
        )

        task.request_ids.append(request.uid)
        task.requests.append(request)
        pub.sendMessage("task_add", t=task)
        return task

    @staticmethod
    def _suppress_contacts_from_node(node_from, node_to, contact_plan):
        for contact in contact_plan:
            # TODO Check the removal of "int(...)" here doesn't cause issues. We should
            #  be able to use strings as the UIDs without any issue, so I don't see why
            #  we need to convert to int
            if contact.frm == node_from and contact.to == node_to:
                contact.suppressed = True
