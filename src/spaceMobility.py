#!/usr/bin/env python3

"""
Functions associated with the mobility patterns of space (satellites) and ground (
gateways and targets) nodes. This is used to generate a contact plan for use within the
CGS framework
"""

import sys
from math import acos, radians
import numpy as np

from misc import slant_range
from spaceNetwork import Spacecraft, GroundNode
from routing import Contact


def review_contacts(all_times, all_nodes, satellites, gateways, targets):
    """
    Given a set of satellites, gateways (destinations) and targets (sources), identify
    the contacts and return a contact plan (list of Contact objects)
    """
    # Identify all of the times at which a contact is possible between node-pairs
    edges = space_connectivity_matrix(
        all_times,
        satellites,
        gateways,
        targets,
    )

    # Concatenate adjacent edges into single "contact" events, so that the dynamic
    # graph is reduced to simply the number of discrete contacts rather than all of
    # time steps that feature a contact.
    cs = build_contact_schedule(all_nodes, edges)

    # Convert the contact schedule into a sequence of NetworkX digraphs
    cs_ = init_contact_schedule(cs)

    # Build the Contact Plan and Network Resource Model
    cp = build_contact_plan(cs_, all_nodes, all_times[-1])

    return cp


def space_connectivity_matrix(
        times: list,
        satellites: dict,
        gateways: dict = None,
        targets: dict = None,
) -> dict:
    """
    Method to identify the connections between targets, satellites and gateways.

    Satellites are considered to have potential to be "in contact" with targets and
    gateways when they are above the minimum elevation above the horizon,
    and in contact with other satellites when closer than some threshold distance from
    another satellite with whom they are able to communicate AND both satellites are
    within the other's respective antenna beam width.

    For satellite pairs and satellite-gateway pairs, once potential visibility has been
    established between each node pair, the associated data rate is calculated (as a
    function of separation distance) and the data transfer capacity is derived at each
    time step. For satellite-target pairs, data transfer capacity is not required.

    :param times: (list) array of the times for the mission simulation
    :param satellites: (dict) Spacecraft objects to be considered
    :param gateways: (dict) Gateway objects to be considered
    :param targets: (dict) Target objects to be considered
    :param isl_dist: (int) distance (in m) between a satellite pair, below which
        contact could be made

    :return edges: (dict) edges that exist at each time step (key). The values are
        tuples of the node pairs in contact at that time
    """
    if not targets:
        targets = {}
    if not gateways:
        gateways = {}

    positions_sats = {x: satellites[x].orbit.eci for x in satellites}
    positions_ground = {
        x: np.array(y.eci) for x, y in {**targets, **gateways}.items()
    }

    positions = {**positions_sats, **positions_ground}

    # For each satellite, identify if it is in contact with a target, satellite and/or
    # gateway at each time step

    # Position vectors between each satellite (k1) and each other node (k2) at every
    # time step (v). Format is pos_vec[k1][k2] = [v1, v2, ..., vt], which each v is a
    # 3-element vector in the Earth Centred Inertial frame, representing the vector
    # from node k1 to node k2
    pos_vec = {}

    # Absolute distance (m) between each satellite (k1) and each other node (k2) at every
    # time step.
    sep = {}

    # Boolean indicating the visibility between node pairs (u, v) at each time step (
    # t). For visibility to be true (from u to v), u must have the v within its antenna
    # beam and v must have u within its antenna beam. If only the former, for example,
    # transmission would be possible but v would not be able to receive the signal
    vis = {}

    for u_uid, u in satellites.items():
        pos_vec[u_uid] = {}
        sep[u_uid] = {}
        vis[u_uid] = {}
        v_others = {
            x: y for x, y in {
                **targets,
                **satellites,
                **gateways
            }.items() if x is not u_uid
        }

        for v_uid, v in v_others.items():

            # Extract the vector (in ECI frame) FROM satellite "u" TO satellite "v"
            pos_vec[u_uid][v_uid] = np.subtract(
                positions[v_uid][:, 0:3],
                positions[u_uid][:, 0:3]
            )
            # Get the magnitude of this vector (i.e. the separation distance)
            sep[u_uid][v_uid] = np.linalg.norm(
                pos_vec[u_uid][v_uid],
                axis=1,
                keepdims=True
            )

            if isinstance(v, Spacecraft):
                # Create array where True represents a potential connection re separation
                vis[u_uid][v_uid] = sep[u_uid][v_uid] < u.comms["isl"].dist_max

            if isinstance(v, GroundNode):
                # FIXME This is innacurate at high/low latitudes due to the oblateness
                #  of the Earth. Should use the elevation angle directly if possible,
                #  rather than converting to a Max Range
                max_range = slant_range(
                    satellites[u_uid].orbit.coe0[0],
                    radians(v.min_el)
                )
                vis[u_uid][v_uid] = sep[u_uid][v_uid] < max_range

    # Define the data rate and contact capacity at each time step between pairs of
    # satellites and gateways
    rate, capacity = get_rate_capacity(satellites, gateways, times, vis, sep)

    # Populate the edges list to include the rates and capacities at each time step
    edges = add_edges(satellites, gateways, targets, rate, capacity, times, vis)

    clear_position_vectors(
        satellites.values(),
        {**targets, **gateways}.values()
    )

    return edges


def build_contact_schedule(nodes, edges):
    """
    Construct a dict containing information about each contact between
    nodes in the network. Each key in the dict is a time, with a value representing the
    list of contacts that begin at that time, and the value is the
    contact attributes, such as the nodes involved, the duration and the
    total data transfer capacity
    :param nodes:
    :param edges:
    :return:
    """
    # Dict to hold the connections that are assumed to form actual contacts during
    # flight. E.g. at a particular time step, it might be possible for node A to have
    # an edge to both nodes B and C, however the entry in engaged entry for that time
    # would just have one pair featuring A, assuming it is a node that can be in a
    # single contact at any one time.
    engaged = {t: [] for t in edges}

    # List to show which pairs were active in a contact during the previous time step
    active_prev = []

    for t_, e_ in edges.items():
        to_delete = []
        all_edges = [x["nodes"] for x in e_]

        # For each pair that was active in the previous step, check to see if the
        # contact has ended or if a better connection has been made, in which case need to
        # end the current contact.
        for pair in active_prev:

            # If either the pair are no longer active in the current step, the contact has
            # concluded so end it
            if pair not in all_edges:
                to_delete.append(pair)

            # Otherwise, the contact must still be active, in which case add the pair
            # to the respective entry in the tracking list
            else:
                engaged[t_].append(pair)

        # Remove the pairs, that have ended, from the active list
        [active_prev.remove(x) for x in to_delete]

        # Now consider the connections that have emerged, or become available this step
        # if at least one contact exists
        for e in [x for x in all_edges if x not in engaged[t_]]:
            u = e[0]
            v = e[1]

            if isinstance(nodes[v], GroundNode) and \
                    (u, v) not in engaged[t_]:
                active_prev.append((u, v))
                engaged[t_].append((u, v))
                continue

            if (u, v) not in engaged[t_] and (u, v) not in to_delete:
                active_prev.append((u, v))
                engaged[t_].append((u, v))

    # Based on the contacts that have been selected being "active", build the actual
    # Contact Schedule that holds the contacts, their duration and capacity in a dict
    # with the key being the start time of the contact.
    schedule = [(k, v) for k,  v in edges.items()]
    contacts = {}
    idx = -1
    for es in schedule:
        idx += 1
        contacts[es[0]] = []
        while engaged[es[0]]:
            for connection in es[1]:
                u = connection["nodes"][0]
                v = connection["nodes"][1]
                if (u, v) not in engaged[es[0]]:
                    continue
                cap = connection["capacity"]
                engaged[es[0]].remove((u, v))

                # for each edge downstream of the contact we're looking at
                for downstream in schedule[idx + 1:]:
                    # If the contact is still ongoing, add 1 to the counter and
                    # extract this contact from the schedule for the next iteration
                    if (u, v) in engaged[downstream[0]]:
                        cap += downstream[1][
                            [x['nodes'] for x in downstream[1]].index((u, v))
                        ]['capacity']

                        engaged[downstream[0]].remove((u, v))

                    # Else if the contact has ended, compile the duration of the contact
                    # and create an entry in the edges list
                    else:
                        t_span = downstream[0] - es[0]

                        # if the "other" node in the contact is a target
                        if isinstance(nodes[v], GroundNode) and nodes[v].is_source:
                            contacts[es[0]].append((
                                v,
                                u,
                                create_edge(
                                    1,
                                    es[0],
                                    t_span,
                                    0.
                                )
                            ))

                        # if both nodes in the contact are either satellites or gateways
                        else:
                            # add an edge to the contact schedule
                            contacts[es[0]].append((
                                u,
                                v,
                                create_edge(
                                    cap,
                                    es[0],
                                    t_span,
                                    nodes[u].cost['transmit']
                                )
                            ))
                        break

    # Trim off all of the empty times
    cs = {t: e for t, e in contacts.items() if e}

    return cs


def get_rate_capacity(satellites, gateways, times, vis, sep):
    capacity = {}
    rate = {}
    for u_uid, u in satellites.items():
        rate[u_uid] = {}
        capacity[u_uid] = {}
        v_others = {
            x: y for x, y in {
                **satellites,
                **gateways
            }.items() if x is not u_uid
        }
        for v_uid, v in v_others.items():
            if isinstance(v, GroundNode):
                if not rate.get(v_uid):
                    rate[v_uid] = {u_uid: []}
                    capacity[v_uid] = {u_uid: []}
                else:
                    rate[v_uid][u_uid] = []
                    capacity[v_uid][u_uid] = []

            capacity[u_uid][v_uid] = []
            rate[u_uid][v_uid] = []
            for x in range(len(times)):
                if vis[u_uid][v_uid][x] and x != len(times)-1:
                    # Get the data rate according to the distance separation
                    r = u.comms[u.comm_pairs[v_uid]].rate_from_dist(
                        sep[u_uid][v_uid][x, 0]
                    )
                    rate[u_uid][v_uid].append(r)

                    if isinstance(v, Spacecraft):
                        # Add the capacity, which is calculated as HALF the amount that
                        # could be transferred during the entire contact due to the
                        # half-duplex property of the ISL
                        # TODO Is there a better, more parametric way of doing the
                        #  capacity? Should we actually be starting the contact at the
                        #  half way point for one of the satellites? How would we even
                        #  decide that?
                        capacity[u_uid][v_uid].append(r * (times[x+1] - times[x]) / 2)
                    else:
                        # Space to Ground links assumed to be full duplex
                        capacity[u_uid][v_uid].append(r * (times[x + 1] - times[x]))

                    if isinstance(v, GroundNode):
                        # Get the rate and capacity from the Gateway to the Satellite
                        # Get the data rate according to the distance separation
                        # Using the S/C "g2s" here, since that is what dictates the
                        # rates that are feasible for the S/C.
                        r_gs = v.comms[v.comm_pairs[u_uid]].rate_from_dist(
                            sep[u_uid][v_uid][x, 0]
                        )
                        rate[v_uid][u_uid].append(r_gs)
                        capacity[v_uid][u_uid].append(r_gs * (times[x + 1] - times[x]))

                else:
                    rate[u_uid][v_uid].append(0)
                    capacity[u_uid][v_uid].append(0)

                    if isinstance(v, GroundNode):
                        rate[v_uid][u_uid].append(0)
                        capacity[v_uid][u_uid].append(0)

    return rate, capacity


def add_edges(satellites, gateways, targets, rate, capacity, times, vis):
    edges = {t: [] for t in times}
    for u in satellites:
        v_sat_gw = {
            x: y for x, y in {
                **gateways,
                **satellites
            }.items() if x is not u
        }
        for v in v_sat_gw:
            try:
                [edges[t].append(
                    {
                        'nodes': (u, v),
                        'rate': rate[u][v][idx],
                        'capacity': capacity[u][v][idx]
                    }
                ) for idx, t in enumerate(times) if vis[u][v][idx]]
            except:
                print('')

            [edges[t].append(
                {
                    'nodes': (v, u),
                    'rate': rate[v][u][idx],
                    'capacity': capacity[v][u][idx]
                }
            ) for idx, t in enumerate(times) if
                vis[u][v][idx] and gateways.get(v)]

        for v in targets:
            [edges[t].append(
                {
                    'nodes': (u, v),
                    'rate': 0,
                    'capacity': 0
                }
            ) for idx, t in enumerate(times) if vis[u][v][idx]]

    return edges


def clear_position_vectors(sats, grounds):
    for x in grounds:
        del x.eci
    for x in sats:
        del x.orbit


def init_contact_schedule(edges):
    """
    A Contact schedule is a collection of static graphs that represents an Evolving
    Graph. Each static graph represents the formation of a new edge. Edge existence is
    captured by the duration of each static graph, such that a new graph
    is not required to represent loss of an edge only.
    :param edges:
    :return cs: populated ContactSchedule object
    """
    cs = {}
    # Add static graphs to the contact schedule by importing the base digraph from the
    # contact schedule and adding specific attributes associated with the specific time
    # step of the graph
    for t, edges in edges.items():
        for e in edges:
            add_edge_to_contact_schedule(
                cs, t, e[0], e[1], e[2]["capacity"], e[2]["duration"], e[2]["cost"])
    return cs


def build_contact_plan(cs):
    """
    Return a table (pandas Dataframe) of contact opportunity
    :param cs:
    :param nodes:
    :return:
    """
    k = 0
    contacts = []

    for t, dg in cs.items():
        for edge in dg:
            contacts.append(
                Contact(
                    edge["from"],
                    edge["to"],
                    edge["time"],
                    edge["time"] + edge["duration"]
                )
            )
            k += 1

    return contacts


def create_edge(capacity, time, duration, cost):
    """
    Method to create an edge (contact) that represent contact between two nodes

    :param capacity: Contact transfer capacity (float)
    :param time: Time at which contact starts
    :param duration: Duration of contact
    :param cost: Cost of transferring data over contact
    """
    d = {
        'capacity': capacity,  # contact capacity (data vol)
        'time': time,  # time of contact
        'duration': duration,  # duration of contact
        'cost': cost  # cost to send data from v}
    }

    return d


def add_edge_to_contact_schedule(cs, t, frm, to, capacity, duration, cost):
    if not cs.get(t):
        cs[t] = [
            {
                "from": frm,
                "to": to,
                "time": t,
                "capacity": capacity,
                "duration": duration,
                "cost": cost,
            }
        ]
    else:
        cs[t].append(
            {
                "from": frm,
                "to": to,
                "time": t,
                "capacity": capacity,
                "duration": duration,
                "cost": cost,
            }
        )
