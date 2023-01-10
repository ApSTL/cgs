from math import radians, pi, sin, cos, sqrt, degrees

import numpy as np
from scipy.integrate import odeint
from src.misc import gast, topo_to_eci, mee_to_cart, mee_to_coe, coe_to_mee, \
    generate_even_dist_on_earth, eci_to_geod


class GroundNode:
    def __init__(self, uid=None, lat=None, lon=None, alt=None, min_el=0,
                 is_source=False):
        """
        Base class for a node that lies on the Earth's surface. This can be inherited
        into other Earth-based nodes that require a geographical location etc.

        :param lat: (float) Geodetic latitude of the target location (degrees)
        :param lon: (float) Geodetic longitude of the target location (degrees)
        :param alt: (float) altitude above sea level (m)
        :param name: (str) optional name for the target object
        :param min_el: (float) Minimum elevation above the horizon (degrees)
        """
        self.uid = uid
        self._lat = lat
        self._lon = lon
        self._alt = alt
        self._min_el = min_el
        self.is_source = is_source
        self.eci = None

    @property
    def lat(self):
        return self._lat

    @lat.setter
    def lat(self, value):
        if value > 90 or value < -90:
            print(self.name, 'latitude is out of range')
        else:
            self._lat = radians(value)

    @property
    def lon(self):
        return self._lon

    @lon.setter
    def lon(self, value):
        if value > 180 or value < -180:
            print(self.name, 'longitude is out of range')
        else:
            self._lon = radians(value)

    @property
    def alt(self):
        return self._alt

    @alt.setter
    def alt(self, value):
        if value > 8848:
            raise ValueError(
                self.name,
                'is at an altitude greater than Mt Everest, are you sure?'
            )
        else:
            self._alt = value

    @property
    def min_el(self):
        return self._min_el

    @min_el.setter
    def min_el(self, el):
        if el > 90:
            raise AttributeError(el)

        else:
            self._min_el = radians(el)

    def eci_coords(self, jd0, duration, t_step):
        """
        Return the Earth centered Inertial coordinates for the gateway for some period
        of time, at each time step
        :param jd0: initial julian date
        :param duration: length of time for which to get coordinates (s)
        :param t_step: length of time step (s)
        :return:
        """
        n_steps = int(duration / t_step)
        r_site = []
        for t in range(n_steps):
            # greenwich apparent sidereal time
            # TODO Can this be made any faster??
            gst = gast(jd0 + (t*t_step)/(24*3600))

            # local siderial time of object
            lst = (gst + radians(self.lon)) % (2 * pi)

            # TODO Can this be made any faster??
            r_site.append(topo_to_eci(radians(self.lat), self.alt, lst))

        self.eci = r_site


class Spacecraft:
    def __init__(self, uid=0, isl_dist=1000000):
        self.uid = uid
        self.isl_dist = isl_dist
        self.orbit = None

    def get_orbit(self, initial, ele_type, jd0, duration, step):
        self.orbit = Orbit(jd0, initial, ele_type)
        self.orbit.propagate_orbit(duration, step)


class Orbit:
    mu = 3.986005e+14  # Earth gravitational constant
    j2 = 0.00108263  # J2 Earth oblateness
    re = 6371000.  # Earth radius - average (m)

    def __init__(
            self,
            jd0,
            initial_state,
            ele_type='coe'
    ):

        # Get the initial conditions in Classical Orbit Elements
        self.coe0 = None
        self.mee0 = None
        self.alt0 = None
        self.get_ic(initial_state, ele_type)
        self.jd0 = jd0

        self.t = None
        self.mee = None  # initialise mod eq elements at each time step
        self.coe = None  # init classical orbit elements
        self.eci = None  # init cartesian elements (ECI frame)
        self.propagated = False

    def get_ic(self, init_conditions, ele_type):
        """
        Get modified equinoctial element form of the orbit parameters
        :param init_conditions: Initial conditions (list of 6 variables)
        :param ele_type: String defining the "type" of element. "mee", "coe", "eci"
        :return:
        """
        # if already using modified equinoctial elements, just return them
        if ele_type == 'mee':
            self.mee0 = init_conditions
            self.coe0 = mee_to_coe(init_conditions)

        # If using classical elements, convert and return
        elif ele_type == 'coe':
            self.mee0 = coe_to_mee(init_conditions)
            self.coe0 = init_conditions

        # TODO add in conversions to MEE in case incoming elements cartesian, or other
        else:
            raise AttributeError(
                f'{ele_type} is not supported as a type of orbit element,'
                f' choose "mee" or "coe"'
            )

        # TODO switch this to account for oblateness
        self.alt0 = self.coe0[0] - self.re

    def propagate_orbit(self, duration, step):

        # FIXME don't like how this bind is being used, needing to pass in time to the
        #  function even though it doesn't use time is nasty
        eq = self.eq_of_mo_mee  # bind to equations of motion method
        t = np.arange(0, duration, step)  # time array used for simulation

        # Carry out numerical integration, returning a numpy array of the
        # modified equinoctial elements
        mee = odeint(eq, self.mee0, t)

        # get keplerian and cartesian results from modified equinoctial elements
        eci = [tuple(mee_to_cart(x)) for x in mee]
        coe = [tuple(mee_to_coe(x)) for x in mee]

        self.t = t
        self.mee = mee
        self.coe = np.array(coe)
        self.eci = np.array(eci)
        self.propagated = True

    @property
    def period(self):
        return 2 * pi * sqrt(self.coe0[0] ** 3 / self.mu)

    @property
    def velocity0(self):
        return sqrt(
            self.mu * (
                (2 / self.coe0[0]) - (1 / self.coe0[0])
            )
        )

    def eq_of_mo_mee(self, u, t):
        """
        Orbit equations of motion for Modified equinoctial elements
        """

        # initialise variables to input
        p = u[0]
        f = u[1]
        g = u[2]
        h = u[3]
        k = u[4]
        L = u[5]

        # calculate support parameters
        sinL = sin(L)
        cosL = cos(L)
        w = 1 + f * cosL + g * sinL
        x = sqrt(p / self.mu)
        r = p / w
        s2 = 1 + h ** 2 + k ** 2

        Dr, Dt, Dn = self.pert_j2_mee(h, k, L, r)

        # calculate rates of change in variables
        dp = ((2 * p * x) / w) * Dt
        df = x * (sinL * Dr + (((w + 1) * cosL + f) / w) * Dt - (((h * sinL - k * cosL) * g) / w) * Dn)
        dg = x * (-cosL * Dr + (((w + 1) * sinL + g) / w) * Dt + (((h * sinL - k * cosL) * f) / w) * Dn)
        dh = (x * s2 * cosL / (2 * w)) * Dn
        dk = (x * s2 * sinL / (2 * w)) * Dn
        dL = (sqrt(self.mu * p) * (w / p) ** 2) + (1 / w) * x * (h * sinL - k * cosL) * Dn

        # return rates of change variables
        return [dp, df, dg, dh, dk, dL]

    def pert_j2_mee(self, h, k, l, r):
        """ calculate perturbation forces due to J2 in MEq frame

        :param h:
        :param k:
        :param l:
        :param r:
        :return:
        """

        x = self.mu * self.j2 * self.re ** 2 / r ** 4
        y = (1 + h ** 2 + k ** 2) ** 2
        sl = sin(l)
        cl = cos(l)

        dr = -(3 / 2) * x * (1 - (12 * (h * sl - k * cl) ** 2) / y)
        dt = -12 * x * (((h * sl - k * cl) * (h * cl + k * sl)) / y)
        dn = -6 * x * (((1 - h ** 2 - k ** 2) * (h * sl - k * cl)) / y)

        return dr, dt, dn


def setup_ground_nodes(jd_start, duration, t_step, nodes, is_source=False, id_counter=0):
    nodes_dict = {}
    locations = []
    if nodes.type == "bespoke":
        for loc in nodes.locations:
            locations.append([loc.lat, loc.lon, getattr(loc, "alt", 0)])

    elif nodes.type == "group":
        if nodes.distribution == "even":
            points_scaled = generate_even_dist_on_earth(nodes.n)
            for p in points_scaled:
                lat, lon, alt = eci_to_geod(jd_start, p)
                locations.append([degrees(lat), degrees(lon), alt])

    for location in locations:
        n = GroundNode(
            id_counter,
            location[0],
            location[1],
            location[2],
            min_el=nodes.min_el,
            is_source=is_source
        )
        n.eci_coords(jd_start, duration, t_step)
        nodes_dict[n.uid] = n
        id_counter += 1

    return nodes_dict


def setup_satellites(jd_start, duration, t_step, satellites, counter=0):
    satellites_dict = {}
    for sat in satellites.orbits:
        s = Spacecraft(counter)
        ic = [
            sat.sma*1000,
            sat.ecc,
            radians(sat.inc),
            radians(sat.raan),
            radians(sat.aop),
            radians(sat.ta)
        ]
        s.get_orbit(ic, 'coe', jd_start, duration, t_step)
        satellites_dict[counter] = s
        counter += 1
    return satellites_dict
