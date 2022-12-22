__author__ = "Christopher Lowe"


# from Lib import random
from math import pi, sin, cos, tan, asin, atan2, sqrt, radians
from numpy import dot
from random import random, randint, choice
import string
import numpy as np
import time
import pickle

from src.routing import Contact


R_E = 6371000.8
MU_E = 3.986005e+14
USED_IDS = set()


# *** GENERIC DATA/MATHS FUNCTIONS ***
def geometric_cdf(p, k):
    """
    Returns the likelihood that an event will have been successful after k trials,
    given a success probability of p. I.e. the mean number of trials before success is
    1/p. E.g. a die throw will have a p of 1/6 that a number 1 is landed, such that the
    likelihood of having thrown a 1 after 3 (k) trials is 0.4213
    :param p: (float) probability of success for each trial [0:1]
    :param k: (int) number of trials
    :return: (float) Likelihood that a successful trial has been executed
    """
    return 1 - (1 - p)**k


def fibonacci_sphere(samples=1):
    """
    Return cartesian coordinates on a sphere with a uniform distribution of points
    based on the Fibonacci spiral .
    :param samples:
    :return:
    """
    points = []
    phi = pi * (3. - sqrt(5.))  # golden angle in radians

    for i in range(samples):
        y = 1 - (i / float(samples - 1)) * 2  # y goes from 1 to -1
        radius = sqrt(1 - y * y)  # radius at y

        theta = phi * i  # golden angle increment

        x = cos(theta) * radius
        z = sin(theta) * radius

        points.append((x, y, z))

    return points


# *** ORBITAL CONVERSIONS AND FUNCTIONS ***
def gsite(eci, jd, lat, lon, alt):
    """ convert from eci to topological coordinates (elevation & range relative to a ground site)

    :param eci: position vector in ECI coordinates (m)
    :param jd: julian date
    :param lat: latitude of ground site (rads)
    :param lon: longitude of ground site (rads)
    :param alt: altitude of ground site (m)
    :return elevation: elevation above horizon (radians)
    """

    # earth rotation rate
    omega = (2*pi)/(24*60*60)

    # greenwich apparent sidereal time
    gst = gast(jd)

    # local siderial time of object
    lst = (gst+lon) % (2*pi)
    r_site = topo_to_eci(lat, alt, lst)

    # eci vector from object to satellite
    rho_ijk = list()
    for i in range(0, 3):
        rho_ijk.append(eci[i] - r_site[i])

    # object to satellite slant range
    slant_dot = dot(rho_ijk, rho_ijk)
    slant = sqrt(slant_dot)

    rho_hat_ijk = list()
    for i in range(0, 3):
        rho_hat_ijk.append(rho_ijk[i]/slant)

    wxr = list()
    wxr.append(-omega * eci[1])
    wxr.append(omega * eci[0])
    wxr.append(0)

    slat = sin(lat)
    clat = cos(lat)
    slst = sin(lst)
    clst = cos(lst)

    tmatrix = [
        [slat * clst, slat * slst, -clat],
        [-slst, clst, 0],
        [clat * clst, clat * slst, slat]
    ]

    rho_hat_sez = list()
    for i in range(0, 3):
        a = 0
        for j in range(0, 3):
            a = a + tmatrix[i][j] * rho_hat_ijk[j]
        rho_hat_sez.append(a)

    elevation = asin(rho_hat_sez[2])  # elevation

    return elevation


def ecf_to_eci(r_ecf, v_ecf, gast):
    """
    Convert from Earth-centred Earth-fixed coordinates to Earth-centered Inertial
    coordinates
    :param r_ecf: Position vector in ECEF (m)
    :param v_ecf: Velocity vector in ECEF (m/s)
    :param gast: Greenwich apparent siderial time
    :return:
    """

    # earth rotation rate
    omega = (2*pi)/(24*60*60)

    tm = (
        (cos(gast), sin(gast), 0),
        (sin(gast), cos(gast), 0),
        (0, 0, 1)
    )

    tmdot = (
        (omega * sin(gast), omega * cos(gast), 0),
        (-omega * cos(gast), -omega * sin(gast), 0),
        (0, 0, 0)
    )

    tmt = []
    tmdott = []
    for i in range(3):
        tmt.append([])
        tmdott.append([])
        for j in range(3):
            tmt[i].append(tm[j][i])
            tmdott[i].append(tmdot[j][i])

    r_eci = []
    v_eci_ = []
    for i in range(3):
        s = 0
        t = 0
        for j in range(3):
            s = s + tmt[i][j] * r_ecf[j]
            t = t + tmt[i][j] * v_ecf[j]

        r_eci.append(s)
        v_eci_.append(t)

    vtmp = []
    for i in range(3):
        s = 0
        for j in range(3):
            s = s + tmdott[i][j] * r_ecf[j]
        vtmp.append(s)

    v_eci = []
    for i in range(3):
        v_eci.append(v_eci_[i] + vtmp[i])

    return r_eci, v_eci


def coe_to_mee(coe):
    """ converts from keplerian to modified equinoctial elements

    :param a: semi-major axis
    :param e: eccentricity
    :param i: inclination
    :param raan: right ascension of ascending node
    :param om: argument of perigee
    :param v: true anomaly
    :return [p, f, ...]: Modified equinoctial elements
    """

    a = coe[0]
    e = coe[1]
    i = coe[2]
    raan = coe[3]
    om = coe[4]
    v = coe[5]

    p = a * (1 - e ** 2)
    f = e * cos(om + raan)
    g = e * sin(om + raan)
    h = tan(i / 2) * cos(raan)
    k = tan(i / 2) * sin(raan)
    l = raan + om + v
    return [p, f, g, h, k, l]


def mee_to_coe(mee):
    """ convert from modified equinoctial elements to Keplerian elements
    :param p:
    :param f:
    :param g:
    :param h:
    :param k:
    :param l:
    :return [a, e, ...]: Keplerian (classical orbit) elements
    """
    p = mee[0]
    f = mee[1]
    g = mee[2]
    h = mee[3]
    k = mee[4]
    l = mee[5]

    f2 = f ** 2
    g2 = g ** 2
    h2 = h ** 2
    k2 = k ** 2
    a = p / (1 - f2 - g2)
    e = sqrt(f2 + g2)
    i = 2 * atan2(sqrt(h2 + k2), 1)
    raan = atan2(k, h)
    om = atan2(g * h - f * k, f * h + g * k)
    nu = l - atan2(g, f)

    i = i % (2 * pi)
    om = om % (2 * pi)
    raan = raan % (2 * pi)
    nu = nu % (2 * pi)

    return [a, e, i, raan, om, nu]


def mee_to_cart(mee, mu=MU_E):
    """ converts from mod. equinoctial ele. to cartesian

    :param mee
    :param mu: Grav constant
    :return [rx, ry, rz, vx, vy, vz]: position vector and velocity vectors in cartesian coordinates
    """

    p = mee[0]
    f = mee[1]
    g = mee[2]
    h = mee[3]
    k = mee[4]
    l = mee[5]

    h2 = h ** 2
    k2 = k ** 2
    al2 = h2 - k2
    s2 = 1 + h2 + k2
    cl = cos(l)
    sl = sin(l)
    w = 1 + f * cl + g * sl
    r = p / w
    a = r / s2
    b = (1 / s2) * sqrt(mu / p)

    rx = a * (cl + al2 * cl + 2 * h * k * sl)
    ry = a * (sl - al2 * sl + 2 * h * k * cl)
    rz = 2 * a * (h * sl - k * cl)

    vx = -b * (sl + al2 * sl - 2 * h * k * cl + g - 2 * f * h * k + al2 * g)
    vy = -b * (-cl + al2 * cl + 2 * h * k * sl - f + 2 * g * h * k + al2 * f)
    vz = 2 * b * (h * cl + k * sl + f * h + g * k)

    return [rx, ry, rz, vx, vy, vz]


def eci_to_geod(jdate, r_pos):
    """
    Convert from ECI position to Lat, Lon, Alt position
    :param jdate: Julian Date
    :param r_pos: ECI position vector (m)
    :return lat: Latitude (degrees)
    :return lon: Longitude (degrees)
    :return alt: Altitude (m)
    """
    # Greenwich apparent sidereal time
    gst = gast(jdate)

    r_mag = np.linalg.norm(r_pos)
    geoc_decl = asin(r_pos[2] / r_mag)
    [alt, lat] = geodet(r_mag / 1000., geoc_decl)

    x_ecf = (r_pos[0] * cos(gst)) + (r_pos[1] * sin(gst))
    y_ecf = (r_pos[1] * cos(gst)) - (r_pos[0] * sin(gst))

    lamda = atan2(y_ecf, x_ecf)

    if pi < lamda:
        lon = lamda - 2 * pi

    else:
        lon = lamda

    return lat, lon, 0


def geodet(rmag, dec):
    """
    geodetic latitude and altitude
    :param rmag: geocentric radius (kilometers)
    :param dec: geocentric declination (radians) (+north, -south; -pi/2 <= dec <= +pi/2)
    :return lat: geodetic latitude (radians) (+north, -south; -pi/2 <= lat <= +pi/2)
    :return alt: geodetic altitude (kilometers)
    """
    req = 6.3781363e+3
    flat = 1 / 298.257

    n = req / rmag
    o = flat * flat

    a = 2 * dec
    p = sin(a)
    q = cos(a)

    a = 4 * dec
    r = sin(a)
    s = cos(a)

    lat = dec + flat * n * p + o * n * r * (n - .25)
    alt = rmag + req * (flat * .5 * (1 - q) + o * (.25 * n - .0625) * (1 - s) - 1)

    return alt, lat


def gast(jdate):
    """ Greenwich apparent sidereal time

    :param jdate: julian date
    :return gst: greenwich siderial time
    """
    dtr = pi/180  # degrees to radians
    atr = dtr/3600  # arc second to radians

    # time arguments
    t = (jdate - 2451545) / 36525 # number of julian centuries since 12:00 01 Jan 2000
    t2 = t * t
    t3 = t * t2

    # fundamental trig arguments (modulo 2pi functions)
    l = (dtr * (280.4665 + 36000.7698 * t)) % (2*pi)
    lp = (dtr * (218.3165 + 481267.8813 * t)) % (2*pi)
    lraan = (dtr * (125.04452 - 1934.136261 * t)) % (2*pi)

    # nutations in longitude and obliquity
    dpsi = atr * (-17.2 * sin(lraan) - 1.32 * sin(2 * l) - 0.23 * sin(2 * lp) + 0.21 * sin(2 * lraan))
    deps = atr * (9.2 * cos(lraan) + 0.57 * cos(2 * l) + 0.1 * cos(2 * lp) - 0.09 * cos(2 * lraan))

    # mean and apparent obliquity of the ecliptic
    eps0 = (dtr * (23 + 26 / 60 + 21.448 / 3600) + atr * (-46.815 * t - 0.00059 * t2 + 0.001813 * t3)) % (2*pi)
    obliq = eps0 + deps

    # greenwich mean and apparent sidereal time
    gstm = (dtr * (280.46061837 + 360.98564736629 * (jdate - 2451545) + 0.000387933 * t2 - t3 / 38710000)) % (2*pi)
    gst = (gstm + dpsi * cos(obliq)) % (2*pi)

    return gst


def topo_to_eci(lat, alt, lst):
    """ ground site position vector (ECI) from topological components

    :param lat: latitude of object on earth (radians [-pi/2,pi/2])
    :param alt: altitude of object on earth (meters above sea level)
    :param lst: local sidereal time (radians [0,2*pi])
    :return rsiteX: ground site position vector in ECI coordinates (X-component)
    :return rsiteY: ground site position vector in ECI coordinates (Y-component)
    :return rsiteZ: ground site position vector in ECI coordinates (Z-component)
    """

    rEq = 6371000.  # earth equatorial radius
    flat = 1/298.257  # earth flatenning parameter
    slat = sin(lat)
    clat = cos(lat)
    slst = sin(lst)
    clst = cos(lst)

    # compute geodetic constants
    b = sqrt(1 - (2 * flat - flat * flat) * slat * slat)
    c = rEq / b + 0.001 * alt
    d = rEq * (1 - flat) * (1 - flat) / b + 0.001 * alt

    # compute x, y & z components of position vector
    rsiteX = c * clat * clst
    rsiteY = c * clat * slst
    rsiteZ = d * slat

    return [rsiteX,
            rsiteY,
            rsiteZ]


def slant_range(sma, min_el):
    """
    Return the maximum range at which a satellite at a particular semi-major axis can
    communicate with a gateway, given a certain minimum elevation angle
    :param sma: Semi major axis (m)
    :param min_el: Minimum elevation (radians)
    :return:
    """
    re = 6371e3
    s_rho = re / sma
    cmin_el = cos(min_el)
    s_lam = sin(pi / 2 - min_el - asin(cmin_el * s_rho))
    return re * s_lam / (cmin_el * s_rho)


def earth_rotation(tspan, t_steps, yout):
    om = 2 * np.pi / 86164.1  # rate of Earth rotation (rad / s)
    th = om * (tspan[1] - tspan[0]) / t_steps  # Earth rotation during single step (radians)

    for kw, val in yout.items():
        y_rot = []
        k = 0
        for v in val.T:  # for each location
            x_pos = v[0] * cos(k * -th) - v[1] * sin(k * -th)
            y_pos = v[0] * sin(k * -th) + v[1] * cos(k * -th)
            z_pos = v[2]

            y_rot.append([x_pos, y_pos, z_pos])
            k += 1

        yout[kw] = np.array(y_rot).T
    return yout


# *** TRAJECTORY FUNCTIONS ***
def walker_topology(t, p, f=0, con_type='delta', raan0=0., ta0=0.):
    """
    Get the relative separations (Right Ascension and True Anomaly) of satellites in a Walker constellations
    REF: https://en.wikipedia.org/wiki/Satellite_constellation#Walker_Constellation
    :param t: Total number of satellites
    :param p: Number of planes
    :param f: Phase of the true anomaly separation between adjacent planes. Difference in true anomaly between
              "equivalent" satellites in adjacent planes (in degrees) is = f*360/t
    :param con_type: String to indicate if a "delta" or "star" constellation is required
    :param raan0: Right ascension of ascending node for "satellite number 1" (degrees)
    :param ta0: True anomaly for "satellite number 1" (degrees)
    :return raan: list of Right ascension of ascending node values (radians), one entry for each satellite
    :return ta: list of True Anomaly values (radians), one entry for each satellite
    """
    s = int(t / p)  # satellites per plane
    pu = 2 * pi / t  # pattern unit
    raan = []
    ta = []

    for j in range(p):  # for each plane
        # raan.append([])
        # ta.append([])
        for k in range(s):  # for each satellite in plane j
            if con_type == 'star':  # if a "star" constellation is required
                if isinstance(raan0, list):
                    raan.append(radians(raan0[j]) % (2 * pi))
                else:
                    # right ascension of ascending node
                    raan.append((radians(raan0) + (s * pu * j / 2)) % (2 * pi))
            else:  # if a "delta" constellation is required
                if isinstance(raan0, list):
                    raan.append(radians(raan0[j]) % (2 * pi))
                else:
                    raan.append((radians(raan0) + (s * pu * j)) % (2 * pi))  # right
                # ascension of ascending node

            ta.append((radians(ta0) + (p * pu * k + f * pu * j)) % (2 * pi))  # true
            # anomaly

    return raan, ta


def random_topology(n, sma_range, inc_range):
    sma = []
    inc = []
    raan = []
    ta = []

    for x in range(n):
        sma_ = sma_range[0] + random() * (sma_range[1] - sma_range[0])
        sma.append(sma_)
        inc_ = inc_range[0] + random() * (inc_range[1] - inc_range[0])
        inc.append(inc_)

        raan.append(random() * 2 * pi)
        ta.append(random() * 2 * pi)

    return sma, inc, raan, ta


# *** TIME-RELATED FUNCTIONS ***
def greg2jd(month, day, year):
    """ convert from gregorian calendar to julian date

    :param month:
    :param day:
    :param year:
    :return jdn: julian day number
    """
    y = year
    m = month
    b = 0
    c = 0

    if m <= 2:
        y = y - 1
        m = m + 12

    if y < 0:
        c = -.75

    # check for valid calendar date (5th - 14th Oct 1582 not a valid period)
    if year < 1582:
        pass
    elif year > 1582:
        a = np.floor(y / 100)
        b = 2 - a + np.floor(a / 4)
    elif month < 10:
        pass
    elif month > 10:
        a = np.floor(y / 100)
        b = 2 - a + np.floor(a / 4)
    elif day <= 4:
        pass
    elif day > 14:
        a = np.floor(y / 100)
        b = 2 - a + np.floor(a / 4)
    else:
        print('dates specific within 5th - 14th Oct 1582, which is not a valid date for JD conversion')
        quit() # exit simulation

    jd = np.floor(365.25 * y + c) + np.floor(30.6001 * (m + 1))
    jdn = jd + day + b + 1720994.5

    return jdn


def generate_even_dist_on_earth(n):
    points_xyz = fibonacci_sphere(n)
    return [[x * R_E for x in y] for y in points_xyz]


def id_generator(size=12, chars=string.ascii_uppercase + string.digits):
    id = ''.join(choice(chars) for _ in range(size))
    if id in USED_IDS:
        id_generator(size, chars)
    USED_IDS.add(id)
    return id


def cp_load(file_name, max_contacts=None):
    __contact_plan = []
    nodes = set()
    with open(file_name, 'r') as cf:
        for contact in cf.readlines():
            if contact[0] == '#':
                continue
            if not contact.startswith('a contact'):
                continue

            fields = contact.split(' ')[2:]  # ignore "a contact"
            start, end, frm, to, rate, owlt = map(int, fields)
            nodes.add(frm)
            nodes.add(to)
            __contact_plan.append(
                Contact(start=start, end=end, frm=frm, to=to, rate=rate, owlt=owlt))
            if len(__contact_plan) == max_contacts:
                break

    print('Load contact plan: %s contacts were read.' % len(__contact_plan))
    print(__contact_plan)
    return __contact_plan


def ground_nodes_load(file_name):
    nodes = []
    with open(file_name, "r") as nodes_from_file:
        for node in nodes_from_file.readlines():
            node_items = node.split(" ")
            nodes.append(
                {
                    "name": node_items[0],
                    "lat": float(node_items[1]),
                    "lon": float(node_items[2])
                }
            )
    return nodes
