#!/usr/bin/env python3

import json
from types import SimpleNamespace
import numpy as np
import pickle

from main import main
import misc as _misc


def save_data(data, scheduling_scheme, rsl, base_file):
	fn = f"{scheduling_scheme}_{round(rsl, 1)}"
	with open(f"{base_file}_{fn}", "wb") as file:
		pickle.dump(data, file)


# [valid_pickup, define_pickup, valid_delivery, resource_aware, define_delivery]
schemes = {
	# "naive":              [False, False, False, False, False],
	# "first":              [True,  True,  False, False, False],
	# "cgs_cgr":            [True,  True,  True,  False, False],
	"cgs_cgr_resource":   [True,  True,  True,  True,  False],
	"cgs_msr":            [True,  True,  True,  True,  True],
}

# uncertainties = [1.0, 0.9, 0.8, 0.7]
uncertainties = [0.7]

# congestions = [.1, .2, .3, .4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.]
# congestions = np.linspace(0.1, 0.9, 3)
congestions = [0.1, 1.0]

# Scheduling capabilities can either be:
#   0. Centralised (normal) - No local rescheduling
#   1. Decentralised (pre-acquire only) - Can reschedule tasks before they're acquired
#   2. Decentralised (any) - Can reschedule tasks at any time (if feasible)
schedulers = [0, 1, 2]

filename = "input_files//walker_delta_16.json"
results_file_base = "results//decentral//results"
with open(filename, "rb") as read_content:
	inputs = json.load(read_content, object_hook=lambda d: SimpleNamespace(**d))

for con in congestions:
	inputs.traffic.congestion = con
	for scheme_name, scheme in schemes.items():
		inputs.traffic.msr = True if scheme[4] else False
		for uncertainty in uncertainties:
			for scheduler in schedulers:
				_misc.USED_IDS = set()
				analytics = main(inputs, scheme, uncertainty, scheduler)
				save_data(analytics, scheme_name, con, results_file_base)
