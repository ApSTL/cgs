#!/usr/bin/env python3

import json
from types import SimpleNamespace
import pickle

import main as _main
import misc as _misc

# Scheduling schemes, for which the value list items correspond to:
#   [valid_pickup, define_pickup, valid_delivery, resource_aware, define_delivery]
# TODO: comment out schemes that should not be included in the analysis
schemes = {
	"naive": [False, False, False, False, False],
	"first": [True,  True,  False, False, False],
	"cgs_pu": [True,  True,  True,  False, False],
	"cgs_cgr": [True,  True,  True,  True,  False],
	"cgs_msr": [True,  True,  True,  True,  True],
}

# TODO: comment out uncertainty values that should not be considered
uncertainties = [
	1.0,
	# 0.9,
	# 0.8,
	# 0.7,
	# 0.6,
	# 0.5,
	# 0.4,
	# 0.3,
	# 0.2,
	# 0.1
]

# Request Submission Load values to be evaluated
congestions = [
	1.0,
	# 0.9,
	# 0.8,
	# 0.7,
	# 0.6,
	0.5,
	# 0.4,
	# 0.3,
	# 0.2,
	0.1
]


filename = "input_files//walker_delta_16.json"
results_file_base = "results//multi//results"
with open(filename, "rb") as read_content:
	inputs = json.load(read_content, object_hook=lambda d: SimpleNamespace(**d))

for con in congestions:
	for scheme_name, scheme in schemes.items():
		for uncertainty in uncertainties:

			# Reset the unique IDs used during the previous simulation
			_misc.USED_IDS = set()

			# Set the Request Submission Load (congestion) in the inputs object
			inputs.traffic.congestion = con

			# Set the use of Moderate Source Routing to True if defined in the scheme
			inputs.traffic.msr = True if scheme[4] else False

			# Execute the main simulation function
			analytics = _main.main(inputs, scheme, uncertainty)

			# Save the results to a pickle file to be evaluated later
			filename = f"{scheme_name}_{uncertainty}_{round(con, 1)}"
			with open(f"{results_file_base}_{filename}", "wb") as file:
				pickle.dump(analytics, file)
