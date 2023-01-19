import pickle
import matplotlib.pyplot as plt
import numpy as np
import itertools
from statistics import mean, stdev
# import seaborn as sns
# sns.color_palette("pastel")

from misc import my_ceil


def get_fraction_of_first_pickups(results, first_pickup_ids):
	# This is the number of bundle acquisitions that differ from the "first" scheme
	first_pickups_counter = 0
	pickup_ids = {
		b.task.requests[0].uid: b.created_at
		for b in results.get_bundles_delivered_in_active_period()
	}
	for req, time_ in first_pickup_ids.items():
		if req in pickup_ids and time_ == pickup_ids[req]:
			first_pickups_counter += 1
	return first_pickups_counter / len(pickup_ids)


def plot_performance_metrics(schemes, uncertainties, congestions, metrics):
	# plt.rcParams['axes.grid'] = True
	plt.style.use('_mpl-gallery')
	fig, ax = plt.subplots(3, 3)
	plt.subplots_adjust(left=0.05, bottom=0.07, right=0.96, top=0.93)

	for scheme, props in schemes.items():
		for uncertainty, prop_un in uncertainties.items():
			for metric in metrics:
				ax[metric["row"], metric["col"]].plot(
					congestions,
					metric[scheme][uncertainty],
					linewidth=2,
					color=props["colour"],
					linestyle=prop_un["linestyle"]
				)

				# Pick-up latency, from Request to Pickup
				ax[metric["row"], metric["col"]].set(
					xlim=(0, 2), xticks=np.arange(0, 2.5, 0.5),
					ylim=(0, metric["max"]),
					yticks=np.arange(0, metric["max"] + metric["tick"], metric["tick"])
				)

				ax[metric["row"], metric["col"]].set_ylabel(metric["y_label"])

	ax[2, 0].set_xlabel("Congestion")
	ax[2, 1].set_xlabel("Congestion")
	ax[2, 2].set_xlabel("Congestion")

	# Add a legend at the top of the figure
	fig.legend(
		ax[0, 0].lines,
		# ["Naive", "First", "CGS (PU)", "CGS (CGR)", "CGS (MSR)"],
		["Reliability = 0.7", "Reliability = 0.8", "Reliability = 0.9", "Reliability = 1.0"],
		loc='upper center',
		# ncol=len(schemes),
		ncol=len(uncertainties)
	)

	# for a in ax.flat:
	# 	a.label_outer()

	plt.show()


def plot_first_pickups(schemes, first_pickups, congestions):
	plt.style.use('_mpl-gallery')
	fig, ax = plt.subplots(1, 1)

	for scheme, props in schemes.items():
		ax.plot(
			congestions,
			first_pickups[scheme][1.0],
			linewidth=2,
			color=props["colour"],
		)

		ax.set(
			xlim=(0, 2), xticks=np.arange(0, 2.5, 0.5),
			ylim=(0, 1),
			yticks=np.arange(0, 1.1, .1)
		)

		ax.set_ylabel("Fraction of first pickups")

	ax.set_xlabel("Congestion")
	plt.subplots_adjust(left=0.11, bottom=0.1, right=0.95, top=0.89)

	# Add a legend at the top of the figure
	fig.legend(
		ax.lines,
		["Naive", "First", "CGS (PU)", "CGS (CGR)", "CGS (MSR)"],
		# ["Uncertainty = 0.7", "Uncertainty = 0.8", "Uncertainty = 0.9",
		#  "Uncertainty = 1.0"],
		loc='upper center',
		ncol=len(schemes),
		# ncol=len(uncertainties)
	)

	# for a in ax.flat:
	# 	a.label_outer()

	plt.show()


filename_base = "results//nominal//results"
# filename_base = "results//uncertainty//results"
congestions = [round(x, 1) for x in np.linspace(0.1, 0.9, 9)]
congestions.extend([round(x, 1) for x in np.linspace(1.0, 2.0, 6)])
# congestions = [0.1]

schemes = {
	"naive": {"colour": "black"},
	"first": {"colour": "blue"},
	"cgs_cgr": {"colour": "red"},
	"cgs_cgr_resource": {"colour": "green"},
	"cgs_msr": {"colour": "orange"}
}

uncertainties = {
	# 0.7: {"linestyle": "dotted"},
	# 0.8: {"linestyle": "dashdot"},
	# 0.9: {"linestyle": "dashed"},
	1.0: {"linestyle": "solid"}
}

request_latency = {
	"row": 0, "col": 0, "y_label": "Total latency (hrs)", "max": 1, "tick": 1}
task_latency = {
	"row": 0, "col": 1, "y_label": "Pickup latency (hrs)", "max": 1, "tick": 1}
bundle_latency = {
	"row": 0, "col": 2, "y_label": "Delivery latency (hrs)", "max": 1, "tick": 1}

request_ratio = {
	"row": 1, "col": 0, "y_label": "Request ratio", "max": 1, "tick": 0.2}
task_ratio = {
	"row": 1, "col": 1, "y_label": "Pickup ratio", "max": 1, "tick": 0.2}
# delivery_ratio = {
# 	"row": 1, "col": 2, "y_label": "Delivery ratio", "max": 1, "tick": 0.2}
hop_count = {
	"row": 1, "col": 2, "y_label": "Hop count", "max": 1, "tick": 0.5}

requests_accepted = {
	"row": 2, "col": 0, "y_label": "Req. accepted, x1000", "max": 1, "tick": 1}
# requests_rejected = {
# 	"row": 2, "col": 1, "y_label": "Req. rejected, x1000", "max": 1, "tick": 0.5}
requests_failed = {
	"row": 2,  "col": 1, "y_label": "Req. failed, x1000", "max": 1, "tick": 1}
requests_delivered = {
	"row": 2, "col": 2, "y_label": "Req. delivered, x1000", "max": 1, "tick": 1}

first_pu_frac = {
	scheme: {
		uncertainty: [] for uncertainty in uncertainties
	} for scheme in schemes
}

metrics = [
	request_latency,
	task_latency,
	bundle_latency,
	request_ratio,
	task_ratio,
	# delivery_ratio,
	hop_count,
	requests_accepted,
	# requests_rejected,
	requests_failed,
	requests_delivered,
]

for metric in metrics:
	for scheme in schemes:
		metric[scheme] = {}
		for uncertainty in uncertainties:
			metric[scheme][uncertainty] = []

first_pickup_ids = {}
for con in congestions:
	# filename = f"{filename_base}_{scheme}_{uncertainty}_{con}"
	filename = f"{filename_base}_first_{con}"
	first_results = pickle.load(open(filename, "rb"))

	# Dict showing the time at which the pickup occurred (value) for each request (key)
	first_pickup_ids[con] = {
		b.task.requests[0].uid: b.created_at
		for b in first_results.get_bundles_delivered_in_active_period()
	}

# for scheme, con in itertools.product(schemes, congestions):
for scheme, uncertainty, con in itertools.product(schemes, uncertainties, congestions):
	# filename = f"{filename_base}_{scheme}_{uncertainty}_{con}"
	filename = f"{filename_base}_{scheme}_{con}"
	results = pickle.load(open(filename, "rb"))

	request_latency[scheme][uncertainty].append(mean(results.request_latencies) / 3600)
	task_latency[scheme][uncertainty].append(mean(results.pickup_latencies_delivered) / 3600)
	bundle_latency[scheme][uncertainty].append(mean(results.delivery_latencies) / 3600)

	request_ratio[scheme][uncertainty].append(results.request_delivery_ratio)
	task_ratio[scheme][uncertainty].append(results.task_delivery_ratio)
	# delivery_ratio[scheme][uncertainty].append(results.bundle_delivery_ratio)
	hop_count[scheme][uncertainty].append(results.hop_count_average_delivered)

	requests_accepted[scheme][uncertainty].append(results.tasks_processed_count / 1000)
	# requests_rejected[scheme][uncertainty].append(results.requests_rejected_count / 1000)
	requests_failed[scheme][uncertainty].append(results.requests_failed_count / 1000)
	requests_delivered[scheme][uncertainty].append(results.requests_delivered_count / 1000)

	request_latency["max"] = max(request_latency["max"], request_latency[scheme][uncertainty][-1])
	task_latency["max"] = max(task_latency["max"], task_latency[scheme][uncertainty][-1])
	bundle_latency["max"] = max(bundle_latency["max"], bundle_latency[scheme][uncertainty][-1])

	request_ratio["max"] = max(request_ratio["max"], request_ratio[scheme][uncertainty][-1])
	task_ratio["max"] = max(task_ratio["max"], task_ratio[scheme][uncertainty][-1])
	# delivery_ratio["max"] = max(delivery_ratio["max"], delivery_ratio[scheme][uncertainty][-1])
	hop_count["max"] = max(hop_count["max"], hop_count[scheme][uncertainty][-1])

	requests_accepted["max"] = max(requests_accepted["max"], requests_accepted[scheme][uncertainty][-1])
	# requests_rejected["max"] = max(requests_rejected["max"], requests_rejected[scheme][uncertainty][-1])
	requests_failed["max"] = max(requests_failed["max"], requests_failed[scheme][uncertainty][-1])
	requests_delivered["max"] = max(requests_delivered["max"], requests_delivered[scheme][uncertainty][-1])

	first_pu_frac[scheme][uncertainty].append(
		get_fraction_of_first_pickups(results, first_pickup_ids[con])
	)
#
# plot_performance_metrics(schemes, uncertainties, congestions, metrics)
plot_first_pickups(schemes, first_pu_frac, congestions)
