"""
Plots that are specific to decentralised routing comparison. This includes the number
of tasks that were rescheduled pre-acquisition, post-acquisition and in total.

In theory, the act of rescheduling should increase the probability that a request is
fulfilled through successful bundle pick-up and delivery. This would, however, increase
traffic levels and, therefore, may increase average latency performance. Indeed,
because full network resource information isn't available at the remote nodes carrying
out the rescheduling, it may be the case that they're rescheduling along infeasible
routes, which subsequently get dropped.
"""
import pickle
import itertools

import plotResults


filename_base = "results//decentral//results"
# rsls = [round(x, 1) for x in np.linspace(0.1, 0.9, 9)]
# rsls.extend([round(x, 1) for x in np.linspace(1.0, 2.0, 6)])
rsls = [.1, .5, 1.0, 1.5, 2.0]

schemes = {
	# "naive": {"colour": "black"},
	# "first": {"colour": "blue"},
	# "cgs_cgr": {"colour": "red"},
	"cgs_cgr_resource": {"colour": "green"},
	"cgs_msr": {"colour": "orange"}
}

uncertainties = {
	0.7: {"linestyle": "dotted"},
	# 0.8: {"linestyle": "dashdot"},
	# 0.9: {"linestyle": "dashed"},
	1.0: {"linestyle": "solid"}
}

centralisations = {
	"central": {"marker": '.'},
	"decentral": {"marker": 'x'}
}

reschedule_count_total = {
	"row": 0, "col": 0, "y_label": "No. tasks rescheduled (all)", "max": 1, "tick": 1}
reschedule_count_pre = {
	"row": 0, "col": 1, "y_label": "No. tasks rescheduled (pre-pickup)", "max": 1, "tick": 1}
reschedule_count_post = {
	"row": 0, "col": 2, "y_label": "No. tasks rescheduled (post-pickup)", "max": 1, "tick": 1}

metrics = [reschedule_count_total, reschedule_count_pre, reschedule_count_post]
for metric in metrics:
	for scheme in schemes:
		metric[scheme] = {}
		for uncertainty in uncertainties:
			metric[scheme][uncertainty] = {}
			for scheduler in centralisations:
				metric[scheme][uncertainty][scheduler] = []

for scheme, uncertainty, scheduler, rsl in itertools.product(
		schemes, uncertainties, centralisations, rsls):
	# filename = f"{filename_base}_{scheme}_{uncertainty}_{scheduler}_{rsl}"
	# filename = f"{filename_base}_{scheme}_{uncertainty}_{rsl}"
	filename = f"{filename_base}_{scheme}_{uncertainty}_{scheduler}_{rsl}"
	results = pickle.load(open(filename, "rb"))
	reschedule_count_total[scheme][uncertainty][scheduler].append(
		results.tasks_rescheduled_count)
	reschedule_count_pre[scheme][uncertainty][scheduler].append(
		results.tasks_rescheduled_pre_pickup_count)
	reschedule_count_post[scheme][uncertainty][scheduler].append(
		results.tasks_rescheduled_post_pickup_count)

	reschedule_count_total["max"] = max(
		reschedule_count_total["max"],
		reschedule_count_total[scheme][uncertainty][scheduler][-1]
	)
	reschedule_count_pre["max"] = max(
		reschedule_count_pre["max"],
		reschedule_count_pre[scheme][uncertainty][scheduler][-1]
	)
	reschedule_count_post["max"] = max(
		reschedule_count_post["max"],
		reschedule_count_post[scheme][uncertainty][scheduler][-1]
	)

legend = [
	"CGR @ 0.7 (central)",
	"CGR @ 0.7 (decentral)",
	"CGR @ 1.0 (central)",
	"CGR @ 1.0 (decentral)",
	"CGS @ 0.7 (central)",
	"CGS @ 0.7 (decentral)",
	"CGS @ 1.0 (central)",
	"CGS @ 1.0 (decentral)"
]

plotResults.plot_performance_metrics(
	schemes, uncertainties, centralisations, rsls, metrics, (1, 3), legend)
