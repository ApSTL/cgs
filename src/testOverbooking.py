import sys
import unittest

from misc import cp_load
from node import Node
from bundles import Bundle
from routing import cgr_yens


class TestContactOverbooking(unittest.TestCase):
	def setUp(self) -> None:
		contact_plan = cp_load('contact_plans/test_overbooking.txt', 5000)
		self.node1 = Node(1, contact_plan=contact_plan)
		for n in [2, 3]:
			self.node1.route_table[n] = cgr_yens(1, n, contact_plan, 0, sys.maxsize)
			self.node1.outbound_queue[n] = []

		self.bundle_lp1 = Bundle(1, 3, size=1, priority=0, created_at=0)
		self.bundle_lp2 = Bundle(1, 3, size=1, priority=0, created_at=1)
		self.bundle_mp1 = Bundle(1, 3, size=2, priority=1, created_at=2)
		self.bundle_hp1 = Bundle(1, 3, size=3, priority=2, created_at=3)

	def test_overbooking_from_medium_and_high_priority_bundles(self):
		# First, assign the two low-priority bundles, which should get allocated the
		# route 1->2->3
		self.node1.buffer.append(self.bundle_lp1)
		self.node1.buffer.append(self.bundle_lp2)
		self.node1._bundle_assignment(1)
		self.assertIn(self.bundle_lp1, self.node1.outbound_queue[2])
		self.assertIn(self.bundle_lp2, self.node1.outbound_queue[2])

		# Next, assign the medium priority bundle, which should result in overbooking
		# of the contact 1->2 and 2->3, and re-routing of the LP bundles to the direct
		# contact 1->3
		self.node1.buffer.append(self.bundle_mp1)
		self.node1._bundle_assignment(2)
		self.assertIn(self.bundle_lp1, self.node1.outbound_queue[3])
		self.assertIn(self.bundle_lp2, self.node1.outbound_queue[3])
		self.assertIn(self.bundle_mp1, self.node1.outbound_queue[2])

		# finally, assign the high priority (large) bundle, which should be assigned to
		# the large direct contact, but cause one of the LP bundles to be dropped. It
		# should be the newer LP bundle (created at t=1) that's dropped, since that is
		# effectively the lowest.
		self.node1.buffer.append(self.bundle_hp1)
		self.node1._bundle_assignment(3)
		self.assertIn(self.bundle_lp1, self.node1.outbound_queue[3])
		self.assertIn(self.bundle_mp1, self.node1.outbound_queue[2])
		self.assertIn(self.bundle_hp1, self.node1.outbound_queue[3])

		# In terms of the MAV on each contact:
		#   1->2 began with vol = 2
		#   2->3 began with vol = 3
		#   1->3 began with vol = 4
		# Therefore, we should end up with:
		#   1->2 MAV = [0, 0, 2]
		#   2->3 MAV = [1, 1, 3]
		#   1->3 MAV = [0, 1, 1]
		self.assertEqual(self.node1.contact_plan[0].mav, [0, 0, 2])
		self.assertEqual(self.node1.contact_plan[1].mav, [1, 1, 3])
		self.assertEqual(self.node1.contact_plan[2].mav, [0, 1, 1])


if __name__ == '__main__':
	unittest.main()
