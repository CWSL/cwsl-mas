"""
Authors: Tim Bedin, Tim Erwin

Copyright 2014 CSIRO

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Tests for the Constraint class.

"""

import unittest

from cwsl.core.constraint import Constraint


class TestConstraint(unittest.TestCase):

    def setUp(self):

        self.tas_constraint = Constraint('variable', ['tas'])
        self.model_constraint = Constraint('model', ['ACCESS1-0', 'BNU-ESM'])

    def testEquality(self):
        """ Test that two constraints compare as equal. """

        constraint_1 = Constraint('model', ['ACCESS1-0'])
        constraint_2 = Constraint('model', ['ACCESS1-0'])

        constraint_3 = Constraint('variable', ['tas', 'pr'])
        constraint_4 = Constraint('variable', ['pr', 'tas'])

        self.assertEqual(constraint_1, constraint_2)
        self.assertEqual(constraint_3, constraint_4)

    def testEquality_2(self):
        """ Testing equality in a set as this seems to be a problem."""

        constraint_set_1 = set([Constraint('model', ['MIROC4h',
                                                     'ACCESS1-3',
                                                     'MIROC-ESM-CHEM']),
                                    Constraint('experiment', ['historical']),
                                    Constraint('agg_type', ['clim']),
                                    Constraint('variable', ['tas']),
                                    Constraint('grid', ['native']),
                                    Constraint('start', ['1986']),
                                    Constraint('end', ['2005']),
                                    Constraint('realization', ['1']),
                                    Constraint('initialization', ['1']),
                                    Constraint('perturbed_physics', ['1']),
                                    Constraint('modeling_realm', ['atmos']),
                                    Constraint('mip_table', ['Amon']),
                                    Constraint('activity', ['CMIP5']),
                                    Constraint('frequency', ['seas']),
                                    Constraint('institute', ['MIROC'])])

        constraint_set_2 = set([Constraint('model', ['MIROC4h',
                                                     'ACCESS1-3',
                                                     'MIROC-ESM-CHEM']),
                                    Constraint('experiment', ['historical']),
                                    Constraint('agg_type', ['clim']),
                                    Constraint('variable', ['tas']),
                                    Constraint('grid', ['native']),
                                    Constraint('start', ['1986']),
                                    Constraint('end', ['2005']),
                                    Constraint('realization', ['1']),
                                    Constraint('initialization', ['1']),
                                    Constraint('perturbed_physics', ['1']),
                                    Constraint('modeling_realm', ['atmos']),
                                    Constraint('mip_table', ['Amon']),
                                    Constraint('activity', ['CMIP5']),
                                    Constraint('frequency', ['seas']),
                                    Constraint('institute', ['MIROC'])])

        for constraint in constraint_set_1:
            self.assertTrue(constraint in constraint_set_2)

    def test_getter(self):
        """ Test getting the value of a constraint key. """

        self.assertEqual(
            self.tas_constraint.key,
            'variable',
            '''Constraint key not set correctly - should be {0}
               but equals {1}'''.format('variable', self.model_constraint.key))

        self.assertItemsEqual(
            self.tas_constraint.values,
            ['tas'],
            '''Constraint values not set correctly - should be {0}
               but equals {1}'''.format(['tas'], self.tas_constraint.values))

        self.assertEqual(self.model_constraint.key,
            'model',
            '''Constraint key not correctly set - should be {0} but
               equals {1}'''.format('variable', self.model_constraint.key))

        self.assertItemsEqual(
            self.model_constraint.values,
            ['ACCESS1-0', 'BNU-ESM'],
            '''Constraint values not set correctly - should be {0}
               but equals {1}'''.format(['ACCESS1-0', 'BNU-ESM'],
                                       self.tas_constraint.values))

    def test_setter(self):
        """ Test setting the value of a constraint key. """

        self.tas_constraint.values = ['pr']
        self.assertItemsEqual(
            self.tas_constraint.values,
            ['pr'],
            '''Setter method not setting values correctly''')

    def test_constraint_set(self):
        """ Test to make sure that putting constraint objects into a set works correctly."""

        some_constraints = set([Constraint('model', ['ACCESS1-3']),
                                Constraint('experiment', ['historical']),
                                Constraint('agg_type', ['clim']),
                                Constraint('variable', ['tas']),
                                Constraint('grid', ['native']),
                                Constraint('start', ['1986']),
                                Constraint('end', ['2005']),
                                Constraint('realization', ['1']),
                                Constraint('initialization', ['1']),
                                Constraint('perturbed_physics', ['1']),
                                Constraint('modeling_realm', ['atmos']),
                                Constraint('mip_table', ['Amon']),
                                Constraint('activity', ['cmip5']),
                                Constraint('frequency', ['seas']),
                                Constraint('institute', ['CSIRO-BOM'])])

        unsorted_list = list(some_constraints)
        sorted_list = sorted(unsorted_list)

        #print("Unsorted = {0}\nSorted = {1}".format(unsorted_list, sorted_list))

    def test_hash_and_equality(self):
        """ Test to see that constraint objects have the same hash and
        compare as equal properly. """

        some_constraints_1 = [Constraint('model', ['ACCESS1-3']),
                              Constraint('institute', ['CSIRO-BOM']),
                              Constraint('experiment', ['historical']),
                              Constraint('agg_type', ['clim']),
                              Constraint('grid', ['native']),
                              Constraint('start', ['1986']),
                              Constraint('end', ['2005']),
                              Constraint('realization', ['1']),
                              Constraint('initialization', ['1']),
                              Constraint('variable', ['tas']),
                              Constraint('perturbed_physics', ['1']),
                              Constraint('modeling_realm', ['atmos']),
                              Constraint('mip_table', ['Amon']),
                              Constraint('activity', ['cmip5']),
                              Constraint('frequency', ['seas'])]

        some_constraints_2 = [Constraint('model', ['ACCESS1-3']),
                              Constraint('institute', ['CSIRO-BOM']),
                              Constraint('experiment', ['historical']),
                              Constraint('agg_type', ['clim']),
                              Constraint('grid', ['native']),
                              Constraint('start', ['1986']),
                              Constraint('end', ['2005']),
                              Constraint('realization', ['1']),
                              Constraint('initialization', ['1']),
                              Constraint('variable', ['tas']),
                              Constraint('perturbed_physics', ['1']),
                              Constraint('modeling_realm', ['atmos']),
                              Constraint('mip_table', ['Amon']),
                              Constraint('activity', ['cmip5']),
                              Constraint('frequency', ['seas'])]

        set1 = set(some_constraints_1)
        set2 = set(some_constraints_2)

        self.assertEqual(set1, set2)

        for i in xrange(len(some_constraints_1)):
            self.assertEqual(some_constraints_1[i], some_constraints_2[i])

            self.assertEqual(hash(some_constraints_1[i]),
                             hash(some_constraints_2[i]))

            self.assertEqual(repr(some_constraints_1[i]),
                             repr(some_constraints_2[i]))

    def test_iteration(self):
        """ Test that you can iterate through values in a Constraint. """
        new_constraint = Constraint('things', ['this', 'that', 'something_else'])

        out_vals = [out for out in new_constraint]
        expected_outs = [('things', 'this'),
                         ('things', 'that'),
                         ('things', 'something_else')]

        self.assertItemsEqual(expected_outs, out_vals)
