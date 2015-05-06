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

Contains the Constraint class.

"""

from copy import deepcopy


class Constraint(object):
    '''
    A Constraint represents a single restriction on a DataSet object.

    A DataSet owns a set of Constraints.

    Each Constraint has a key and a set with zero or more values.
    new_constraint.key = 'variable' new_constraint.values = set(['tas','pr'])
    or,
    new_constraint.key = 'model' new_constraint.values = set(['ACCESS1-0'])
    or,
    new_constraint.key = 'perturbed_physics_number' new_constraint.values = set([])


    '''

    def __init__(self, key, values):
        '''
        @param key: The element of the DataSet that we wish to constrain.
        @type key: string

        @param values: A list of values that the DataSet's attributes will
                       be constrained to.
        @type values: list

        '''

        # The unicode stuff is to deal with some funny equality/hash problems.
        # it may not actually be necessary.
        self.key = str(key)

        str_vals = [str(val) for val in values]
        self.values = set(str_vals)

    def __repr__(self):
        # The values set needs to be transformed to a sorted list
        # so that the string representation is the same for equal
        # sets.
        list_vals = list(self.values)

        sorted_list = sorted(list_vals)

        return "Constraint object: key = {0}, values = {1}".\
            format(self.key, sorted_list)

    def __hash__(self):
        # The class needs to be hashable in order to work
        # inside a set.
        return(hash(repr(self)))

    def __eq__(self, other):
        # Constraints are equal if their hashes are the same.
        return hash(self) == hash(other)

    def __iter__(self):
        self.values_iter = iter(self.values)
        return self

    def next(self):
        current_val = self.values_iter.next()

        return self.key, current_val
    
    @staticmethod
    def remove_constraints(cons_name_list, cons_set):
        cons_copy = deepcopy(cons_set)
        
        cons_names = [cons.key for cons in cons_set]
        
        to_remove = []
        
        for cons_name in cons_name_list:
            if cons_name not in cons_names:
                raise ConstraintNotFoundError
        
        for constraint in cons_set:
            if constraint.key in cons_name_list:
                to_remove.append(constraint)
                
        for removed in to_remove:
            cons_copy.remove(removed)
            
        return cons_copy
        
        
        
class ConstraintNotFoundError(Exception):
    ''' Raised if you try and remove a Constraint that doesn't exist. '''
    pass

