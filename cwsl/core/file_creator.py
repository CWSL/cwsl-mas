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

Contains the FileCreator class.

"""

import os
import re
import itertools
import logging

from cwsl.core.dataset import DataSet
from cwsl.core.constraint import Constraint
from cwsl.core.metafile import MetaFile


module_logger = logging.getLogger('cwsl.core.file_creator')


class FileCreator(DataSet):
    ''' This class is a DataSet that creates the output MockClimateFiles
    objects, given an output pattern and a set of Constraints.

    A FileCreator has a 'output_pattern' attribute which defines
    what the filename of any created output files should be.

    The ArgumentCreator class compares these possible created files
    and throws away any that cannot be created as they do not have
    matching files in the input DataSet.

    The output pattern has a particular syntax. The pattern is given as a
    string with attribute names surrounded by % signs.

    eg:

    "/projects/ua6/CAWCR_CVC_processed/%variable%_%modeling_realm%_%model%.nc"

    This class will raise an error if it is instantiated with a pattern with
    empty constraints - it does not make sense to have a file creator that
    has 'empty' or 'all' constraints - they must be in canonical form.

    It will also raise an error if instantiated with 'extra' constraints
    that do not match those from its output pattern.

    '''

    def __init__(self, output_pattern, extra_constraints):
        ''' This constructor sets up the FileCreator from the
        pattern of its output and 'extra' constraints that set the value of
        its attributes.

        '''

        module_logger.debug("Building FileCreator with pattern: {0} and extra_constraints: {1}"
                            .format(output_pattern, extra_constraints))

        self.output_pattern = output_pattern

        # Construct the initial constraints from the output pattern.
        self.constraints = FileCreator.constraints_from_pattern(output_pattern)

        # Add the extra constraints to the self.constraints, strip out any that
        # are not part of the output pattern.
        self.merge_constraints(extra_constraints)

        # This object must create files, so after merging all constraints must
        # be in canonical form.
        # "extra" and "info" are keywords for non-compulsory constraints that 
        # are replaced by a placeholder value.
        for constraint in self.constraints:
            if not constraint.values:
                split_key = constraint.key.split('_')
                if 'extra' in split_key:
                    constraint.values = set(['noextras'])
                elif 'info' in split_key:
                    constraint.values = set(['orig'+split_key[0]])
                else:
                    module_logger.error("Constraint {0} is empty - should be in canonical form!"
                                        .format(constraint))
                    raise EmptyConstraintError("Constraint {0} is empty - should be in canonical form!"
                                               .format(constraint))                

        # A set to hold all the valid combinations of attributes.
        self.valid_combinations = set()
        # One to hold the valid_hashes
        self.valid_hashes = set()

        module_logger.debug("After init, self.constraints: {}"
                            .format(self.constraints))
        self.cons_names = [cons.key for cons in self.constraints]

    def get_files(self, att_dict, check=False, update=True):
        """ This method returns all possible MockClimateFiles from the
        FileCreator that match an input attribute dictionary.

        If check is True, then we check that the hash of the for the
        file is in the - 'valid_hashes' hash list. This is used when using
        the FileCreator as an input, we only want to give files that
        actually exists.

        """

        module_logger.debug("Search attribute dict is: {}".format(att_dict))
        module_logger.debug("Before getting constraint, all constraints are: {}"
                            .format(self.constraints))

        # Get the keys of the input dictionary.
        search_keys = [att for att in att_dict.keys()]

        cons_names = [cons.key for cons in self.constraints]
        to_loop = []

        # We do this for every constraint in the FileCreator
        for key in cons_names:
            if key not in search_keys:
                # If a key is not in the att_dict, grab the existing constraint.
                existing_cons = self.get_constraint(key)
                to_loop.append((existing_cons.key, existing_cons.values))
                module_logger.debug("Adding {0} to to_loop: constraint from output"
                                    .format((existing_cons.key, existing_cons.values)))
                assert(type(existing_cons.values == set))
            else:
                new_cons = Constraint(key, [att_dict[key]])
                to_loop.append((new_cons.key, new_cons.values))
                module_logger.debug("Adding {0} to to_loop: constraint from input"
                                    .format((new_cons.key, new_cons.values)))

        module_logger.debug("to_loop is: {0}".format(to_loop))

        keys = [cons[0] for cons in to_loop]
        values = [cons[1] for cons in to_loop]

        module_logger.debug("Keys to search for are: {0}".format(keys))
        module_logger.debug("Building itertools product for values: {0}"
                            .format(values))
        new_iter = itertools.product(*values)

        outfiles = []
        for combination in new_iter:
            new_file = self.climate_file_from_combination(keys, combination,
                                                          check, update)
            if new_file:
                outfiles.append(new_file)

        return outfiles

    @property
    def files(self):
        """ This property returns all the real files
        that exist in this file_creator.

        """

        huge_iterator = itertools.product(*[cons.values
                                            for cons in self.constraints])
        cons_names = [cons.key for cons in self.constraints]

        for combination in huge_iterator:
            # Create a set of constraints for this combination.
            climate_file =  self.climate_file_from_combination(cons_names, combination,
                                                               check=True, update=False)
            if climate_file:
                yield climate_file

    def get_constraint(self, attribute_name):
        """ Get a particular constraint by name."""

        for constraint in self.constraints:
            if constraint.key == attribute_name:
                return constraint

        # If it can't be found, return None.
        return None

    def merge_constraints(self, new_constraints):
        """ This function adds the constraint values to the constraints from
        a pattern.

        """

        existing_cons_names = [cons.key for cons in self.constraints]
        module_logger.debug("existing cons names is: {}"
                            .format(existing_cons_names))

        # Now add the constraints - only if they are in the pattern!
        module_logger.debug("new_constraints is: {}"
                            .format(new_constraints))
        for cons in new_constraints:
            if cons.key in existing_cons_names:
                self.constraints.add(cons)

        attribute_names = [cons.key for cons in self.constraints]

        repeated_atts = []
        for name in attribute_names:
            if attribute_names.count(name) > 1:
                repeated_atts.append(name)

        to_remove = [cons for cons in self.constraints
                     if cons.key in repeated_atts]

        new_cons_dict = {}
        for cons in to_remove:
            new_cons_dict[cons.key] = set([])

        module_logger.debug("Constraints to remove: {0}"
                            .format(to_remove))

        for cons in to_remove:
            new_cons_dict[cons.key] = new_cons_dict[cons.key].union(cons.values)
            self.constraints.remove(cons)

        module_logger.debug("New Constraints Dictionary: {0}"
                            .format(new_cons_dict))

        for key in new_cons_dict:
            self.constraints.add(Constraint(key, new_cons_dict[key]))

        module_logger.debug("New output constraints are: {0}"
                            .format(self.constraints))

    def climate_file_from_combination(self, keys, next_combination,
                                      check, update):
        """ Make a possible output ClimateFiles object from
        a combination of attributes.
        """

        # Turn the combination tuple into a dictionary with
        # attribute names.
        sub_dict = {}
        cons_list = []
        for key, value in zip(keys, next_combination):
            sub_dict[key] = value
            cons_list.append(Constraint(key, [value]))

        module_logger.debug("Substitution dictionary sub_dict = {0}".format(sub_dict))
        new_file = self.output_pattern
        for key in sub_dict:
            att_sub = "%" + key + "%"
            new_file = re.sub(att_sub, sub_dict[key], new_file)

        new_path = os.path.dirname(new_file)
        file_name = os.path.basename(new_file)

        new_climate_file = MetaFile(path_dir=new_path,
                                    filename=file_name,
                                    all_atts=sub_dict)

        file_hash = hash(new_climate_file)

        if check:
            # Hash the climate file and see if it is in the dictionary.
            # If it is not, return None.
            if file_hash not in self.valid_hashes:
                module_logger.debug("Hash check failed - file hash not in self.valid_hashes")
                module_logger.debug("Climate file is: {0}".format(new_climate_file))
                module_logger.debug("Hash is: {0}".format(file_hash))
                return None

        if update:
            # Add the hash to the 'valid_hashes' set.
            self.valid_hashes.add(file_hash)
            module_logger.debug("Adding file hash to self.valid_hashes")
            module_logger.debug("Climate file is: {0}".format(new_climate_file))
            module_logger.debug("Hash is: {0}".format(file_hash))
            # Add this set of constraints it to the self.valid_combinations set
            # for looping.

            self.valid_combinations.add(frozenset(cons_list))

        return new_climate_file

    @staticmethod
    def default_pattern(out_constraints, temp=False):
        """ Creates a default pattern from a set of constraints.

        Mostly for testing - we could extend this to use real patterns.

        """

        out_pattern = ''

        for cons in out_constraints:
            out_pattern += '%' + cons.key + '%_'

        output = out_pattern[:-1]

        if temp:
            output = os.path.join(os.environ["TMPDIR"], output)

        return output

    @staticmethod
    def constraints_from_pattern(pattern_string):
        """ This function builds a set of constraint objects from
        an output pattern.

        """
        regex_pattern = r"%(\S+?)%"

        attribute_names = re.findall(regex_pattern, pattern_string)

        constraint_list = [Constraint(att_name, [])
                           for att_name in attribute_names]

        return set(constraint_list)


class EmptyConstraintError(Exception):
    def __init__(self, constraint):
        self.constraint = constraint
        module_logger.error("Constraint {} is empty but must contain values"
                            .format(self.constraint))

    def __repr__(self):
        return repr(self.constraint)


class ExtraConstraintError(Exception):
    def __init__(self, constraint):
        self.constraint = constraint
        module_logger.error("Constraint {} passed to FileCreator is not found in the output pattern!"
                            .format(self.constraint))

    def __repr__(self):
        return repr(self.constraint)
