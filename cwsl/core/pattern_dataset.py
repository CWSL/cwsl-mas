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

Contains the PatternDataSet class.

"""

import os.path
import logging
import glob
import re
import itertools
from collections import defaultdict

from cwsl.core.metafile import MetaFile
from cwsl.core.constraint import Constraint
from cwsl.core.dataset import DataSet
from cwsl.core.file_creator import FileCreator

module_logger = logging.getLogger('cwsl.core.pattern_dataset')


class PatternDataSet(DataSet):
    """ This implmentation of the DataSet abstract class builds a collection of

    files and attributes by scanning a pattern on the file system.

    """

    def __init__(self, pattern_to_glob, constraint_set=set()):
        """ Arguments:

        pattern_to_glob: this is a string filename pattern, with placeholders
                         surrounding attribute names.
                         e.g. "/home/billy/test/%colour%/%texture%/%fruit%_%colour%.%file_type%"
        
        constraint_set: A set of Constraint objects to restrict the values the
                        attributes can take.
        """
        
        self._files = None

        self.check_filename_pattern(pattern_to_glob,
                                    constraint_set)

        if constraint_set:
            self.restricted_patterns = []
            given_names, given_values = zip(*[(cons.key, cons.values)
                                              for cons in constraint_set])

            # Generate all combinations
            for combination in itertools.product(*given_values):
                new_pattern = pattern_to_glob
                for i, name in enumerate(given_names):
                    pat_name = "%" + name + "%"
                    new_pattern = re.sub(pat_name, combination[i], new_pattern)
                self.restricted_patterns.append(new_pattern)
        else:
            self.restricted_patterns = [pattern_to_glob]

        # Build a globbing pattern from each restricted pattern.
        self.glob_patterns = [re.sub(r"%.+?%", r"*", pattern)
                              for pattern in self.restricted_patterns]

        # Build a regex from the original pattern.
        self.regex_pattern = self.generate_regex(pattern_to_glob)

        # Update the constraints to only include those that are matched.
        self.constraints = self.update_constraints()

        # Update the constraints.
        bad_cons_names = [cons.key for cons in constraint_set]
        to_remove = []
        for cons in self.constraints:
            if cons.key in bad_cons_names:
                to_remove.append(cons)

        for cons in to_remove:
            self.constraints.remove(cons)

        self.constraints = self.constraints.union(constraint_set)
        self.cons_names = [cons.key for cons in self.constraints]

        self.subsets = self.create_subsets()

        # Find all the valid values for the constraints for later
        # looping.
        self.valid_combinations = self.generate_valids()

    @property
    def files(self):
        # If there are already files found, do not glob.
        if not self._files:
            self._files = self.glob_fs()

        return self._files

    def check_filename_pattern(self, glob_pattern, constraints):
        """ Check that added constraints are also found in the input pattern."""
        
        generated_cons = FileCreator.constraints_from_pattern(glob_pattern)
        gen_names = [cons.key for cons in generated_cons]
        for cons in constraints:
            if cons.key not in gen_names:
                raise ConstraintNotFoundError("Constraint {} is not found in output pattern {}".
                                              format(cons.key, glob_pattern))

    def glob_fs(self):
        """ Returns a list of the files that match the

        glob patterns.

        """

        found_files = []
        for pattern in self.glob_patterns:
            found_files += [PathString(present_file)
                            for present_file in glob.glob(pattern)]
        return found_files

    def __iter__(self):
        """ Make the object iterable """

        return iter(self.files)

    def get_constraint(self, attribute_name):
        """ Get a particular constraint by name."""

        for constraint in self.constraints:
            if constraint.key == attribute_name:
                return constraint

        # If it can't be found, return None.
        return None

    def generate_regex(self, pattern_to_glob):
        """ From the input pattern (of from %model%_%variable%_%agg_type% etc.)

        create a regular expression that will match the file name and extract
        the named attributes (model=?, variable=?, att_type=?)

        """

        # First thing, replace '.' with '\.' and make sure we're going
        # to match the start and the end of the pattern string.
        pattern_to_glob = re.sub(r"\.", r"\.", pattern_to_glob)
        pattern_to_glob = r"^" + pattern_to_glob + r"$"

        found_values = []
        new_val = pattern_to_glob

        # Find all the named attributes
        all_matches = re.findall(r"%.+?%", pattern_to_glob)

        for match_thing in all_matches:
            name = match_thing[1:-1]

            if match_thing not in found_values:
                # Change to a named group.
                group_string = r"(?P<" + name + r">.+?)"
                new_val = re.sub(match_thing, group_string, new_val, count=1)
                found_values.append(match_thing)
            else:
                # Use the existing group.
                group_string = r"(?P=" + name + r")"
                new_val = re.sub(match_thing, group_string, new_val, count=1)

        return new_val

    def update_constraints(self):
        """ Loop through the files in the dataset and update

        self.constraints to match only the constraint values
        that actually exist in the DataSet.

        """

        new_cons_dir = defaultdict(set)
        for found_file in self.files:
            match = re.match(self.regex_pattern, found_file)
            if match:
                for thing in match.groupdict():
                    new_cons_dir[thing].add(match.groupdict()[thing])
            else:
                module_logger.error("Pattern regex did not match found file!")
                raise Exception

        cons_set = set()
        for new_cons in new_cons_dir:
            cons_set.add(Constraint(new_cons, new_cons_dir[new_cons]))

        return cons_set

    def get_files(self, reqs_dict, **kwargs):
        """ Get the required file set from the dataset,

        takes in a reqs_dict of structure:
        {"attribute": "value"},
        for example:
           {"variable": "tas",
            "model": "NorESM1",
            "activity": "CMIP5"}

        This method would then return all the files in the dataset that have
        variable tas, model NorESM and activity CMIP5.

        This requires self.create_subsets() to be called before running.

        Returns a list of MetaFile objects.

        """

        files_returned = []

        for key in reqs_dict:
            if key in self.cons_names:
                att_value = reqs_dict[key]
                files_returned.append(self.subsets[key][att_value])

        all_files = set.intersection(*files_returned)

        output = []
        for full_path in all_files:
            path, name = os.path.split(full_path)
            output.append(MetaFile(name, path, {}))

        return output

    def create_subsets(self):
        """ Sets up a hash table to allow you to get the required files
        by key and attribute.

        """
        new_dict = defaultdict(dict)

        for found_file in self.files:
            match = re.match(self.regex_pattern,
                             found_file)
            if match:
                groups = match.groupdict()
                for att in groups:
                    value = groups[att]
                    try:
                        new_dict[att][value].add(found_file)
                    except KeyError:
                        new_dict[att][value] = set([found_file])

        return new_dict

    def generate_valids(self):
        """ Generate the valid combinations of constraints.

        This is quite inefficient - doubles the number of regex
        matches.

        """

        new_valids = set()

        # Loop over files
        for present_file in self.files:
            match = re.match(self.regex_pattern, present_file)
            if match:
                valids = []
                matchdict = match.groupdict()
                for cons in matchdict:
                    valids.append(Constraint(cons, [matchdict[cons]]))
                new_valids.add(frozenset(valids))

        return new_valids


class PathString(str):
    """ Helper class so that the file name strings
    can pick up a full_path attribute.

    """

    def __init__(self, value):
        super(PathString, self).__init__(value)
        self.full_path = value

class ConstraintNotFoundError(Exception):
    """ Exception class for misspelled constraints. """
    pass
