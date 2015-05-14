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

Contains the Dataset class.

"""

import abc


class DataSet(object):
    """ This is an abstract base class so that we can
    check if things are DataSets, do some abstract methods etc.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_files(self, reqs_dict, **kwargs):
        """ Return the matching files from the dataset given a dictionary
        of restrictions. """
        return

    @abc.abstractmethod
    def get_constraint(self, attribute_name):
        """ Given an attribute name, return the matching constraint."""
        return

    def alias_constraint(self, existing_constraint, alias):
        """ Alias a constraint for masking purposes. """

        try:
            self.alias_map[alias] = existing_constraint
        except AttributeError:
            self.alias_map = {}
            self.alias_map[alias] = existing_constraint
