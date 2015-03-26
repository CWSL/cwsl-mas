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

This module contains the MetaFile class.

"""

import os
import logging


class MetaFile(object):
    """This object represents a single file and its stored attributes."""

    def __init__(self, filename, path_dir, all_atts):

        self.path_dir = path_dir
        self.filename = filename

        self.full_path = os.path.join(path_dir, filename)
        self.all_atts = all_atts

    def __repr__(self):

        return "<MetaFile: %s>" % self.full_path

    def __hash__(self):

        frozen_keys = frozenset(sorted(self.all_atts.keys()))
        frozen_vals = frozenset(sorted(self.all_atts.values()))

        val = hash(frozen_keys) ^ hash(frozen_vals) ^ hash(self.full_path)

        return val

    def __eq__(self, other):
        """ Two MetaFiles are equal if their hashs are equal."""
        
        return(hash(self) == hash(other))
