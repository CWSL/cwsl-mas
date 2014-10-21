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

This module contains the MetaFiles class.

Part of the Model Analysis Service VisTrails plugin
Copyright CSIRO 2013

"""


import os



class MetaFiles(object):
    ''' This object holds the same information that a ClimateFile does,

    but stands in because you can't create ClimateFiles objects without
    the correct ForeignKeys and the directories will not exist in the
    db yet.

    '''

    def __init__(self, filename, path_dir, all_atts):

        self.path_dir = path_dir
        self.filename = filename

        self.full_path = os.path.join(path_dir, filename)
        self.all_atts = all_atts

    def create_file(self):
        ''' This method takes the information stored in the MetaClimateFile and

        creates a ClimateFile object that is saved in the database.

        '''
        pass

        #host_machine, created = HostMachines.objects.get_or_create(name=config.HOST_NAME)
        #return FileSystemTrawler.create_file_object(self.all_atts, host_machine)

    def __repr__(self):

        return "<MetaFile: %s>" % self.full_path

    def __hash__(self):

        frozen_keys = frozenset(sorted(self.all_atts.keys()))
        frozen_vals = frozenset(sorted(self.all_atts.values()))

        val = hash(frozen_keys) ^ hash(frozen_vals) ^ hash(self.full_path)

        return val
