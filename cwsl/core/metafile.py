""" This module contains the MetaFiles class.

Part of the Model Analysis Service VisTrails plugin
Copyright CSIRO 2013

"""

__version__ = "$Revision: 1523 $"

import os

#import fs_trawler.configuration as config
#from fs_trawler.file_system_trawler import FileSystemTrawler
#from cwsl.db_django.MAS_db.models import HostMachines


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
