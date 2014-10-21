'''
Created on 26/05/2014

Contains the PatternGenerator class.

@filename pattern_generator.py

Copyright CSIRO 2014

'''

__version__ = "$Revision: 1548 $"

import os

from cwsl.configuration import configuration


class PatternGenerator(object):
    """ Class to pick the correct pattern (if one exists) and pass it into

    a VisTrails module.

    """

    def __init__(self, destination, data_type):
        self.destination = destination
        self.data_type = data_type

        # Get the user and project information.
        try:
            self.project = os.environ["PROJECT"]
        except KeyError:
            self.project = "NoProjectSet"
        self.user = os.environ["USER"]

        self.pbas = self.load_pbas()
        self.paths = self.load_paths()

        # Check the inputs to make sure that they make sense.
        self.check_inputs()

    def check_inputs(self):

        # Throw some errors if the parameters don't make sense.
        drs_only = ["downloaded"]
        if (self.destination != "drstree" and self.data_type in drs_only):
            raise BadCombinationError
        if self.data_type not in self.paths:
            raise PatternNotFoundError

    @property
    def pattern(self):
        """ Return the correct full pattern. """
        pba = self.pbas[self.destination]
        fullpath = self.paths[self.data_type]

        return os.path.join(pba, fullpath)

    def load_pbas(self):
        """ The paths before activity are pulled from the configuration object. """

        pba_dict = {}
        pba_dict["drstree"] = configuration.drs_basepath
        pba_dict["authoritative"] = configuration.authoritative_basepath
        pba_dict["user"] = configuration.user_basepath

        return pba_dict

    def load_paths(self):
        """ The patterns live here! """

        fullpath_dict = {}
        fullpath_dict["seasonal"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                 "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%-%seas_agg%_%grid%.nc")
        fullpath_dict["downloaded"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                   "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%origstart%-%origend%.nc")    
        fullpath_dict["timeseries"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                   "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_cdat-5-1-0.xml")
        fullpath_dict["cdat_lite_catalogue"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                            "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_cdat-lite-6-0rc2-py2.7.xml")
        fullpath_dict["seasonal_aggregate"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                           "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%-%seas_agg%_%grid%.nc")
        return fullpath_dict


# Here are some exceptions that get raised if the patterns are not correct.
class BadCombinationError(Exception):
    """ To be raised if a combination that doesn't make sense is requested:

    for example, user downloaded data .

    """
    # TODO (TB): actually, user downloaded data could be useful.

    pass


class PatternNotFoundError(Exception):
    """ Raised if a requested pattern is not found. """

    pass
