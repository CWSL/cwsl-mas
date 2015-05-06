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

Contains the PatternGenerator class.

"""

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
                                                 "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%-%seas_agg%_%grid%.%suffix%")
        fullpath_dict["monthly_ts"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                   "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%_%grid%.%suffix%")
        fullpath_dict["seasonal_indices"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                         "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%-%seas_agg%_%index%_%grid%.%suffix%")
        fullpath_dict["monthly_indices"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                        "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%_%index%_%grid%.%suffix%")
        fullpath_dict["downloaded"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                   "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%origstart%-%origend%.nc")
        fullpath_dict["timeseries"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                   "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_cdat-5-1-0.xml")
        fullpath_dict["cdat_lite_catalogue"] = os.path.join("%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                            "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_cdat-lite-6-0rc2-py2.7.%suffix%")
        fullpath_dict["seasonal_aggregate"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                           "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%year_start%-%year_end%-%seas_agg%_%agg_type%_%grid%.%suffix%")
        fullpath_dict["timeslice_change"] = os.path.join("%mip%/%product%/%grid%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/",
                                                         "%variable%_%mip_table%_%model%_%experiment%_%ensemble%_%fut_start%-%fut_end%_%change_type%-wrt_%hist_start%-%hist_end%_%seas_agg%_%grid%.nc")

        return fullpath_dict


# Here are some exceptions that get raised if the patterns are not correct.
class BadCombinationError(Exception):
    """ Raised if a combination that doesn't make sense is requested."""

    pass


class PatternNotFoundError(Exception):
    """ Raised if a requested pattern is not found."""

    pass
