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

"""

import os

from vistrails.core.configuration import ConfigurationObject

# Try to get a project from environment variable for
# setting up directory structure
try:
    PROJECT = os.environ['PROJECT']
except KeyError:
    PROJECT = 'no_project_set'
USER = os.environ['USER']

configuration = ConfigurationObject(
    #Path to MIP data
    drs_basepath='',
    #Path to QA/QC processed data
    authoritative_basepath='',
    #Path to put user files
    user_basepath='/short/%s/%s/' % (PROJECT, USER),
    #Execution Manager
    execution_manager='SimpleExecManager',
    #Execution Options
    execution_options='update',
    #Dummy run
    simulate_execution=False,
    #Data manager
    data_manager='SimpleDataManager',
    #Turn on debugging
    debug=False,
    #Path to CWSL Climate Toolkit
    cwsl_ctools_path='',
    )
