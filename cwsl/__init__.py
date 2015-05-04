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
import sys
import logging

module_logger = logging.getLogger('cswl')
ch = logging.StreamHandler()
# When not testing, only log WARNING and above.
ch.setLevel(logging.WARNING)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
module_logger.addHandler(ch)

#Initialise project path
PROJECT_PATH = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
sys.path.append(PROJECT_PATH)

from cwsl.configuration import configuration

name = 'Climate and Weather Science Laboratory'
identifier = 'csiro.au.cwsl'
version = '0.1.0'

def package_dependencies():
    import vistrails.core.packagemanager
    manager = vistrails.core.packagemanager.get_package_manager()
    if manager.has_package('org.vistrails.vistrails.spreadsheet'):
        return ['org.vistrails.vistrails.spreadsheet']
    else:
        return []
