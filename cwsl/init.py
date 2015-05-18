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

This module registers all the VisTrails modules that can be
added to the GUI.

"""

# Vistrails imports
from vistrails.core.packagemanager import get_package_manager
from vistrails.gui.preferences import QPackageConfigurationDialog
from vistrails.core.modules.module_registry import get_module_registry

# Available Tools
# TODO: Imports will get large as tools get added, 
#  add automatic import similar to Spreadsheet package
import cwsl.vt_modules.drs_dataset as drs
from cwsl.vt_modules.vt_dataset import VtDataSet
from cwsl.vt_modules.vt_cdscan import CDScan
from cwsl.vt_modules.vt_xmltonc import XmlToNc
from cwsl.vt_modules.vt_seas_vars import SeasVars
from cwsl.vt_modules.vt_climatology import Climatology
from cwsl.vt_modules.change import TimesliceChange
from cwsl.vt_modules.vt_nino34 import IndicesNino34
from cwsl.vt_modules.vt_general_command_pattern import GeneralCommandPattern
from cwsl.vt_modules.vt_constraintbuilder import ConstraintBuilder
from cwsl.vt_modules.vt_plot_timeseries import PlotTimeSeries
from cwsl.vt_modules.vt_field_agg import FieldAggregation
from cwsl.vt_modules.vt_meridional_agg import MeridionalAggregation
from cwsl.vt_modules.vt_zonal_agg import ZonalAggregation
from cwsl.vt_modules.vt_vertical_agg import VerticalAggregation
from cwsl.vt_modules.vt_remap import Remap
from cwsl.vt_modules.vt_dataset_arithmetic import DatasetArithmetic
from cwsl.vt_modules.imageviewer import ImageViewerPanel
from cwsl.vt_modules.cmip5_constraints import CMIP5Constraints
from cwsl.vt_modules.sdm_extract import SDMDataExtract
from cwsl.vt_modules.cod_dataset import ChangeOfDate
from cwsl.vt_modules.json_extract import ExtractTimeseries



def initialize(*args, **keywords):

    # We'll first create a local alias for the module_registry so that
    # we can refer to it in a shorter way.
    reg = get_module_registry()

    # VisTrails cannot currently automatically detect your derived
    # classes, and the ports that they support as input and
    # output. Because of this, you as a module developer need to let
    # VisTrails know that you created a new module. This is done by calling
    # function addModule:
    reg.add_module(VtDataSet, abstract=True)
    reg.add_module(drs.DataReferenceSyntax, abstract=True)

    #Datasets
    reg.add_module(drs.GlobalClimateModel, namespace='DataSets|Generic',
                   name="Global Climate Model Dataset")
    reg.add_module(drs.RegionalClimateModel, namespace='DataSets|Generic',
                   name='Regional Climate Model DataSet')
    reg.add_module(drs.CMIP5, namespace='DataSets|GCM',
                   name="CMIP5")
    reg.add_module(drs.CMIP3, namespace='DataSets|GCM',
                   name="CMIP3")
    reg.add_module(drs.RegionalClimateModel_CCAM_NRM,
                   name='CSIRO-CCAM-NRM', namespace='DataSets|RCM')
    reg.add_module(drs.RegionalClimateModel_SDMa_NRM,
                   name='BOM-SDMa-NRM', namespace='DataSets|RCM')

    #Aggregation
    reg.add_module(CDScan, name='Merge Timeseries', namespace='Aggregation')
    reg.add_module(SeasVars, name='Seasonal Timeseries',
                   namespace='Aggregation')
    reg.add_module(XmlToNc, name='Timeslice',
                   namespace='Aggregation')
    reg.add_module(Climatology, name="Climatology",
                   namespace='Aggregation')
    reg.add_module(FieldAggregation, name="Field Aggregation",
                   namespace="Aggregation")
    reg.add_module(MeridionalAggregation, name="Meridional Aggregation",
                   namespace="Aggregation")
    reg.add_module(ZonalAggregation, name="Zonal Aggregation",
                   namespace="Aggregation")
    reg.add_module(VerticalAggregation, name="Vertical Aggregation",
                   namespace="Aggregation")

    #Change
    reg.add_module(TimesliceChange, name="Timeslice Change",
                   namespace='Change')

    #Indices
    reg.add_module(IndicesNino34, name='Nino3.4', namespace='Indices')
 
    #Visualisation
    reg.add_module(PlotTimeSeries, name='Plot Timeseries', namespace='Visualisation')
    #ImageViewerPanel depends on the Spreadsheet package
    reg.add_module(ImageViewerPanel, name='Image Viewer', namespace='Visualisation')
    reg.add_input_port(ImageViewerPanel, 'in_dataset', 'csiro.au.cwsl:VtDataSet')

    #General
    reg.add_module(ConstraintBuilder, name='Constraint Builder',
                   namespace='Utilities')
    reg.add_module(CMIP5Constraints, name='CMIP5 Constraints',
                   namespace='Utilities')
    reg.add_module(GeneralCommandPattern, name='General Command Line Program',
                   namespace='Utilities')
    reg.add_module(Remap, name='Remap horizontal grid',
                   namespace='Utilities')
    reg.add_module(DatasetArithmetic, name='Dataset Arithmetic',
                   namespace='Utilities')

    # Statistical Downscaling.
    reg.add_module(ChangeOfDate, name='Change of Date Files',
                   namespace='Statistical Downscaling')
    reg.add_module(SDMDataExtract, name='Data Extraction',
                   namespace='Statistical Downscaling')
    reg.add_module(ExtractTimeseries, name='Extract JSON Timeseries',
                   namespace='Statistical Downscaling')


def menu_items():
    """
    menu_items() -> tuple of (str,function)
    It returns a list of pairs containing text for the menu and a
    callback function that will be executed when that menu item is selected.
    """
    def package_configuration():
        """
        Create a shortcut to Edit->Preferences->Module Package->Enabled Packages->Configure in menu.
        """
        pkgmgr = get_package_manager()
        package = pkgmgr.get_package(identifier)
        dlg = QPackageConfigurationDialog(None, package)
        dlg.exec_()

    lst = []
    lst.append(("Configure", package_configuration))
    return tuple(lst)
