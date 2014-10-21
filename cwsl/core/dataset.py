'''
Created on 14/02/2014

Contains the Dataset class.

@filename: dataset.py

Part of the CWSLab Model Analysis Service VisTrails plugin.

Copyright CSIRO 2014

'''

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
