from vistrails.packages.spreadsheet.basic_widgets import SpreadsheetCell
from vistrails.packages.spreadsheet.spreadsheet_controller import spreadsheetController
from vistrails.packages.spreadsheet.widgets.imageviewer.imageviewer import ImageViewerCellWidget

import os

class TestImageViewerCell(SpreadsheetCell):
    """
    ImageViewerCell is a custom Module to display labels, images, etc.
    
    """
    def compute(self):
        """ compute() -> None
        Dispatch the display event to the spreadsheet with images and labels
        
        """
        if self.hasInputFromPort("File"):
            window = spreadsheetController.findSpreadsheetWindow()
            file_to_display = self.getInputFromPort("File")
            fileValue = window.file_pool.make_local_copy(file_to_display.name)
        else:
            fileValue = None
        self.displayAndWait(ImageViewerCellWidget, (fileValue, ))

class ImageViewerPanel(SpreadsheetCell):
    """
    ImageViewerCell is a custom Module to display groups of images using the Spreadsheet package
    
    """
    def compute(self):
        """ compute() -> None
        Dispatch the display event to the spreadsheet with images and labels
        
        """

        if self.hasInputFromPort('in_dataset'):
            dataset = self.getInputFromPort('in_dataset')
            window = spreadsheetController.findSpreadsheetWindow()
            for f in dataset.files:
                if os.path.exists(f.full_path):
                    fileValue = window.file_pool.make_local_copy(f.full_path)
                    self.displayAndWait(ImageViewerCellWidget, (fileValue, ))
        else:
            self.displayAndWait(ImageViewerCellWidget, (None, ))
