import logging

module_logger = logging.getLogger('cwsl')
ch = logging.StreamHandler()
# When not testing, only log WARNING and above.
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
module_logger.addHandler(ch)
