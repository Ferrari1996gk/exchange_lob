__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"


"""
Description: Set of utility functions for an agent-based market simulator.
"""


def flatten(t: list):
    """Return a flattened list"""
    return [item for sublist in t for item in sublist]
