"""
Module:
    motiv.version

Description:
    contains constants defining the package version

Note:
    - In case of a bugfix/hotfix increment BUILD_NUMBER
    - In case of adding a feature that modifies the package,
        but does not break interfaces, increment VERSION_MINOR
    - In case of introducing a change that breaks
        backward-comptability, increment VERSION_MAJOR
"""

def get_version():
    VERSION_MAJOR = 0
    VERSION_MINOR = 1
    BUILD_NUMBER = 0

    VERSION = f"{VERSION_MAJOR}.{VERSION_MINOR}.{BUILD_NUMBER}"
    return VERSION

__version__ = get_version()
__all__ = ['__version__']
