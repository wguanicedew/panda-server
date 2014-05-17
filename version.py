""" 
    version
"""
import commands
import ConfigParser
import os


def get_version_base_release_type():
    """
        get_version_base ... get version string base from the setup.cfg
    """
    config = ConfigParser.ConfigParser()
    config.read(os.path.dirname(os.path.realpath(__file__)) + '/setup.cfg')
    version_base = config.get("global", "version")
    release_type = config.get("global", "release_type")
    return (version_base, release_type)


def get_git_version():
    """
        get version of this git commit
    """
    dir = os.path.dirname(os.path.realpath(__file__))
    ### count cap of total number of commits in this repository
    ncommits = 1
    try:
        ncommits = commands.getoutput('git shortlog | wc -l')
    except:
        pass
    ### get last revision ID, short version
    last_rev_id = ''
    try:
        last_rev_id = commands.getoutput('git show -s --pretty=format:%h ')
    except:
        pass
    ### get number of revisions on this branch
    nrevs = 1
    try:
        nrevs = commands.getoutput('git reflog | wc -l')
    except:
        pass

    return str('.dev-' + ncommits + '-' + last_rev_id + '-' + nrevs)


def get_version():
    isStable = False
    version_base, release_type = get_version_base_release_type()
    if release_type == 'stable':
        isStable = True
    __version__ = version_base
    if not isStable:
        __version__ += get_git_version()
    return __version__


__version__ = get_version()

