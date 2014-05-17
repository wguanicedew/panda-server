""" 
    version
"""
version_base = "0.0.2"

import commands
import os


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


__version__ = version_base + get_git_version()


