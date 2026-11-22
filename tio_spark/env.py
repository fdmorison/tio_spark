"""
Environment of the tio_spark family.

Loads the project `.env` file, found in the working directory or one of its
parents, and lets its variables win over the ones already set in the shell.
A `SPARK_HOME` in the project `.env` therefore wins over one exported by the
user's shell profile.
"""

from environs import Env

_env = Env(expand_vars=True)
_env.read_env(".env", recurse=True, override=True)
