"""Convenience methods for a homogeneous Spark setup
"""
from contextlib import contextmanager
from functools import update_wrapper
from pyspark.sql import SparkSession
from six import iteritems

import os

session = None
context = None


def create(name=None, config=None, options=None):
    """Create a new Spark session if needed

    Will use the name and configuration options provided to create a new
    spark session and populate the global module variables.

    :param name: the name of the spark application
    :param config: configuration parameters to be applied before building the spark session
    :param options: environment options for launching the spark session
    """
    global session, context

    if session:
        return session

    # TODO auto-generate name?
    if not name:
        raise ValueError("need a name for a new spark session")

    if options:
        os.environ['PYSPARK_SUBMIT_ARGS'] = options + ' pyspark-shell'

    session = SparkSession.builder.appName(name)

    if config:
        for k, v in iteritems(config):
            session.config(k, v)

    session = session.getOrCreate()
    context = session.sparkContext

    return session


def assign_to_jobgroup(f):
    """Assign a spark job group to the jobs started within the decorated
    function

    The job group will be named after the function, with the docstring as
    description.
    """
    n = f.func_name
    d = f.__doc__.strip()

    def new_f(*args, **kwargs):
        with jobgroup(n, d):
            return f(*args, **kwargs)
    return update_wrapper(new_f, f)


# TODO spark currently has no interface to get the current job group, thus
# save it in a stack and restore manually
__gstack = [(None, None)]


@contextmanager
def jobgroup(name, desc=""):
    """Temporarily assign a job group to spark jobs within the context

    :param name: the name of the spark group to use
    :param desc: a longer description of the job group
    """
    global __gstack
    context.setJobGroup(name, desc)
    __gstack.append((name, desc))
    try:
        yield
    finally:
        __gstack.pop()
        context.setJobGroup(*__gstack[-1])
