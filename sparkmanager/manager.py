"""Module doing the actual Spark management
"""
from contextlib import contextmanager
from functools import update_wrapper
from pyspark.sql import SparkSession
from six import iteritems

import os


class SparkManager(object):
    """Manage Spark with a singular object
    """
    def __init__(self):
        self.__session = None
        self.__context = None

        self.__allowed = None
        self.__overlap = None

        self.__gstack = [(None, None)]

    @property
    def spark(self):
        """:property: the Spark session
        """
        return self.__session

    @property
    def sc(self):
        """:property: the Spark context
        """
        return self.__context

    def __getattr__(self, attr):
        """Provide convenient access to Spark functions
        """
        if attr in self.__dict__:
            return self.__dict__[attr]
        if self.__overlap is None:
            raise ValueError("Spark has not been initialized yet!")
        if attr in self.__overlap:
            raise AttributeError("Cannot resolve attribute unambiguously!")
        if attr not in self.__allowed:
            raise AttributeError("Cannot resolve attribute!")
        return getattr(self.__session, attr, getattr(self.__context, attr))

    def create(self, name=None, config=None, options=None):
        """Create a new Spark session if needed

        Will use the name and configuration options provided to create a new
        spark session and populate the global module variables.

        :param name: the name of the spark application
        :param config: configuration parameters to be applied before
                       building the spark session
        :param options: environment options for launching the spark session
        """
        if self.__session:
            return self.__session

        # TODO auto-generate name?
        if not name:
            raise ValueError("need a name for a new spark session")

        if options:
            os.environ['PYSPARK_SUBMIT_ARGS'] = options + ' pyspark-shell'

        session = SparkSession.builder.appName(name)

        if config:
            for k, v in iteritems(config):
                session.config(k, v)

        self.__session = session.getOrCreate()
        self.__context = self.__session.sparkContext

        s_attr = set(dir(self.__session))
        c_attr = set(dir(self.__context))

        self.__allowed = s_attr | c_attr
        self.__overlap = s_attr & c_attr

        identical = set(i for i in self.__overlap
                        if getattr(self.__session, i) is getattr(self.__context, i))
        self.__overlap -= identical
        self.__allowed |= identical

        return self.__session

    def assign_to_jobgroup(self, f):
        """Assign a spark job group to the jobs started within the decorated
        function

        The job group will be named after the function, with the docstring as
        description.
        """
        n = f.func_name
        d = f.__doc__.strip()

        def new_f(*args, **kwargs):
            with self.jobgroup(n, d):
                return f(*args, **kwargs)
        return update_wrapper(new_f, f)

    @contextmanager
    def jobgroup(self, name, desc=""):
        """Temporarily assign a job group to spark jobs within the context

        :param name: the name of the spark group to use
        :param desc: a longer description of the job group
        """
        self.__context.setJobGroup(name, desc)
        self.__gstack.append((name, desc))
        try:
            yield
        finally:
            self.__gstack.pop()
            self.__context.setJobGroup(*self.__gstack[-1])
