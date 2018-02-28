Spark Management Consolidated
=============================

A small module that will load as a singleton class object to manage Spark
related things:

.. code:: python

   import sparkmanager as sm

   # Create a new application
   sm.create("My fancy name")

   data = sm.spark.range(5)
   with sm.jobgroup("broadcasting some data"):
       data = sm.broadcast(data.collect())

The Spark session can be accessed via ``sm.spark``, the Spark context via
``sm.sc``. Both attributes are instantiated once the ``create`` method is
called, with the option to call unambiguous methods from both directly via
the :py:class:`SparkManager` object:

.. code:: python

   c = sm.parallelize(range(5))
   d = sm.sc.parallelize(range(5))
   assert c.collect() == d.collect()
