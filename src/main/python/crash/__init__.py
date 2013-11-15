# 
# Copyright 2013 Cloudera Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if not _script:
    raise RuntimeError('[BUG] _script is not set!')

import sys
import types
import inspect

# crunch
import org.apache.crunch.types.avro.Avros as Avros
import org.apache.crunch.PCollection as PCollection
import org.apache.crunch.PTable as PTable
import org.apache.crunch.PGroupedTable as PGroupedTable

# misc crash
import com.cloudera.crash.Analytic as CrashAnalytic
import com.cloudera.crash.generics.CustomData.GENERIC_SCHEMA as SCHEMA
import com.cloudera.crash.utils.ToTableShim as ToTableShim

# carrier classes
import com.cloudera.crash.carriers.Combiner as Combiner
import com.cloudera.crash.carriers.FromCollection as FromCollection
import com.cloudera.crash.carriers.FromTable as FromTable
import com.cloudera.crash.carriers.FromGroupedTable as FromGroupedTable

# set up avro types
_generic_type = Avros.generics(SCHEMA)
_collection_type = _generic_type
_table_type = Avros.tableOf(_generic_type, _generic_type)

class Analytic(CrashAnalytic):
    def __init__( self, name ):
        self._name = name
        self._stages_by_name = {}
        self._collections_by_name = {}
        self._pipeline = _script.getPipeline()
        self._last_collection = None

    def verbose( self ):
        """ Enables debugging on the underlying pipeline.
        """
        self._pipeline.enableDebug()

    def read( self, source_file ):
        """ Reads source_file text into a PCollection of lines.
        """
        self._last_collection = self._pipeline.readTextFile( source_file )
        self._collections_by_name[ source_file ] = self._last_collection
        return self._last_collection

    def write( self, target_file ):
        """ Writes the last collection to a text file
        """
        self._pipeline.writeTextFile( self._last_collection, target_file )

    def group( self ):
        """ Adds a group-by operation to the last collection.

        If the last collection is a PTable, it will be grouped. If the last
        collection is a PCollection, it will be grouped by key with null values
        if the collection does not contain Pairs.  If the last collection is a
        PGroupedTable, this will do nothing.
        """
        if isinstance( self._last_collection, PGroupedTable ):
            pass
        elif isinstance( self._last_collection, PTable ):
            self._last_collection = self._last_collection.groupByKey()
        else:
            self._last_collection = self._last_collection.parallelDo( ToTableShim(), _table_type )
            self._last_collection = self._last_collection.groupByKey()

    def parallel( self, *args, **kwargs ):
        """ Decorates a function to be used as a parallel do.

        Decorating a function with this method will add that function to the
        existing pipeline to process the collection produced by the last DoFn.
        If arguments are provided, they are used for configuration.

        Currently, a single String argument is passed to read and other
        arguments are ignored. The collection produced by read is used as the
        input to the function.

        Example:
          @parallel
          def func( context, key[, value] ):
              pass
        """
        return self._decorator( 'parallel', *args, **kwargs )

    def combine( self, *args, **kwargs ):
        """ Decorates a function to be used as a combiner.

        Decorating a function with this method will add that function to the
        existing pipeline to process the (grouped) collection produced by the
        last DoFn. If the last collection is a PTable, it will be grouped. If
        the last collection is a PCollection, it will be grouped with null
        values.

        The collection produced by this function is a PTable.

        Example:
          @combine
          def combine_func( context, key, value ):
              pass
        """
        return self._decorator( 'combine', *args, **kwargs )

    def reduce( self, *args, **kwargs ):
        """ Decorates a function to be used as a reducer.

        Decorating a function with this method will add that function to the
        existing pipeline to process the (grouped) collection produced by the
        last DoFn. If the last collection is a PTable, it will be grouped. If
        the last collection is a PCollection, it will be grouped with null
        values.

        Example:
          @reduce
          def combine_func( context, key, value ):
              pass
        """
        return self._decorator( 'reduce', *args, **kwargs )

    # add aliases
    map = parallel
    summarize = reduce

    def _decorator( self, stage, *args, **kwargs ):
        """ Creates a decorator that adds the function as the given stage

        Because this method may be used as a decorator itself, it will check
        the args and run the decorator function if necessary.
        """
        def decorator( func ):
            # create an infected carrier
            carrier = self._carrier_for( stage, func )
            infected = infect( carrier, func )
            # add it to the pipeline
            self._add_stage( stage, func.func_name, infected )
            # return a Function; the decorated object should still be callable
            return infected

        func, others = _split_args( types.FunctionType, args )
        if func:
            # used as a decorator
            return decorator( func )
        else:
            # used as a decorator factory
            if len(others) > 0:
                self.read( others[0] )
            return decorator

    def _add_stage( self, stage, name, infected ):
        """ Adds an infected carrier instance as a stage
        """
        if stage == 'reduce':
            self.group()
            self._last_collection = self._last_collection.parallelDo( name, infected, _collection_type )
        elif stage == 'combine':
            self.group()
            self._last_collection = self._last_collection.combineValues( infected )
        else:
            self._last_collection = self._last_collection.parallelDo( name, infected, _collection_type )

        self._collections_by_name[ name ] = self._last_collection
        self._stages_by_name[ name ] = infected

        return self._last_collection

    def _carrier_for( self, stage, func ):
        """ Returns an appropriate carrier class

        This takes into account:
        * stage:
            reduce => FromGroupedTable
            combine => Combiner
        * the last collection:
            PTable => FromTable,
            PGroupedTable => FromGroupedTable
        * the arity of the function:
            3 => user expects pair input (2 + context)
            2 => user expects a single input + context
        """
        if stage == 'combine':
            return Combiner
        elif stage == 'reduce':
            return FromGroupedTable

        if isinstance( self._last_collection, PGroupedTable ):
            return FromGroupedTable
        elif isinstance( self._last_collection, PTable ):
            return FromTable
        else:
            # arity 3 => context, key, value
            if arity( func ) == 3:
                return FromTable
            else:
                return FromCollection

    def last( self ):
        """ Returns the last collection produced
        """
        return self._last_collection

    def getStage( self, name ):
        """ Returns a stage by name

        This is called by the java StandIn to get specific stages
        """
        return self._stages_by_name[ name ]

    # alias for python style
    get_stage = getStage

    def get_collection( self, name ):
        return self._collections_by_name[ name ]

def _split_args( some_type, args ):
    if len( args ) >= 1 and isinstance( args[0], some_type ):
        return args[0], args[1:]
    else:
        return None, args


_last_analytic = None

def run( analytic ):
    """ Sets the analytic that will be run

    By default, this is the default analytic or the analytic created by
    analytic( name )
    """
    _analytics.clear()
    _analytics.add( analytic )

def analytic( name ):
    """ Creates a new named analytic and sets it to be run
    """
    # TODO: in this version, multiple analytics will be backed by the same
    # pipeline and conflict with one another
    global _last_analytic
    _last_analytic = Analytic( name )
    run(_last_analytic)
    return _last_analytic

def _get_analytic():
    """ Returns the last analytic used
    """
    global _last_analytic
    if _last_analytic == None:
        # name isn't currently used, so just default it
        _last_analytic = Analytic('default')
    return _last_analytic

# decorators that use the last analytic

def parallel( *args, **kwargs ):
    return _get_analytic().parallel( *args, **kwargs )

def map( *args, **kwargs ):
    return _get_analytic().parallel( *args, **kwargs )

def combine( *args, **kwargs ):
    return _get_analytic().combine( *args, **kwargs )

def reduce( *args, **kwargs ):
    return _get_analytic().reduce( *args, **kwargs )

def summarize( *args, **kwargs ):
    return _get_analytic().reduce( *args, **kwargs )

def read( *args, **kwargs ):
    return _get_analytic().read( *args, **kwargs )

def write( *args, **kwargs ):
    return _get_analytic().write( *args, **kwargs )

# in an older version, the infected class was wrapped in a Function object like
# this. I forget why, so I'm keeping this around until the same functionality
# is reimplemented, in case it is needed.
#class Function(object):
#    def __init__( self, func, infected ):
#        self.raw_func = func
#        self.infected = infected
#
#    def name( self ):
#        return self.raw_func.func_name
#
#    # this should still be usable as a function
#    def __call__( self, *args, **kwargs ):
#        self.raw_func( *args, **kwargs )

def infect( carrier_class, func ):
    """ Returns an instance of the carrier class with the work method func
    """
    inst_class = type( func.func_name, (carrier_class,), {
            # this is where setup/teardown methods go, too
            'work': func,
            'name': func.func_name,
            '__call__': func.__call__
        } )
    return inst_class( func.func_name, _script )

def arity( func ):
    return len( inspect.getargspec( func ).args )

# helpers

# it's possible to set __getattr__ on some java classes to make configuration
# easier by aliasing lower_case_names to camelCase equivalents
#
#def getattr_camelcase():
#    pass
#
#def to_camel_case(str):
#    pass
