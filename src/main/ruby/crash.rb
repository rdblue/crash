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

# ensure the lib directory is in the search path
require 'pathname'
lib_path = Pathname.new(__FILE__).expand_path.parent
$LOAD_PATH << lib_path.to_s unless $LOAD_PATH.include? lib_path.to_s

module Crunch
  java_import 'org.apache.crunch.PCollection'
  java_import 'org.apache.crunch.PTable'
  java_import 'org.apache.crunch.PGroupedTable'
  java_import 'org.apache.crunch.types.avro.Avros'
end

module Crash
  module Carriers
    java_import 'com.cloudera.crash.carriers.FromCollection'
    java_import 'com.cloudera.crash.carriers.FromTable'
    java_import 'com.cloudera.crash.carriers.FromGroupedTable'
    java_import 'com.cloudera.crash.carriers.Combiner'
    java_import 'com.cloudera.crash.utils.ToTableShim'
  end

  module Misc
    java_import 'com.cloudera.crash.Analytic'
  end

  java_import 'com.cloudera.crash.generics.CustomData'

  GENERIC_TYPE = Crunch::Avros.generics(CustomData::GENERIC_SCHEMA);
  COLLECTION_TYPE = GENERIC_TYPE
  TABLE_TYPE = Crunch::Avros.table_of(GENERIC_TYPE, GENERIC_TYPE)

  class Analytic
    # implement the Analytic interface so individual stages can be retrieved
    include Misc::Analytic

    attr_reader :pipeline

    # for now, source and sink should be String filenames because the
    # implementation is using read_text_file and write_text_file
    def initialize( name, options={}, &block )
      @name = name
      @pipeline = $SCRIPT.get_pipeline

      @collections_by_name = {}
      @stages_by_name = {}

      instance_exec( &block ) if block_given?
    end

    def parallel( name, options={}, &block )
      if options[:from]
        read options[:from]
      end
      return add_stage( :parallel, name, &block )
    end
    alias_method :map, :parallel

    def reduce( name, options={}, &block )
      return add_stage( :reduce, name, &block )
    end
    alias_method :summarize, :reduce

    def combine( name, options={}, &block )
      return add_stage( :combine, name, &block )
    end

    def read( source )
      @last_collection = @pipeline.read_text_file( source )
      @collections_by_name[ source ] = @last_collection
      return @last_collection
    end

    def write( sink )
      @pipeline.write_text_file( @last_collection, sink )
      nil
    end

    def verbose!
      @pipeline.enable_debug
    end

    def getStage( name )
      return @stages_by_name[ name ]
    end
    alias_method :stage, :getStage

    private

    def add_stage( stage, name, &block )
      raise RuntimeError, 'A block is required' unless block_given?

      infected = infect( name, carrier_for( stage, block ), &block )

      case stage
      when :reduce
        group!
        @last_collection = @last_collection.parallel_do( name, infected, COLLECTION_TYPE )
      when :combine
        group!
        @last_collection = @last_collection.combine_values( infected )
      else
        @last_collection = @last_collection.parallel_do( name, infected, COLLECTION_TYPE )
      end

      @collections_by_name[ name ] = @last_collection
      @stages_by_name[ name ] = infected

      return @last_collection
    end

    def infect( name, carrier_class, &block )
      $stderr.puts "Instantiating #{carrier_class} for #{name}"

      inst_class = Class.new( carrier_class ) do
        define_method( :work, &block )
      end

      # it is possible to name the class by assigning it to a constant here

      return inst_class.new( name, $SCRIPT )
    end

    def group!
      case @last_collection
      when Crunch::PGroupedTable
        # do nothing
      when Crunch::PTable
        # already a table, just need to group
        @last_collection = @last_collection.group_by_key # TODO: add options
      when Crunch::PCollection
        # need to get a PTable then group
        @last_collection = @last_collection.parallel_do( Carriers::ToTableShim.new, TABLE_TYPE )
        @last_collection = @last_collection.group_by_key # TODO: add options
      else
        throw RuntimeError.new('Last collection is invalid')
      end
    end

    def carrier_for( stage, block )
      case stage
      when :combine
        return Carriers::Combiner
      when :reduce
        return Carriers::FromGroupedTable
      end

      case @last_collection
      when Crunch::PGroupedTable
        return Carriers::FromGroupedTable
      when Crunch::PTable
        return Carriers::FromTable
      when Crunch::PCollection
        # the arity gives us an idea of whether the last was written as a table
        # or as a collection
        if block.arity == 2
          return Carriers::FromTable
        else
          return Carriers::FromCollection
        end
      end

      throw RuntimeError.new('Last collection is invalid')
    end

    # where do before/after blocks go?
    # before/after blocks will be registered to a name, which can come collect
    # them from the analytic when it is time to run them
  end
end

def analytic( name, options={}, &block )
  return Crash::Analytic.new( name, options, &block )
end

def bananalytic( name, options={}, &block )
  puts " _"
  puts "//\\"
  puts "V  \\"
  puts " \\  \\_"
  puts "  \\,'.`-.          BANANALYTICS!"
  puts "   |\\ `. `."
  puts "   ( \\  `. `-.                        _,.-:\\"
  puts "    \\ \\   `.  `-._             __..--' ,-';/"
  puts "     \\ `.   `-.   `-..___..---'   _.--' ,'/"
  puts "      `. `.    `-._        __..--'    ,' /"
  puts "        `. `-_     ``--..''       _.-' ,'"
  puts "          `-_ `-.___        __,--'   ,'"
  puts '             `-.__  `----"""    __.-\''
  puts "                  `--..____..--'"
  return analytic( name, options, &block )
end
