# coding: utf-8
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
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'crash/version'

Gem::Specification.new do |spec|
  spec.name          = "crash"
  spec.version       = Crash::VERSION
  spec.platform      = "java"
  spec.authors       = ["Ryan Blue"]
  spec.email         = ["rblue@cloudera.com"]
  spec.description   = "A prototype ruby DSL built on crunch"
  spec.summary       = "Crash!"
  spec.homepage      = "http://github.com/cloudera/crash"
  spec.license       = "Apache 2.0"

  spec.files         = Dir["bin/*"] + Dir["lib/**/*.rb"] + Dir["vendor/*.jar"]
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  # jbundler is used to download and vendor dependency jars
  spec.add_development_dependency 'jbundler', '~> 0.4'
end
