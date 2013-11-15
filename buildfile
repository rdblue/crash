# 
# Copyright 2013 Cloudera Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "com.cloudera.crash"
COPYRIGHT = "Copyright 2013 Cloudera Inc."

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << 'https://repository.cloudera.com/artifactory/cloudera-repos/'
repositories.remote << "http://repo1.maven.org/maven2"

desc 'Crash: a JRuby API for map/reduce'
define 'crash' do
  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  manifest['Main-Class'] = 'com.cloudera.crash.Main'

  compile.options.lint = 'all'
  compile.with transitive('org.apache.crunch:crunch-core:jar:0.7.0')
  # this is temporary, while it requires a patched version of crunch
  # set this to wherever the patched crunch-core jar is located
  compile.with download(
      artifact('org.apache.crunch:crunch-core:jar:0.8.0-SNAPSHOT') =>
      'file:///home/blue/.m2/repository/org/apache/crunch/crunch-core/0.8.0-SNAPSHOT/crunch-core-0.8.0-SNAPSHOT.jar'
    )
  compile.with 'com.google.guava:guava:jar:11.0.2'
  compile.with 'org.jruby:jruby-complete:jar:1.7.4'
  compile.with 'org.python:jython-standalone:jar:2.7-b1'

  # add src/main/ruby as a resources directory
  resources.from _('src/main/ruby')
  resources.from _('src/main/python')

  package(:jar)
end
