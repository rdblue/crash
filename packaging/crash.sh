#!/bin/sh
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

_PROJECT_DIR=$(cd "$(dirname "$0")/.."; pwd)
_VENDOR="$_PROJECT_DIR/vendor"
_HADOOP=$(which hadoop)

if [ -n "$_HADOOP" ] ; then
  _HADOOP_CLASSPATH=`hadoop classpath`
  echo "Running in cluster mode"
  java -cp $_HADOOP_CLASSPATH:"$_VENDOR/*" com.cloudera.crash.Main --vendor $_VENDOR $@
else
  echo "Running in standalone mode"
  java -cp "$_VENDOR/*" -Dmem.pipeline=true com.cloudera.crash.Main $@
fi
