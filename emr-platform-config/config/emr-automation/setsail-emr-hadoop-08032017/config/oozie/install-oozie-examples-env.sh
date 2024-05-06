#! /bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "$RM_URI" ]; then
  export RM_URI=ip-10-3-100-145.ec2.internal:8032
fi

if [ -z "$FS_URI" ]; then
  export FS_URI=hdfs://ip-10-3-100-145.ec2.internal:8020
fi

if [ -z "$SPARK_MASTER_URL" ]; then
  export SPARK_MASTER_URL=yarn-cluster
fi

if [ -z "$HIVE_SERVER2_URL" ]; then
  export HIVE_SERVER2_URL=jdbc:hive2://ip-10-3-100-145.ec2.internal:10000/default
fi
