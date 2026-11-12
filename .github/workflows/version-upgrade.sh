#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eu

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <base-version>-<positive-release-number> (run from repository root)" >&2
    exit 1
fi

python3 - "$1" <<'PY'
import re
import sys
import xml.etree.ElementTree as ET

ns = {"m": "http://maven.apache.org/POM/4.0.0"}
pom = ET.parse("pom.xml").getroot()
base = pom.findtext("m:version", namespaces=ns)
group = pom.findtext("m:groupId", namespaces=ns)
if group != "com.linkedin.zookeeper":
    sys.exit("Refusing to version a non-LinkedIn reactor")
if base is None or re.fullmatch(re.escape(base) + r"-[1-9][0-9]*", sys.argv[1]) is None:
    sys.exit("Release tag must match the checked-out base version: {}-<positive-release-number>".format(base))
PY

exec mvn -B -ntp org.codehaus.mojo:versions-maven-plugin:2.16.2:set \
    "-DnewVersion=$1" -DprocessAllModules=true -DgenerateBackupPoms=false
