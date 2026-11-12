#!/usr/bin/env python3

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

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
GROUP = "com.linkedin.zookeeper"


class ForkBuildTest(unittest.TestCase):
    def test_reactor_coordinates(self):
        paths = subprocess.check_output(
            ["git", "ls-files", "*pom.xml"], cwd=ROOT, text=True).splitlines()
        self.assertGreater(len(paths), 1)
        for path in paths:
            pom = ET.parse(ROOT / path).getroot()
            parents = {child: parent for parent in pom.iter() for child in parent}
            for group in pom.findall(".//m:groupId", NS):
                if group.text == "org.apache.zookeeper":
                    self.assertEqual(parents[group].tag, "{" + NS["m"] + "}exclusion", path)
            if path != "pom.xml":
                self.assertEqual(pom.findtext("m:parent/m:groupId", namespaces=NS), GROUP, path)

    def test_curator_excludes_upstream_artifact(self):
        pom = ET.parse(ROOT / "zookeeper-compatibility-tests/zookeeper-compatibility-tests-curator/pom.xml")
        exclusions = pom.findall(".//m:exclusion/m:groupId", NS)
        self.assertEqual([group.text for group in exclusions], ["org.apache.zookeeper"] * 2)

    def test_assembly_is_deployable(self):
        pom = ET.parse(ROOT / "zookeeper-assembly/pom.xml")
        deploy = [plugin for plugin in pom.findall(".//m:plugin", NS)
                  if plugin.findtext("m:artifactId", namespaces=NS) == "maven-deploy-plugin"]
        self.assertEqual(len(deploy), 1)
        self.assertEqual(deploy[0].findtext("m:configuration/m:skip", namespaces=NS), "false")
        assembly = (ROOT / "zookeeper-assembly/src/main/assembly/bin-package.xml").read_text()
        self.assertNotIn("<exclude>org.apache.zookeeper:", assembly)
        for artifact in ("zookeeper-client", "zookeeper-docs", "zookeeper-recipes"):
            self.assertIn("<exclude>{}:{}</exclude>".format(GROUP, artifact), assembly)

    def test_deploy_destinations(self):
        pom = ET.parse(ROOT / "pom.xml")
        for kind in ("repository", "snapshotRepository"):
            repository = pom.find("m:distributionManagement/m:" + kind, NS)
            self.assertIsNotNone(repository)
            self.assertEqual(repository.findtext("m:id", namespaces=NS), "jfrog-linkedin")
            self.assertEqual(repository.findtext("m:url", namespaces=NS),
                             "https://linkedin.jfrog.io/artifactory/zookeeper")

    def invoke_version_script(self, arguments, group=GROUP):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "pom.xml").write_text(
                '<project xmlns="{}"><groupId>{}</groupId><version>3.8.8</version></project>'.format(
                    NS["m"], group))
            executable = work / "mvn"
            executable.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > maven-arguments.txt\n')
            executable.chmod(0o755)
            env = dict(os.environ, PATH=str(work) + os.pathsep + os.environ["PATH"])
            result = subprocess.run(
                ["sh", str(ROOT / ".github/workflows/version-upgrade.sh")] + arguments,
                cwd=work, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            log = work / "maven-arguments.txt"
            return result, log.read_text().splitlines() if log.exists() else None

    def test_valid_release_uses_maven_versioning(self):
        result, arguments = self.invoke_version_script(["3.8.8-12"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(arguments, [
            "-B", "-ntp", "org.codehaus.mojo:versions-maven-plugin:2.16.2:set",
            "-DnewVersion=3.8.8-12", "-DprocessAllModules=true", "-DgenerateBackupPoms=false"])

    def test_invalid_release_is_rejected_before_maven(self):
        for arguments in ([], ["3.8.8-1", "extra"], ["3.8.7-1"], ["3.8.8"],
                          ["3.8.8-0"], ["3.8.8-01"], ["3.8.8-SNAPSHOT"],
                          ["3.8.8-1\n"], ["3.8.8-1;echo unexpected"], ["../3.8.8-1"]):
            with self.subTest(arguments=arguments):
                result, invoked = self.invoke_version_script(arguments)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(invoked)
                self.assertTrue(result.stderr)

    def test_apache_coordinates_are_not_published(self):
        result, invoked = self.invoke_version_script(["3.8.8-1"], "org.apache.zookeeper")
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(invoked)


if __name__ == "__main__":
    unittest.main()
