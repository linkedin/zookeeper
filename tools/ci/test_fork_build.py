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
import shutil
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

    def test_release_deploy_destination(self):
        pom = ET.parse(ROOT / "pom.xml")
        repository = pom.find("m:distributionManagement/m:repository", NS)
        self.assertIsNotNone(repository)
        self.assertEqual(repository.findtext("m:id", namespaces=NS), "jfrog-linkedin")
        self.assertEqual(repository.findtext("m:url", namespaces=NS),
                         "https://linkedin.jfrog.io/artifactory/zookeeper")
        self.assertIsNone(pom.find("m:distributionManagement/m:snapshotRepository", NS))

    def test_jfrog_resolution_profile(self):
        ns = {"s": "http://maven.apache.org/SETTINGS/1.1.0"}
        settings = ET.parse(ROOT / ".github/workflows/settings.xml")
        active = settings.findall("s:activeProfiles/s:activeProfile", ns)
        self.assertIn("artifactory", [profile.text for profile in active])
        profiles = settings.findall("s:profiles/s:profile", ns)
        profile = next(p for p in profiles if p.findtext("s:id", namespaces=ns) == "artifactory")
        for path in ("s:repositories/s:repository", "s:pluginRepositories/s:pluginRepository"):
            repository = profile.find(path, ns)
            self.assertIsNotNone(repository)
            self.assertEqual(repository.findtext("s:id", namespaces=ns), "jfrog-linkedin")
            self.assertEqual(repository.findtext("s:url", namespaces=ns),
                             "https://linkedin.jfrog.io/artifactory/open-source")
            self.assertEqual(repository.findtext("s:snapshots/s:enabled", namespaces=ns), "false")

    def invoke_version_script(self, arguments, expected_version):
        sed = shutil.which("gsed") or shutil.which("sed")
        self.assertIsNotNone(sed, "GNU sed is required for the branch-3.6 release script")
        probe = subprocess.run([sed, "--version"], text=True, capture_output=True)
        self.assertIn("GNU sed", probe.stdout, "Run on Linux or put GNU sed on PATH")
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "sed").symlink_to(sed)
            (work / "module").mkdir()
            fixture = (
                '<project xmlns="{}">\n'
                '  <groupId>{}</groupId>\n'
                '  <artifactId>fixture</artifactId>\n'
                '  <version>3.8.7</version>\n'
                '  <properties><external.version>1.2.3</external.version></properties>\n'
                '</project>\n'
            ).format(NS["m"], GROUP)
            for path in ("pom.xml", "module/pom.xml"):
                (work / path).write_text(fixture)
            env = dict(os.environ, PATH=str(work) + os.pathsep + os.environ["PATH"])
            result = subprocess.run(
                ["sh", str(ROOT / ".github/workflows/version-upgrade.sh")] + arguments,
                cwd=work, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            for path in ("pom.xml", "module/pom.xml"):
                pom = ET.parse(work / path)
                self.assertEqual(pom.findtext("m:version", namespaces=NS), expected_version, path)
                self.assertEqual(pom.findtext("m:properties/m:external.version", namespaces=NS), "1.2.3")

    def test_release_version_inferred_from_pom(self):
        self.invoke_version_script(["3.8.7-12"], "3.8.7-12")

    def test_release_version_explicit_old_and_new(self):
        self.invoke_version_script(["3.8.7", "3.8.7-12"], "3.8.7-12")

    def test_legacy_version_names_are_not_restricted(self):
        for version in ("3.8.7-rc1", "3.8.7-SNAPSHOT", "3.8.7"):
            with self.subTest(version=version):
                self.invoke_version_script([version], version)


if __name__ == "__main__":
    unittest.main()
