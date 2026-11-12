# Apache ZooKeeper [![GitHub Actions CI][ciBadge]][ciLink] [![Travis CI][trBadge]][trLink] [![Maven Central][mcBadge]][mcLink] [![License][liBadge]][liLink]

<p align="left">
  <a href="https://zookeeper.apache.org/">
    <img src="https://zookeeper.apache.org/images/zookeeper_small.gif"" alt="https://zookeeper.apache.org/"><br/>
  </a>
</p>

For the latest information about Apache ZooKeeper, please visit our website at:

   https://zookeeper.apache.org

and our wiki, at:

   https://cwiki.apache.org/confluence/display/ZOOKEEPER

## Packaging/release artifacts

Either downloaded from https://zookeeper.apache.org/releases.html or
found in zookeeper-assembly/target directory after building the project with maven.

    apache-zookeeper-[version].tar.gz

        Contains all the source files which can be built by running:
        mvn clean install

        To generate an aggregated apidocs for zookeeper-server and zookeeper-jute:
        mvn javadoc:aggregate
        (generated files will be at target/site/apidocs)

    apache-zookeeper-[version]-bin.tar.gz

        Contains all the jar files required to run ZooKeeper
        Full documentation can also be found in the docs folder

As of version 3.5.5, the parent, zookeeper and zookeeper-jute artifacts
are deployed to the central repository after the release
is voted on and approved by the Apache ZooKeeper PMC:

  https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper

## Java 8

If you are going to compile with Java 1.8, you should use a
recent release at u211 or above.

## LinkedIn fork builds

This fork builds Maven artifacts under `com.linkedin.zookeeper`. Java package
names remain `org.apache.zookeeper`. The binary assembly keeps the
`apache-zookeeper-[version]-bin.tar.gz` layout expected by the launch scripts.

Run `python3 tools/ci/test_fork_build.py` to check the fork's Maven coordinates
and release-version validation. Build locally with `mvn clean verify`; this does
not publish artifacts. The upstream dependency versions and test suites are
retained.

Publishing uses `.github/workflows/ci-jfrog-workflow.yml` on a published,
non-prerelease GitHub release in `linkedin/zookeeper`. The tag must be the
checked-out base version followed by a positive LinkedIn release number
(for example, `3.8.8-1`). The version script updates reactor versions using
Maven, rather than replacing dependency versions with text substitution.
The workflow uses the existing `MVN_DEPLOY_JFROG_USER` and
`MVN_DEPLOY_JFROG_KEY` repository secrets. Ordinary pushes and pull requests
do not publish artifacts.

# Contributing
We always welcome new contributors to the project! See [How to Contribute](https://cwiki.apache.org/confluence/display/ZOOKEEPER/HowToContribute) for details on how to submit patches as pull requests and other aspects of our contribution workflow.


[ciBadge]: https://github.com/apache/zookeeper/workflows/CI/badge.svg
[ciLink]: https://github.com/apache/zookeeper/actions
[liBadge]: https://img.shields.io/github/license/apache/zookeeper?color=282661
[liLink]: https://github.com/apache/zookeeper/blob/master/LICENSE.txt
[mcBadge]: https://img.shields.io/maven-central/v/org.apache.zookeeper/zookeeper
[mcLink]: https://zookeeper.apache.org/releases
[trBadge]: https://travis-ci.org/apache/zookeeper.svg?branch=master
[trLink]: https://travis-ci.org/apache/zookeeper
