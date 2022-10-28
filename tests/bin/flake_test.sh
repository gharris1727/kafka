#!/usr/bin/env sh

set -eu -o pipefail

# Script to run junit tests inside of a limited-cpu docker environment
# instead of natively on the host machine via gradle
# Requirements:
# 1. Docker to be running in the background
# 2. The following gradle task to be added to your build file in the allprojects stanza
#  task testClasspath {
#    doLast {
#      def classes = new ArrayList<String>();
#      classes.add(sourceSets.main.output.classesDirs.asPath);
#      classes.add(sourceSets.test.output.classesDirs.asPath);
#      classes.addAll(configurations.testRuntimeClasspath);
#      print classes.join(":")
#    }
#  }
# 3. This test dependency for each subproject you wish to test:
# org.junit.platform:junit-platform-console-standalone

# Arguments
# e.g. core
gradleProject="$1"
# e.g. org.example.TestClass#testMethod
testMethod="$2"
# e.g. 0.1
cpus="$3"

# Container locations
pwdDir="/opt/dir"
javaCmd="/opt/java/openjdk/bin/java"
userDir="/root"

# Volumes to mount to the docker container
pwdVolume="$(pwd):$pwdDir" # For the repository
gradleVolume="$HOME/.gradle:$userDir/.gradle" # for cached gradle artifacts

# Step 1: Prepare for the test on the native machine with unrestricted CPU
echo "compiling test classes" >&2
time ./gradlew $gradleProject:jar $gradleProject:testClasses
classpath="$(./gradlew $gradleProject:testClasspath -q | sed -e "s:$gradleVolume:g" -e "s:$pwdVolume:g")"

# Step 2: Run the test inside of a docker container with restricted CPU
echo "running test" >&2
junitOptions="--disable-banner --details=verbose --config=junit.platform.output.capture.stdout=true"
cmd="cd $pwdDir && time $javaCmd -cp \"$classpath\" org.junit.platform.console.ConsoleLauncher $junitOptions -m $testMethod"
time docker run --cpus="$cpus" --rm -it -v "$gradleVolume" -v "$pwdVolume" adoptopenjdk/openjdk11 bash -c "$cmd"
