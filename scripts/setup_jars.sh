#!/usr/bin/env bash
# Download required JARs for PySpark S3A support (hadoop-aws + AWS SDK v2 bundle).
# Run this once after cloning the repo.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/../jars"

mkdir -p "$JARS_DIR"

HADOOP_AWS_VERSION="3.4.2"
AWS_SDK_V2_VERSION="2.29.51"

echo "Downloading hadoop-aws-${HADOOP_AWS_VERSION}.jar..."
curl -sL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o "${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

echo "Downloading aws-sdk-bundle-${AWS_SDK_V2_VERSION}.jar (~612MB, please wait)..."
curl -sL "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar" \
    -o "${JARS_DIR}/bundle-${AWS_SDK_V2_VERSION}.jar"

echo "JARs downloaded to ${JARS_DIR}/"
ls -lh "${JARS_DIR}/"