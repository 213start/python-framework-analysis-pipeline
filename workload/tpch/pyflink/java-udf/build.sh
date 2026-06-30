#!/usr/bin/env bash
# Build FlinkDemo JAR containing PreUDF and PostUDF.
#
# Prerequisites:
#   - JDK 17+ (javac on PATH)
#   - FLINK_HOME set, or flink-table-api-java-uber JAR available
#
# Usage:
#   ./build.sh                    # build with auto-detected Flink JARs
#   FLINK_HOME=/opt/flink ./build.sh  # explicit Flink location
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
JAR_NAME="FlinkDemo-1.0-SNAPSHOT.jar"
OUTPUT_JAR="$SCRIPT_DIR/$JAR_NAME"

# --- Find Flink dependency JARs ---
# Search order: FLINK_HOME/lib, FLINK_HOME, PyFlink lib, /opt/flink/lib
FLINK_CP=""
for search_dir in \
    "${FLINK_HOME:-}/lib" \
    "${FLINK_HOME:-}" \
    "$(python3 -c 'import pyflink,os; print(os.path.join(os.path.dirname(pyflink.__file__),"lib"))' 2>/dev/null)" \
    "/opt/flink/lib"
do
    if [ -d "$search_dir" ]; then
        found=$(ls "$search_dir"/flink-table-api-java-uber-*.jar 2>/dev/null | head -1)
        if [ -n "$found" ]; then
            FLINK_CP="$search_dir/flink-table-api-java-uber-*.jar:$search_dir/flink-dist-*.jar"
            echo "Found Flink JARs in: $search_dir"
            break
        fi
    fi
done

if [ -z "${FLINK_CP:-}" ]; then
    echo "ERROR: Cannot find Flink JARs. Set FLINK_HOME or run inside a Flink container."
    exit 1
fi

# Expand globs
FLINK_CP_EXPANDED=$(echo "$FLINK_CP" | tr ':' '\n' | while read -r pattern; do ls $pattern 2>/dev/null; done | paste -sd: -)

if [ -z "$FLINK_CP_EXPANDED" ]; then
    echo "ERROR: No Flink JARs found matching: $FLINK_CP"
    exit 1
fi

echo "Using classpath: $FLINK_CP_EXPANDED"

# --- Compile ---
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "Compiling Java UDFs..."
javac -cp "$FLINK_CP_EXPANDED" -d "$BUILD_DIR" \
    "$SCRIPT_DIR/PreUDF.java" \
    "$SCRIPT_DIR/PostUDF.java"

# --- Package ---
echo "Packaging $JAR_NAME..."
(cd "$BUILD_DIR" && jar cf "$OUTPUT_JAR" com/)

rm -rf "$BUILD_DIR"

echo "Built: $OUTPUT_JAR"
echo "Done."
