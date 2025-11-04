#!/bin/bash

# Function to print error messages and exit
exit_with_error() {
    local message="$1"
    local code="$2"
    echo "ERROR: $message" >&2
    exit "$code"
}

# --- 1. Take 2 args: inputFolder and outputFolder ---
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <inputFolder> <outputFolder>"
    exit_with_error "Incorrect number of arguments provided." 1
fi

inputFolder="$1"
outputFolder="$2"
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
JAR_PATH=""

echo "Starting Hadoop job script..."
echo "Input folder (HDFS): $inputFolder"
echo "Output folder (HDFS): $outputFolder"
echo "Script directory: $SCRIPT_DIR"

# --- 2. Check for BigData1.jar in the same folder as script ---
if [ -f "${SCRIPT_DIR}/BigData1.jar" ]; then
    JAR_PATH="${SCRIPT_DIR}/BigData1.jar"
    echo "Found BigData1.jar: ${JAR_PATH}"
else
    echo "BigData1.jar not found in script directory."
    # --- 3. If not, check if there is *any* jar in the folder. If a jar is found ask user to confirm it's execution ---
    candidate_jars=($(find "${SCRIPT_DIR}" -maxdepth 1 -name "*.jar"))
    if [ "${#candidate_jars[@]}" -eq 1 ]; then
        read -p "Found a single JAR file: ${candidate_jars[0]}. Do you want to use it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            JAR_PATH="${candidate_jars[0]}"
            echo "Using ${JAR_PATH} as confirmed by user."
        else
            exit_with_error "User declined to use the found JAR. Exiting." 2
        fi
    elif [ "${#candidate_jars[@]}" -gt 1 ]; then
        echo "Multiple JAR files found in the directory. Please specify which one to use by renaming it to BigData1.jar or provide a single JAR."
        echo "Found JARs:"
        for jar in "${candidate_jars[@]}"; do
            echo "  - $jar"
        done
        exit_with_error "Ambiguous JAR file. Exiting." 2
    else
        # --- 4. If no jar print error message and exit with error code 2 ---
        exit_with_error "No JAR file found in the script directory." 2
    fi
fi


# Make the JAR_PATH available as an environment variable
export HIVE_UDF_JAR_PATH="${JAR_PATH}"
echo "Exported HIVE_UDF_JAR_PATH=${HIVE_UDF_JAR_PATH}"


# Determine the main class from the JAR (assuming it's still org.example.Main)
# In a more robust script, you might extract this from MANIFEST.MF or take it as an argument.
MAIN_CLASS="org.example.Main"
echo "Main class: $MAIN_CLASS"


# --- 5. Check if there is data in hdfs at inputFolder ---
echo "Checking for input data in HDFS at: $inputFolder"
if ! hdfs dfs -ls "$inputFolder" > /dev/null 2>&1; then
    exit_with_error "Input folder '$inputFolder' does not exist or is empty in HDFS." 3
fi
# Further check if it actually contains files (beyond just directory existence)
if [ "$(hdfs dfs -ls "$inputFolder" | wc -l)" -lt 2 ]; then # wc -l will be 1 for just the dir itself, so >1 for content
    exit_with_error "Input folder '$inputFolder' exists but appears to be empty in HDFS." 3
fi
echo "Input data found in HDFS."

# --- 6. Check if outputFolder exists; if yes delete it and print a message ---
echo "Checking for existing output folder in HDFS at: $outputFolder"
if hdfs dfs -test -d "$outputFolder"; then
    echo "Output folder '$outputFolder' already exists in HDFS. Deleting it..."
    if ! hdfs dfs -rm -r "$outputFolder"; then
        exit_with_error "Failed to delete existing output folder '$outputFolder'." 1
    fi
    echo "Existing output folder deleted."
fi

# --- 7. Run the jar via `hadoop jar` ---
echo "Running Hadoop job..."
# Use the determined JAR_PATH, MAIN_CLASS, and the provided input/output folders
# Remember that ToolRunner for `hadoop jar` puts the main class in args[0]
# so input is args[1] and output is args[2] in the Java code.
if ! hadoop jar "${JAR_PATH}" "${MAIN_CLASS}" "$inputFolder" "$outputFolder"; then
    # --- 8. All other exceptions: exit with code 1 ---
    exit_with_error "Hadoop job failed during execution." 1
fi

echo "Hadoop job completed successfully!"
echo "Output is available in HDFS at: $outputFolder"
exit 0