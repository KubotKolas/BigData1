#!/bin/bash

# --- Configuration ---
HIVE_HQL_FILENAME="hive.hql" # Expected default HQL script name
BEELINE_ARGS="-u jdbc:hive2://localhost:10000 -n hive -p ''" # Default beeline connection args, adjust as needed

# --- Functions ---

# Function to display error messages and exit
error_exit() {
    echo "ERROR: $1" >&2
    exit "$2"
}

# Function to check if an HDFS path exists and is not empty
check_hdfs_input() {
    local path="$1"
    local path_var_name="$2"

    echo "Checking HDFS input directory: $path_var_name=$path"
    if ! hdfs dfs -test -e "$path"; then
        error_exit "HDFS path for $path_var_name does not exist: $path" 2
    fi
    if ! hdfs dfs -ls "$path" | grep -q '^-'; then # Check for at least one file, excluding directories
        error_exit "HDFS path for $path_var_name exists but is empty: $path" 2
    fi
    echo "HDFS input directory $path_var_name is valid."
}

# --- Main Script Logic ---

# 1. Argument Parsing
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_dir_1> <input_dir_2> <output_dir>"
    echo "  <input_dir_1>: HDFS path for MR output (e.g., /user/data/mr_output)"
    echo "  <input_dir_2>: HDFS path for CSV data (e.g., /user/data/teams_csv)"
    echo "  <output_dir>: HDFS path for JSON output (e.g., /user/data/json_summary)"
    exit 1
fi


# # Check for HIVE_UDF_JAR_PATH environment variable
# if [ -z "$HIVE_UDF_JAR_PATH" ]; then
#     error_exit "Environment variable HIVE_UDF_JAR_PATH is not set. This is required for the Hive UDF JAR." 3
# fi
# echo "HIVE UDF JAR Path (from env var): $HIVE_UDF_JAR_PATH"

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

echo "Looking for the jar containing UDF with JSON conversion"
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

HIVE_UDF_JAR_PATH=$JAR_PATH


MR_OUTPUT_HDFS_PATH="$1"
TEAMS_CSV_HDFS_PATH="$2"
JSON_OUTPUT_HDFS_PATH="$3"

# 2. Find HQL Script
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
HQL_SCRIPT=""

if [ -f "${SCRIPT_DIR}/${HIVE_HQL_FILENAME}" ]; then
    HQL_SCRIPT="${SCRIPT_DIR}/${HIVE_HQL_FILENAME}"
    echo "Found default HQL script: ${HQL_SCRIPT}"
else
    # Find all .hql files in the directory
    HQL_FILES=($(find "$SCRIPT_DIR" -maxdepth 1 -name "*.hql" -print0 | xargs -0))
    NUM_HQL_FILES=${#HQL_FILES[@]}

    if [ "$NUM_HQL_FILES" -eq 1 ]; then
        HQL_SCRIPT="${HQL_FILES[0]}"
        read -p "Found single HQL script '${HQL_SCRIPT}'. Do you want to run it? (y/n): " confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            error_exit "User opted not to run the script." 1
        fi
        echo "Running: ${HQL_SCRIPT}"
    elif [ "$NUM_HQL_FILES" -gt 1 ]; then
        echo "Multiple HQL scripts found in '${SCRIPT_DIR}':"
        for f in "${HQL_FILES[@]}"; do
            echo "  - $(basename "$f")"
        done
        error_exit "Please ensure only one HQL script or a '${HIVE_HQL_FILENAME}' script is present, or specify which to run." 1
    else
        error_exit "No HQL script found in '${SCRIPT_DIR}'. Looking for '${HIVE_HQL_FILENAME}' or any '*.hql' file." 1
    fi
fi

# 3. Check Input Directories
check_hdfs_input "$MR_OUTPUT_HDFS_PATH" "input_dir_1 (MR output)"
check_hdfs_input "$TEAMS_CSV_HDFS_PATH" "input_dir_2 (CSV data)"

# 4. Check/Manage Output Directory
echo "Checking HDFS output directory: $JSON_OUTPUT_HDFS_PATH"
if hdfs dfs -test -e "$JSON_OUTPUT_HDFS_PATH"; then
    if hdfs dfs -ls "$JSON_OUTPUT_HDFS_PATH" | grep -q '^-'; then
        echo "WARNING: HDFS output directory '$JSON_OUTPUT_HDFS_PATH' exists and is not empty. Clearing contents."
        hdfs dfs -rm -r -skipTrash "${JSON_OUTPUT_HDFS_PATH}/*" || error_exit "Failed to clear output directory contents." 2
    else
        echo "HDFS output directory '$JSON_OUTPUT_HDFS_PATH' exists and is empty."
    fi
else
    echo "HDFS output directory '$JSON_OUTPUT_HDFS_PATH' does not exist. Creating it."
    hdfs dfs -mkdir -p "$JSON_OUTPUT_HDFS_PATH" || error_exit "Failed to create output directory." 2
fi
echo "HDFS output directory is ready."

# 5. Run Beeline
echo "Starting Beeline job with HQL script: ${HQL_SCRIPT}"
echo "MR Output Location: ${MR_OUTPUT_HDFS_PATH}"
echo "Teams CSV Location: ${TEAMS_CSV_HDFS_PATH}"
echo "JSON Output Location: ${JSON_OUTPUT_HDFS_PATH}"

beeline ${BEELINE_ARGS} \
  --hiveconf mr_output_location="${MR_OUTPUT_HDFS_PATH}" \
  --hiveconf teams_csv_location="${TEAMS_CSV_HDFS_PATH}" \
  --hiveconf json_output_location="${JSON_OUTPUT_HDFS_PATH}" \
  --hiveconf udf_jar_path="${HIVE_UDF_JAR_PATH}" \
  -f "$HQL_SCRIPT"

BEELINE_EXIT_CODE=$?

if [ "$BEELINE_EXIT_CODE" -eq 0 ]; then
    echo "Beeline job completed successfully."
else
    error_exit "Beeline job failed with exit code $BEELINE_EXIT_CODE." "$BEELINE_EXIT_CODE"
fi

exit 0