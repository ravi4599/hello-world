#!/bin/sh
VAR_PATH="$1"
ENV=$2

echo "VAR_PATH: ${VAR_PATH}"
echo "ENV: ${ENV}"

SRC_VAR_FILE="${VAR_PATH}/${ENV}.json"
TGT_VAR_FILE="${VAR_PATH}/variables.json"

if [ -a "${SRC_VAR_FILE}" ]; then
  echo "Copying ${SRC_VAR_FILE}"
  mv "${SRC_VAR_FILE}" "${TGT_VAR_FILE}"
else
  echo "File ${SRC_VAR_FILE} wasn't found. Nothing to copy"
fi

if [ "${ENV}" = "dev" ]; then #in case of extensions
  echo "Processing variables for ${ENV} environment"
  for i in $(airflow variables); do
    echo "Removing ${i} ..."
    airflow variables --delete "${i}"
  done
  if [ -a "${TGT_VAR_FILE}" ]; then
    echo "Importing ${TGT_VAR_FILE}"
    airflow variables import "${TGT_VAR_FILE}"
  else
    echo "File ${TGT_VAR_FILE} wasn't found. Nothing to import"
  fi
else
  echo "Skipping variables processing for ${ENV} environment"
fi

if [ -d "${VAR_PATH}" ]; then
  echo "Cleaning unnecessary files under ${VAR_PATH}"
  find "$VAR_PATH" -type f ! -name 'variables.json' -delete
else
  echo "Folder ${VAR_PATH} wasn't found"
fi