#!/bin/sh
BASE_PATH=$1
ENV=$2

echo "BASE_PATH: ${BASE_PATH}"
echo "ENV: ${ENV}"

for project in "${BASE_PATH}"/*; do
  if [ -d "${project}" ]; then
    ENV_FILE="${project}/${ENV}.yml"
    if [ -a "${ENV_FILE}" ]; then
      echo "Copying ${ENV_FILE}"
      cp -rf "${project}/${ENV}.yml" "${project}/conf.yml"
    else
      echo "${ENV_FILE} wasn't found"
    fi
    echo "Cleaning unnecessary files under ${project}"
    find "${project}" -type f ! -name '*.py' -a ! -name "conf.yml" -a ! -name "*.pyc" -delete
  fi
done

if grep -q "${BASE_PATH}" ~/.bashrc; then
  echo "PATH UPDATED"
else
  echo "Updating PATH"
  echo "PATH=${PATH}:${BASE_PATH}/global_utils" >>~/.bashrc
fi
