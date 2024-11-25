from global_utils import global_confs, load_azure_kv_confs
import requests
import json
import time
import logging

azure_confs = load_azure_kv_confs()

new_cluster_automated = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 32
    },
    "spark_version": "7.3.x-scala2.12",
    "spark_conf": {

        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.io.cache.maxMetaDataCache": "1g",
        "spark.databricks.io.cache.maxDiskUsage": "50g"
    },
    "node_type_id": "Standard_E16s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
    "UseCase": "UDP 2",
    "Team": "DATA ENG",
    "Env": "Prod"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": "true",
    "init_scripts": [

        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisHiveMetastoreDBR7.0/DevTestHiveMetastore.sh"
            }
        }
    ]
}

E2E_ARG_cluster_automated  = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 32
    },
    "spark_version": "7.3.x-scala2.12",
    "spark_conf": {

        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.io.cache.maxMetaDataCache": "1g",
        "spark.databricks.io.cache.maxDiskUsage": "50g"
    },
    "node_type_id": "Standard_E32s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
    "UseCase": "UDP 2",
    "Team": "DATA ENG",
    "Env": "Prod"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": "true",
    "init_scripts": [

        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisHiveMetastoreDBR7.0/DevTestHiveMetastore.sh"
            }
        }
    ]
}






altametrics_cluster_automated = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 32
    },
    "spark_version": "7.3.x-scala2.12",
    "spark_conf": {

        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.io.cache.maxMetaDataCache": "1g",
        "spark.databricks.io.cache.maxDiskUsage": "50g"
    },
    "node_type_id": "Standard_E16s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
    "UseCase": "UDP 2",
    "Team": "DATA ENG",
    "Env": "Prod"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": "true",
    "init_scripts": [

        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisHiveMetastoreDBR7.0/DevTestHiveMetastore.sh"
            }
        },
        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script_2')
            }
        }
    ]
}



new_cluster_automated_extended = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 32
    },
    "spark_version": "6.6.x-scala2.11",
    "spark_conf": {

        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.databricks.pyspark.enableProcessIsolation": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true"
    },
    "node_type_id": "Standard_E16s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
    "UseCase": "UDP 2",
    "Team": "DATA ENG",
    "Env": "Prod"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": "true",
    "init_scripts": [

        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisClusterInitScripts/DevTestHiveMetastore.sh"
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisClusterInitScripts/newrelic_install.sh"
            }
        },
    ]
}

customer_new_cluster_automated = {
        "autoscale": {
        "min_workers": 1,
        "max_workers": 32
        },
        "spark_version": "7.3.x-scala2.12",
        "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1" ,
        "spark.databricks.delta.optimizeWrite.enabled": "true"
        },
        "node_type_id": "Standard_E16s_v3",
        "ssh_public_keys": [],
        "custom_tags": {
        "UseCase": "UDP 2",
        "Team": "DATA ENG",
        "Env": "Prod"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": "true",
        "init_scripts": [
        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisHiveMetastoreDBR7.0/DevTestHiveMetastore.sh"
            }
        }
    ]
}

new_cluster_automated_status = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 32
    },
    "spark_version": "7.3.x-scala2.12",
    "spark_conf": {

        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.io.cache.compression.enabled": "false",
        "spark.databricks.passthrough.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.service.server.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.conda.condaMagic.enabled": "true",
        "enable_local_disk_encryption": "true",
        "spark.databricks.io.cache.maxMetaDataCache": "1g",
        "spark.databricks.io.cache.maxDiskUsage": "50g"
    },
    "node_type_id": "Standard_E16s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
    "UseCase": "UDP 2",
    "Team": "DATA ENG",
    "Env": "Prod"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "enable_elastic_disk": "true",
    "init_scripts": [

        {
            "dbfs": {
                "destination": global_confs.get('cluster_init_script')
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/PolarisHiveMetastoreDBR7.0/DevTestHiveMetastore.sh"
            }
        }
    ]
}

# ---------------------Talend----------------------------------
def wait_for_talend_job(executable_id, fail_on_error=False, *args, **kwargs):
    talend_server_baseurl = azure_confs.get_variable("talend-server-baseurl")
    url = talend_server_baseurl + "/executions"
    token = "Bearer " + azure_confs.get_variable("talend-server-token")
    if kwargs:
        payload_dict = dict(executable=executable_id, parameters=kwargs)
    else:
        payload_dict = dict(executable=executable_id)

    header = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }

    logging.info("Executing Talend Call \n URL: {}\n PARAMETERS {}".format(url, payload_dict))
    r = requests.post(url, headers=header, data=json.dumps(payload_dict))

    response = r.json()
    logging.info("Enqueued Talend job.. \n Response: {}".format(response))
    execution_id = response["executionId"]

    execution_status = ""
    while execution_status != "EXECUTION_SUCCESS":
        status_url = url + "/" + execution_id
        logging.info("Validate Talend Status \n URL: {}".format(status_url))
        status = requests.get(status_url, headers=header)
        status_request = status.json()
        execution_status = status_request["executionStatus"]
        logging.info("Current Execution Status of: " + execution_status)
        if execution_status in ("EXECUTION_FAILED", "DEPLOY_FAILED"):
            logging.error("Execution fail. Please review the status.")
            raise RuntimeError("Talend Job Orchestration failed with status {}".format(execution_status))            
        elif execution_status != "EXECUTION_SUCCESS":
            time.sleep(10)

    logging.info("finished")
