# Pipeline
## NOTES
1. This repository uses common script(s) and templated stage(s) from the [udp-pipeline-modules](https://dev.azure.com/InspirePolaris/_git/udp-pipeline-modules) repository. Below example uses [airflow2.yml](https://dev.azure.com/InspirePolaris/_git/udp-pipeline-modules?path=/templates/pipelines/airflow2.yml)
```
extends:
  template: templates/pipelines/airflow2.yml@udp-pipeline-modules
  parameters:
    commonRepositoryId: udp-pipeline-modules
```
2. Version of the common repository can be found in the azure-pipelines.yml[./azure-pipelines.yml]. In below example version is set to use tag _0.9.0_
```
resources:
  repositories:
  - repository: udp-pipeline-modules
    type: git
    name: udp-pipeline-modules/udp-pipeline-modules
    ref: refs/tags/0.9.0
```

## Steps
1. AZ CLI Login
2. Prepare Workspace - get infromation from KeyVault - PAT, VM's IP addresses, usernames and private key
3. Test SSH Connection
4. Cleanup old DAGs on remote host(s)
5. Copy [dags](./dags) folder to the rempote host(s) into `$AIRFLOW_HOME/dags/<REPOSITORY_NAME>/`
6. Process configuration files
7. Restart Airflow Webserver
8. Cleanup SSH Private Key
