# Init local environment
- Open Terminal
- Turn off Zscaler
- Be sure that Docker Desktop is running
- Run the following command
```
docker build --progress=plain --platform linux/amd64 ./docker/airflow --tag inspire/airflow-2.7.3 --no-cache

```
* Set your local environment
```
pip install -r ./docker/airflow/requirements.txt
pip install --no-deps --extra-index-url=https://yggttga73ttik2nebcn2xi3y5o7wthsbt2e3e3ypsjuuwzm6mswq@pkgs.dev.azure.com/InspirePolaris/udp-dio/_packaging/udp-dio/pypi/simple/ -r ./docker/airflow/requirements_polaris.txt
```
* Wait until it finishes the build
* Open `docker-compose.yaml` under `./docker` folder
* In volumes section modify the snowsql folder with the structure 
```
<location_in_your_laptop>:/opt/airflow/snowsql
```
* Run the following commands
```
cd ./docker
docker-compose up
```
