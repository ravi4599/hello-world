FROM apache/airflow:2.7.3-python3.11

COPY ./.env /opt/airflow/.env
COPY ./requirements.txt /opt/airflow/requirements.txt
COPY ./requirements_polaris.txt /opt/airflow/requirements_polaris.txt

RUN pip install -r /opt/airflow/requirements.txt
RUN pip install --no-deps --extra-index-url=https://yggttga73ttik2nebcn2xi3y5o7wthsbt2e3e3ypsjuuwzm6mswq@pkgs.dev.azure.com/InspirePolaris/udp-dio/_packaging/udp-dio/pypi/simple/ -r /opt/airflow/requirements_polaris.txt