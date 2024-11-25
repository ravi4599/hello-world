import json

import yaml
import inspect
import os
from airflow.providers.microsoft.azure.secrets.key_vault import AzureKeyVaultBackend
from airflow.models import Variable
import logging


def __merge_dict_list(dict_list):
    """
    :param dict_list: sequence of dicts to merge from left to right overriding duplicated keys taking the last coming
    :return: a dict with all unique keys
    """
    a = dict()
    for i_dict in dict_list:
        if i_dict:
            a.update(i_dict)
    return a


def __load_conf_yml_to_dict(basepath):
    """
    Load conf.yml file under specific basepath and return it as dict
    :param basepath:
    :return: conf.yml as dict
    """
    with open("{}/conf.yml".format(basepath)) as f:
        return yaml.safe_load(f)


global_confs = __load_conf_yml_to_dict(os.path.dirname(__file__))


def load_confs():
    """
    Load conf.yml file in the same caller's folder and convert it to a dict
    :return: dict containing global and dag-dependent conf(s)
    """
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    basepath = os.path.dirname(module.__file__)
    confs = __merge_dict_list([global_confs.copy(), __load_conf_yml_to_dict(basepath)])
    logging.info("Loaded configurations from yml: \n {}".format(confs))
    return confs


def load_azure_kv_confs(azure_kv_url=None):
    """
    Loads all AKV secrets as python dict
    :param azure_kv_url: KeyVault base url
    :return: dict containing couples secretname->secret_value
    """
    azure_url = azure_kv_url or global_confs["azure_kv_url"]
    logging.info("Using azure_vault url {}".format(azure_url))

    if not azure_url:
        raise ValueError("Provide azure_kv_url!")

    return AzureKeyVaultBackend(vault_url=azure_url)


def load_AF_variables(variable_parent_name, json_deser=True):
    """
    Loads AF Variables stored under Admin in AF portal by
    :param variable_parent_name: AF variable key name
    :param json_deser: true if the value is a in json format
    :return: AF Variable value
    """
    af_variables = json.loads(Variable.get(variable_parent_name)) \
        if json_deser \
        else str(Variable.get(variable_parent_name))
    logging.info("Loaded Airflow Variables: \n {}".format(af_variables))
    return af_variables
