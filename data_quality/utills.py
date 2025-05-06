import pandas as pd
import great_expectations as gx
import logging

import boto3
import yaml
import awswrangler as wr

from io import StringIO
from botocore.exceptions import ClientError
from pandas import DataFrame
from jinja2 import Template
import jmespath
from enum import Enum, auto

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SeverityLevel(Enum):
    HIGH = auto()
    MEDIUM = auto()
    LOW = auto()

class UtillsExecution:
    def __init__(self):
        pass
    def read_s3_text(self,bucket, key):
        s3 = boto3.client('s3')
        print(key)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')

    def read_s3_yaml(self,bucket, object_key):
        try:
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=bucket, Key=object_key)
            config = yaml.safe_load(StringIO(obj['Body'].read().decode('utf-8')))
            return config
        except ClientError as e:
            print(f"Error de S3: {e}")
            return None
        except yaml.YAMLError as e:
            print(f"Error al parsear YAML: {e}")
            return None
        
    
    def BuildTemplateSQL(self,yaml_file,sql_file):

        sql_query = Template(sql_file).render(yaml_file['parameters'])

        return sql_query
    
    def build_sql_template(self,environment):
    
        main_key = 'query_dq'
        query_list = []
        yaml_path = self.yaml_file[main_key]

        if yaml_path is not None: 
            # get source sql file
            for source in yaml_path:
                s3_path_parts = source['route'][5:].split('/', 1)
                bucket_name = str(s3_path_parts[0]).replace("-env-","-"+environment+"-")
                object_key = s3_path_parts[1] if len(s3_path_parts) > 1 else ''

                sql_file = self.read_s3_text(bucket=str(bucket_name).replace("-env-","-"+environment+"-"),key=object_key)

                sql_query = Template(sql_file).render(source['parameters'])

                query_list.append(sql_query)

        return query_list


    def BuildDF(self,sql_query,parameters) -> DataFrame:

        df_query = wr.athena.read_sql_query(sql=sql_query,
                                    database=parameters['parameters']['database_input'], 
                                    ctas_approach=False)
        
        return df_query
    
    def run_yaml_rules(self,yaml_file:dict, key_path:str):
        
        main_key = 'query_dq'
        config = yaml_file
        
        rules = jmespath.search(f"[*]", config) if key_path is None else jmespath.search(f"{main_key}[*].{key_path}", config)
        
        for rule in rules:
            try:
                rule_name_1 = rule.get('rule')
                severity_str_2 = rule.pop("severity", "HIGH")

                rule_func = getattr(self, rule_name_1)
                rule_func(**rule, severity=SeverityLevel[severity_str_2])

            except Exception as e:
                logger.error(f"Error reading: {e}")
                raise