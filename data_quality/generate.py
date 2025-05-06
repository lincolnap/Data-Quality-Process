from data_quality.rules import QARulesPandas,BuildTemplate
from data_quality.utills import UtillsExecution
import jmespath
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class RunRules(UtillsExecution):
    def __init__(self, bucket_artifact, source_file_yaml,environment,column_partition,value_partition):
        super().__init__()
        self.utills = UtillsExecution()     
        self.source_file_yaml = source_file_yaml
        self.column_partition = column_partition
        self.value_partition = value_partition
        self.environment = environment
        self.bucket_artifact = bucket_artifact.replace("-env-","-"+environment+"-")

    def execute_rules(self):
        self.yaml_file = self.utills.read_s3_yaml(self.bucket_artifact, self.source_file_yaml)
        logger.info("Execute Custom Query")

        if 'query_dq' in  self.yaml_file:
            self.files = BuildTemplate(
                bucket_artifact=self.bucket_artifact,
                source_file_yaml=self.source_file_yaml
            )
          

            sql_file_temp = self.files.build_sql_template(environment=self.environment)
            yaml_file = self.files.yaml_file
        
            for query, parameters in zip(sql_file_temp, yaml_file['query_dq']):
                df = self.utills.BuildDF(sql_query=query, parameters=parameters)
                QARulesPandas(df=df, yaml=yaml_file, key_path='rules_dq').run()


        if 'rules' in  self.yaml_file:
           generic_rules = jmespath.search(f"rules[*]", self.yaml_file)
           
           for item in generic_rules:
               
               if 'parameters' in item:
                   
                   query_generic = f"""SELECT * FROM {item['parameters']['table_name']}""" #WHERE {self.column_partition} = '{self.value_partition}'

                   df = self.utills.BuildDF(sql_query=query_generic
                                           ,parameters=item)
               elif 'expectations' in item:
                   
                    QARulesPandas(df=df,yaml=item['expectations'],key_path=None).run()