
import great_expectations as gx
import awswrangler as wr
import logging

import boto3
from botocore.exceptions import ClientError
import yaml
from io import StringIO
from jinja2 import Template
import pandas as pd

import time
from functools import wraps

from data_quality.utills import UtillsExecution, SeverityLevel

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def time_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        resultado = func(*args, **kwargs)
        end = time.perf_counter()

        time_execution = end - start
        minutos = int(time_execution // 60)
        segundos = time_execution % 60

        if minutos > 0:
            logger.info(
                f"Time execution of step {func.__name__}: {minutos} minutos y {segundos:.2f} segundos")
        else:
            logger.info(
                f"Time execution of step {func.__name__}: {segundos:.2f} segundos")

        return resultado
    return wrapper


def handle_expectation_result(default_severity: SeverityLevel = SeverityLevel.HIGH):
    def decorator(func):
        @wraps(func)
        def wrapper(self, **kwargs):
            severity = kwargs.pop('severity', default_severity)
            validation_result = func(self, **kwargs)
            BOLD = '\033[1m'
            UNDERLINE = '\033[4m'
            END = '\033[0m'

            if validation_result['success'] == False:
                error_msg = (
                    f"""Validation Failed with column {BOLD}{UNDERLINE}{validation_result['expectation_config']['kwargs']['column']}{END} and rule {BOLD}{validation_result['expectation_config']['type']}{END}\n"""
                    f"Severity: {severity.name}\n"
                    f"unexpected_percent: {BOLD}{validation_result['result']['unexpected_percent']}{END}\n"
                    f"element_count :{validation_result['result']['element_count']}\n"
                    f"unexpected_percent :{validation_result['result']['unexpected_percent']}\n"
                )


                if severity == SeverityLevel.HIGH:
                    raise ValueError(error_msg)

                elif severity == SeverityLevel.MEDIUM:
                    return logger.warning(error_msg, exc_info=False)

                else:
                    return logger.info(error_msg, exc_info=False)
                
            if validation_result['success'] == True:
                error_msg = (
                    f"""Validation Success with rule {BOLD}{validation_result['expectation_config']['type']}{END}\n"""
                    f"Severity: {severity.name}\n"
                    f"Column: {kwargs['column']}\n"
                    
                )
            else:
                logger.info(
                    f"Validation Success\n"
                    f"Column: {kwargs['column']}\n"
                )
        return wrapper
    return decorator


class GenericRules:
    def __init__(self):
        pass
        # boto3.setup_default_session(profile_name='liberty-cwc')

    @time_execution
    def check_s3_file_exists(self, bucket_name, file_path):
        """
        Check if a specific file exists in an S3 bucket.

        :param bucket_name: Name of the S3 bucket
        :param file_path: Path to the file to check
        :return: True if the file exists, False otherwise
        """

        s3 = self.boto3.client('s3')

        try:
            s3.head_object(Bucket=bucket_name, Key=file_path)
            print(f"File '{file_path}' exists in bucket '{bucket_name}'.")
            return True
        except Exception as e:
            self.logger.error(
                f"The path no exist: {bucket_name}{file_path}", stack_info=False, exc_info=False)
            return False


class QARulesEngine(GenericRules):
    def __init__(self, df):
        self.context = gx.get_context()
        self.df = df

    def DefineGe(self):
        data_source = self.context.data_sources.add_pandas("pandas")
        data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "batch definition")
        batch = batch_definition.get_batch(
            batch_parameters={"dataframe": self.df})
        return batch


class QARulesPandas(QARulesEngine):
    def __init__(self, df,yaml:list,key_path:str):
        super().__init__(df)
        self.yaml = yaml
        self.key_path = key_path
        self.batch = self.DefineGe()
        

    def execute(self):
        """Ejecuta todas las reglas del YAML y retorna resultados"""
        UtillsExecution.run_yaml_rules(self, yaml_file=self.yaml, key_path=self.key_path)

    def run(self):
        """Método público para ejecutar las reglas de calidad"""
        self.execute()


    @handle_expectation_result()
    def ExpectColumnValuesToBeBetween(self, **kwargs):
        """Expect the column values to not be null.

            :param column: Column than you want to evaluate
            :return: Result about pass the rule quality
        """

        column = kwargs['column']
        min = kwargs['min']
        max = kwargs['max']

        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=column, min_value=min, max_value=max)

        validation_result = self.batch.validate(expectation)
        self.expectations.append(expectation) 
        return validation_result

    @handle_expectation_result()
    def ExpectColumnNotNull(self, **kwargs):
        """Expect the column values to not be null.

            :param column: Column than you want to evaluate
            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=kwargs['column'])

        validation_result = self.batch.validate(expectation)
        
        return validation_result

    @handle_expectation_result()
    def ExpectColumnAverageValuesNull(self, **kwargs):
        """Expect the column average permit values to be null.

            :param column: Column than you want to evaluate
            :param mostly: average in decimal than permit values null
            :param severity: level of rule for evaluation 
            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToBeNull(
            column=kwargs['column'], mostly=kwargs['mostly'],result_format='BASIC' )

        validation_result = self.batch.validate(expectation)
        
        return validation_result

    @handle_expectation_result()
    def ExpectColumnSumToBeBetween(self, **kwargs):
        """Expect the column sum to be between a minimum value and a maximum value.

            :param column (str): The column name.
            :param min_value (comparable type or None): The minimal sum allowed.
            :param max_value (comparable type or None): The maximal sum allowed.
            :param strict_min (boolean): If True, the minimal sum must be strictly larger than min_value. default=False.
            :param strict_max (boolean): If True, the maximal sum must be strictly smaller than max_value. default=False.
        """
        expectation = gx.expectations.ExpectColumnValuesToBeNull(
            column=kwargs['column'],
            min_value=kwargs['min'],
            max_value=kwargs['max'])

        validation_result = self.batch.validate(expectation)
        
        return validation_result

    @handle_expectation_result()
    def ExpectColumnMatchRegex(self, **kwargs):
        """ Expect the column entries to be strings that match a given regular expression.
            Valid matches can be found anywhere in the string, for example "[at]+"
            will identify the following strings as expected: "cat", "hat", "aa", "a", and "t", 
            and the following strings as unexpected: "fish", "dog".

            Example: 
                    ExpectColumnValuesToMatchRegex(
                                        column="test",
                                        regex="^a.*",
                                    )

            :param column: Column than you want to evaluate
            :param regex: apply regex pattron to column
            :param severity: level of rule for evaluation 
            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
            column=kwargs['column'], regex=kwargs['regex'])

        validation_result = self.batch.validate(expectation)
     
        return validation_result

    @handle_expectation_result()
    def ExpectColumnMatchLikePattern(self, **kwargs):
        """Expect the column entries to be strings that match a given like pattern expression.

           ExpectColumnValuesToMatchLikePattern is a Column Map Expectation.

           Example: 
                    ExpectColumnValuesToMatchLikePattern(
                                            column="test",
                                            like_pattern="[a]%"
                                        )

            :param column: Column than you want to evaluate
            :param like_pattern: apply regex pattron to column
            :param severity: level of rule for evaluation 
            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
            column=kwargs['column'], regex=kwargs['like_pattern'])

        validation_result = self.batch.validate(expectation)
        
        return validation_result

    @handle_expectation_result()
    def ExpectColumnMatchRegexList(self, **kwargs):
        """Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions.

           Example: 
                    ExpectColumnValuesToMatchRegexList(
                            column="test2",
                            regex_list=["^a.*", "^b.*"],
                            match_on="any"
                        )

            :param column: Column than you want to evaluate
            :param like_pattern_list (list): The list of regular expressions which the column entries should match.
            :param match_on (string): 'any' or 'all'. Use 'any' if the value should match at least one regular expression in the list.

            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToMatchRegexList(
            column=kwargs['column'], regex_list=kwargs['regex_list'], match_on=kwargs['match_on'])

        validation_result = self.batch.validate(expectation)
        
        return validation_result
    
    @handle_expectation_result()
    def ExpectColumnValuesToBeInSet(self, **kwargs):
        """Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions.

           Example: 
                    ExpectColumnValuesToBeInSet(
                                    column="test",
                                    value_set=[1, 2],
                                    mostly=.5
                                )

            :param column: Column than you want to evaluate
            :param value_set (set-like): A set of objects used for comparison.
    

            :return: Result about pass the rule quality
        """
        expectation = gx.expectations.ExpectColumnValuesToBeInSet(
            column=kwargs['column'], value_set=kwargs['value_set'], mostly=kwargs['mostly'])

        validation_result = self.batch.validate(expectation)
        
        return validation_result


class BuildTemplate(UtillsExecution):
    def __init__(self,bucket_artifact,source_file_yaml):
        self.utills = UtillsExecution()
        self.yaml_file = self.utills.read_s3_yaml(bucket_artifact,source_file_yaml)
        
        
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
                sql_file = self.utills.read_s3_text(bucket=bucket_name,key=object_key)

                sql_query = Template(sql_file).render(source['parameters'])

                query_list.append(sql_query)

        return query_list