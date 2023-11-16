import json

try:
    from flask import Flask
    from flask_restful import Resource, Api
    from apispec import APISpec
    from marshmallow import Schema, fields
    from apispec.ext.marshmallow import MarshmallowPlugin
    from flask_apispec.extension import FlaskApiSpec
    from flask_apispec.views import MethodResource
    from flask_apispec import marshal_with, doc, use_kwargs
    from marshmallow import Schema, fields
    import datetime, os, sys, json, boto3, uuid, time
    from datetime import datetime
    from dotenv import load_dotenv
    import pynamodb.attributes as at
    from pynamodb.models import Model
    from pynamodb.attributes import *
    from pynamodb.models import Model

    print("All imports are ok............")
except Exception as e:
    print("Error: {} ".format(e))

DEV_ACCESS_KEY = os.getenv("DEV_ACCESS_KEY")
DEV_SECRET_KEY = os.getenv("DEV_SECRET_KEY")
DEV_REGION = os.getenv("DEV_REGION")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")


class GLueJobsMetaData(Model):
    class Meta:
        table_name = DYNAMODB_TABLE_NAME
        aws_access_key_id = DEV_ACCESS_KEY
        aws_secret_access_key = DEV_SECRET_KEY
        region = DEV_REGION

    job_name = UnicodeAttribute(hash_key=True)
    table_name = UnicodeAttribute(range_key=True)

    active = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute(null=True)
    created_by = UnicodeAttribute(null=True)
    cron_schedule = UnicodeAttribute(null=True)
    glue_payload = UnicodeAttribute(null=True)


class EventBridge(object):

    def __init__(self, instance=None):
        self.instance = instance
        self.client = boto3.client("events",
                                   aws_access_key_id=DEV_ACCESS_KEY,
                                   aws_secret_access_key=DEV_SECRET_KEY,
                                   region_name=DEV_REGION)

    def run(self):
        try:

            response = self.client.put_rule(
                Name=self.instance.RuleName,
                ScheduleExpression=self.instance.ScheduleExpression,
                State=self.instance.State,
                Description=self.instance.Description,
            )

            response = self.client.put_targets(
                Rule=self.instance.RuleName,
                Targets=[
                    {
                        'Id': self.instance.Id,
                        'Arn': self.instance.lambdaArn,
                        'Input': json.dumps(self.instance.inputEvent),
                    },
                ],
            )
            return response

        except Exception as e:
            print("error", e)
            return 'error'


class InputTriggers(object):

    def __init__(self, RuleName='', ScheduleExpression='',
                 State='DISABLED', Description='', lambdaArn='',
                 inputEvent={}, Id='test-id'):
        self.RuleName = RuleName
        self.ScheduleExpression = "cron({})".format(ScheduleExpression)
        self.State = State
        self.Description = Description
        self.lambdaArn = lambdaArn
        self.inputEvent = inputEvent
        self.Id = Id


class GlueIngestionFramework:

    def __init__(self):
        self.glue = boto3.client(
            "glue",
            aws_access_key_id=DEV_ACCESS_KEY,
            aws_secret_access_key=DEV_SECRET_KEY,
            region_name=DEV_REGION,
        )

    def create_glue_ingestion_job_with_cron(self,
                                            job_name,
                                            table_name,
                                            active="False",
                                            created_by='datateam',
                                            cron_schedule='0/15 * * * ? *',
                                            lambdaArn="",
                                            glue_payload={},
                                            ):

        trigger_payload = {"job_name": job_name, "table_name": table_name}
        state = "DISABLED"
        RuleName = f"Glue_Job_Fire_{table_name}"

        if active == "true" or active == "True":
            state = "ENABLED"

        instance_ = InputTriggers(RuleName=RuleName,
                                  ScheduleExpression=cron_schedule,
                                  lambdaArn=lambdaArn,
                                  State=state,
                                  inputEvent=trigger_payload
                                  )

        instanceEvent = EventBridge(instance=instance_)
        response_event_bridge = instanceEvent.run()
        print("response_event_bridge", response_event_bridge)

        if response_event_bridge.get("ResponseMetadata").get("HTTPStatusCode").__str__() == "200":
            GLueJobsMetaData(
                job_name=job_name,
                table_name=table_name,
                created_at=datetime.now().__str__(),
                active=active,
                created_by=created_by,
                cron_schedule=cron_schedule,
                glue_payload=json.dumps(glue_payload)
            ).save()

            return True

        return False


class JobSchema(Schema):
    job_name = fields.String(required=True, description="Job name")
    table_name = fields.String(required=True, description="Table name")
    active = fields.String(required=True, description="Active status")
    created_by = fields.String(required=True, description="Created by")
    cron_schedule = fields.String(required=True, description="Cron schedule")
    lambdaArn = fields.String(required=True, description="Lambda ARN")
    glue_payload = fields.Dict(required=True, description="Glue payload", keys=fields.String(), values=fields.String())


class GlueController(MethodResource, Resource):

    @doc(description="""
    Sample Payload
    
     {
        "active":"False",
        "created_by": "soumil",
        "cron_schedule": "0/15 * * * ? *",
        "job_name": "glue-4-test-job",
        "lambdaArn": "arn:aws:lambda:us-east-1:XXXXX:function:handynamodb",
        "table_name": "customers",
        "glue_payload": {
            "ENABLE_CLEANER": "True",
            "ENABLE_HIVE_SYNC": "True",
            "ENABLE_PARTITION": "True",
            "GLUE_DATABASE": "hudidb",
            "GLUE_TABLE_NAME": "customers",
            "HUDI_PRECOMB_KEY": "ts",
            "HUDI_RECORD_KEY": "customer_id",
            "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
            "JOB_NAME": "glue-template",
            "PARTITON_FIELDS": "year,month",
            "SOURCE_FILE_TYPE": "json",
            "SOURCE_S3_PATH": "s3://jt-soumilshah-test/raw/customers/",
            "TARGET_S3_PATH": "s3://jt-soumilshah-test/silver/customers/",
            "USE_SQL_TRANSFORMER": "True",
            "SQL_TRANSFORMER_QUERY": "SELECT * ,extract(year from ts) as year, extract(month from ts) as month, extract(day from ts) as day FROM temp;"
        }
    }
    
    
    """, tags=['Glue Job'])
    @use_kwargs(JobSchema, location=("json"))
    def post(self, **kwargs):

        '''
        Get method represents a GET API method
        '''
        _ = {'message': 'APi are working fine'}
        print("kwargs", kwargs)
        payloads = kwargs

        helper = GlueIngestionFramework()
        response = helper.create_glue_ingestion_job_with_cron(
            job_name=payloads.get("job_name"),
            table_name=payloads.get("table_name"),
            lambdaArn=payloads.get("lambdaArn"),
            active=payloads.get("active"),
            created_by=payloads.get("created_by"),
            cron_schedule=payloads.get("cron_schedule"),
            glue_payload=payloads.get("glue_payload")
        )
        if response:
            return {"statuscode": 200, "message": "created job successfully"}
        else:
            return {"statuscode": -1, "message": "Failed to create Job"}
