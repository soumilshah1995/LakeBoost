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


class  GlueJobFireSchema(Schema):
    job_name = fields.String(required=True, description="Job name")
    table_name = fields.String(required=True, description="Table name")


class GlueJobFireController(MethodResource, Resource):

    @doc(description="""
    Sample Payload
    
     {
        "job_name": "glue-4-test-job",
        "table_name": "customers",
    }
    
    
    """, tags=['Glue Job'])
    @use_kwargs(GlueJobFireSchema, location=("json"))
    def post(self, **kwargs):

        '''
        Get method represents a GET API method
        '''
        table_name = kwargs.get("table_name")
        job_name = kwargs.get("job_name")

        found = False
        payload = {}
        for item in GLueJobsMetaData.query(job_name):

            if item.table_name == table_name:
                found = True
                payload = item.glue_payload

        if found:

            json_glue_payload = json.loads(payload)
            fire_payload = {}
            for key, value in json_glue_payload.items(): fire_payload[f"--{key}"] = value

            glue = boto3.client(
                "glue",
                aws_access_key_id=DEV_ACCESS_KEY,
                aws_secret_access_key=DEV_SECRET_KEY,
                region_name=DEV_REGION,
            )

            response = glue.start_job_run(
                JobName=job_name,
                Arguments=fire_payload
            )
            print(response)
            return {
                'statusCode': 200,
                'body': json.dumps('Job FIred')
            }
