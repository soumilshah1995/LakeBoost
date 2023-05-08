from dotenv import load_dotenv

load_dotenv()

try:
    from API import (app,
                     api,
                     HeathController,
                     GlueController,
                     GlueJobFireController,
                     docs
                     )
except Exception as e:
    print("Modules are Missing : {} ".format(e))

api.add_resource(HeathController, '/health_check')
docs.register(HeathController)

api.add_resource(GlueController, '/create_job')
docs.register(GlueController)

api.add_resource(GlueJobFireController, '/glue_fire_jobs')
docs.register(GlueJobFireController)
