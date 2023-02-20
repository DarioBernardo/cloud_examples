import os
import datetime
from googleapiclient.discovery import build


PROJECT_ID = os.environ['PROJECT_ID']
REGION = os.environ['REGION']
TEMPLATE_LOCATION = os.environ['TEMPLATE_LOCATION'].rstrip("/")
OUTPUT_LOCATION = os.environ['OUTPUT_LOCATION'].rstrip("/")
SUBNETWORK = os.environ.get('SUBNETWORK', f"regions/{REGION}/subnetworks/data-subnet-01")
MACHINE_TYPE = os.environ.get('MACHINE_TYPE', "n1-standard-4")

dataflow = build("dataflow", "v1b3")


def handle_pubsub_message(event, context):
    print(
        f"INPUT (event_id='{context.event_id}', timestamp='{context.timestamp}')"
    )

    if 'data' not in event:
        print(
            f"SKIPPING: no data (event_id='{context.event_id}', timestamp='{context.timestamp}')"
        )
        return

    _try_handle_pubsub_message(event, context)


def _try_handle_pubsub_message(event, context):

    input_file = f"gs://{event['attributes']['bucketId']}/{event['attributes']['objectId']}"
    print(f"This File was just uploaded: {input_file}")

    job_name = f"ingestion_{event['attributes']['bucketId']}_{event['attributes']['objectGeneration']}_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

    out_location = OUTPUT_LOCATION if OUTPUT_LOCATION[-1] != "/" else OUTPUT_LOCATION+"/"

    job_parameters = {
        'pipeline_name': job_name,
        'input': input_file,
        'output_increase': out_location+"increase/data",
        'output_decrease': out_location+"decrease/data",
    }
    environment_parameters = {
        "subnetwork": SUBNETWORK,
        "machine_type": MACHINE_TYPE
    }

    request = dataflow.projects().locations().templates().launch(
        projectId=PROJECT_ID,
        location=REGION,
        gcsPath=TEMPLATE_LOCATION,
        body={
            'jobName': job_name,
            'parameters': job_parameters,
            "environment": environment_parameters
        }
    )
    response = request.execute()
    job = response['job']

    print(
        f"PIPELINE LAUNCHED (event_id='{context.event_id}', timestamp='{context.timestamp}', job_name='{job['name']}', job_id='{job['id']}')"
    )