import asyncio
import random
import string
from dateutil import parser
import datetime
import requests
import json

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

task_queue: str = "catalogue-count-queue"
workflow_name: str = "CountProductsWorkflow"
catalogue_query = "https://datahub.creodias.eu/odata/v1/Products?$filter=((ContentDate/Start ge 2023-03-01T00:00:00.000Z and ContentDate/Start lt 2023-08-31T23:59:59.999Z) and (Online eq true) and (((((Collection/Name eq 'SENTINEL-2'))))))&$expand=Attributes&$expand=Assets&$count=True&$orderby=ContentDate/Start asc"


async def main():
    # Create client to localhost on default namespace
    client = await Client.connect("localhost:7233")

    # you can randomize workflow_id to be able to run several workflows in a parralel
    # same workflow-id cannot be executed in parallel
    # workflow_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=30))
    workflow_id = "CF-tutorial-count-products"
    result = await client.execute_workflow(
        workflow_name, catalogue_query, id=workflow_id, task_queue=task_queue
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
