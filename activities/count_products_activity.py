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


@activity.defn()
async def count_products_activity(query: str) -> int:
    print("query:", query)

    response = requests.get(query, timeout=40)
    response_dict = json.loads(response.text)
    try:
        return int(response_dict["@odata.count"])
    except Exception as e:
        print("Error in action:", e)
        return 0


async def main():
    # Create client to localhost on default namespace
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[],
        activities=[count_products_activity],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
