import grpc
import json
import logging
import argparse
from datetime import datetime, timedelta
from yandexcloud import SDK, RetryInterceptor
from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub
from yandex.cloud.compute.v1.instance_service_pb2 import (
    ListInstancesRequest,
    DeleteInstanceRequest,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

CACHE_VM_ID = "dp7329odurnhplpf5ff0"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service-account-key",
        required=True,
        help="Path to the service account key file",
    )
    parser.add_argument(
        "--folder-id",
        required=True,
        help="The ID of the folder to list instances in",
        default="bjeuq5o166dq4ukv3eec",
    )
    parser.add_argument(
        "--ttl", required=True, help="The TTL for the VMs", default=24, type=int
    )
    parser.add_argument("--apply", action="store_true", help="Apply the changes")

    args = parser.parse_args()

    treshold_time = datetime.now() - timedelta(hours=args.ttl)

    interceptor = RetryInterceptor(
        max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE]
    )

    with open(args.service_account_key, "r") as fp:
        sdk = SDK(
            service_account_key=json.load(fp),
            endpoint="api.ai.nebius.cloud",
            interceptor=interceptor,
        )

    client = sdk.client(InstanceServiceStub)
    response = client.List(ListInstancesRequest(folder_id=args.folder_id))

    for vm in response.instances:
        if vm.id == CACHE_VM_ID:
            logging.info(f"Skipping VM {vm.id} as it is a cache VM")
            continue

        creation_time = vm.created_at.ToDatetime()
        if creation_time < treshold_time:
            logging.info(
                f"VM {vm.id} is older than 24 hours, deleting it, created at {creation_time}"
            )
            if args.apply:
                client.Delete(DeleteInstanceRequest(instance_id=vm.id))
        else:
            logging.info(
                f"VM {vm.id} is younger than 24 hours, keeping it, created at {creation_time}"
            )
