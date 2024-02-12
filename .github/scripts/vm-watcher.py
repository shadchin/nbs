from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub
from yandex.cloud.compute.v1.instance_service_pb2 import ListInstancesRequest,DeleteInstanceRequest
from yandexcloud import SDK,RetryInterceptor
import os
import grpc
from datetime import datetime, timedelta

CACHE_VM_ID="dp7329odurnhplpf5ff0"
threshold_time = datetime.now() - timedelta(hours=24)

interceptor = RetryInterceptor(max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE])

sdk = SDK(iam_token=os.getenv("IAM_TOKEN"), endpoint="api.ai.nebius.cloud", interceptor=interceptor)

client = sdk.client(InstanceServiceStub)
response = client.List(ListInstancesRequest(folder_id=os.getenv("FOLDER_ID")))

for vm in response.instances:
    if vm.id == CACHE_VM_ID:
        continue

    creation_time = vm.created_at.ToDatetime()
    if creation_time < threshold_time:
        print(f"VM {vm.id} is older than 24 hours, deleting it")
        client.Delete(DeleteInstanceRequest(instance_id=vm.id))
    else:
        print(f"VM {vm.id} is younger than 24 hours, keeping it, created at {creation_time}")
