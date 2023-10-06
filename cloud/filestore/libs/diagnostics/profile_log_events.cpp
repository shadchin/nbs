#include "profile_log_events.h"

#include "profile_log.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/public/api/protos/action.pb.h>
#include <cloud/filestore/public/api/protos/checkpoint.pb.h>
#include <cloud/filestore/public/api/protos/cluster.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/endpoint.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>
#include <cloud/filestore/public/api/protos/ping.pb.h>
#include <cloud/filestore/public/api/protos/session.pb.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept HasGetLockType = requires(T t) {
    { t.GetLockType() } -> std::same_as<NProto::ELockType>;
};

template<typename T>
void InitProfileLogLockRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const T& request)
{
    auto* lockInfo = profileLogRequest.MutableLockInfo();
    lockInfo->SetNodeId(request.GetNodeId());
    lockInfo->SetHandle(request.GetHandle());
    lockInfo->SetOwner(request.GetOwner());
    lockInfo->SetOrigin(request.GetLockOrigin());

    lockInfo->SetOffset(request.GetOffset());
    lockInfo->SetLength(request.GetLength());
    if constexpr (HasGetLockType<T>) {
        lockInfo->SetType(request.GetLockType());
    }
    lockInfo->SetPid(request.GetPid());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NFuse {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_MATERIALIZE_REQUEST(name, ...) #name,

static const TString FuseRequestNames[] = {
    FILESTORE_FUSE_REQUESTS(FILESTORE_MATERIALIZE_REQUEST)
};

#undef FILESTORE_MATERIALIZE_REQUEST

const TString& GetFileStoreFuseRequestName(EFileStoreFuseRequest requestType)
{
    const auto index = static_cast<size_t>(requestType);
    if (index >= FileStoreFuseRequestStart &&
            index < FileStoreFuseRequestStart + FileStoreFuseRequestCount)
    {
        return FuseRequestNames[index - FileStoreFuseRequestStart];
    }

    static const TString unknown = "Unknown";
    return unknown;
}

////////////////////////////////////////////////////////////////////////////////

void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    EFileStoreFuseRequest requestType,
    TInstant currentTs)
{
    profileLogRequest.SetRequestType(static_cast<ui32>(requestType));
    profileLogRequest.SetTimestampMcs(currentTs.MicroSeconds());
}

void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo&& profileLogRequest,
    TInstant currentTs,
    const TString& fileSystemId,
    const NCloud::NProto::TError& error,
    IProfileLogPtr profileLog)
{
    profileLogRequest.SetDurationMcs(
        currentTs.MicroSeconds() - profileLogRequest.GetTimestampMcs());
    profileLogRequest.SetErrorCode(error.GetCode());

    profileLog->Write({fileSystemId, std::move(profileLogRequest)});
}


}  // namespace NFuse

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_DEFAULT_METHOD(name)                                         \
    template <>                                                                \
    void InitProfileLogRequestInfo(                                            \
        NProto::TProfileLogRequestInfo& profileLogRequest,                     \
        const NProto::T##name##Request& request)                               \
    {                                                                          \
        Y_UNUSED(profileLogRequest, request);                                  \
    }                                                                          \
// IMPLEMENT_DEFAULT_METHOD

    IMPLEMENT_DEFAULT_METHOD(Ping)
    IMPLEMENT_DEFAULT_METHOD(CreateFileStore)
    IMPLEMENT_DEFAULT_METHOD(DestroyFileStore)
    IMPLEMENT_DEFAULT_METHOD(AlterFileStore)
    IMPLEMENT_DEFAULT_METHOD(ResizeFileStore)
    IMPLEMENT_DEFAULT_METHOD(DescribeFileStoreModel)
    IMPLEMENT_DEFAULT_METHOD(GetFileStoreInfo)
    IMPLEMENT_DEFAULT_METHOD(ListFileStores)
    IMPLEMENT_DEFAULT_METHOD(CreateSession)
    IMPLEMENT_DEFAULT_METHOD(DestroySession)
    IMPLEMENT_DEFAULT_METHOD(PingSession)
    IMPLEMENT_DEFAULT_METHOD(AddClusterNode)
    IMPLEMENT_DEFAULT_METHOD(RemoveClusterNode)
    IMPLEMENT_DEFAULT_METHOD(ListClusterNodes)
    IMPLEMENT_DEFAULT_METHOD(AddClusterClients)
    IMPLEMENT_DEFAULT_METHOD(RemoveClusterClients)
    IMPLEMENT_DEFAULT_METHOD(ListClusterClients)
    IMPLEMENT_DEFAULT_METHOD(UpdateCluster)
    IMPLEMENT_DEFAULT_METHOD(StatFileStore)
    IMPLEMENT_DEFAULT_METHOD(SubscribeSession)
    IMPLEMENT_DEFAULT_METHOD(GetSessionEvents)
    IMPLEMENT_DEFAULT_METHOD(ResetSession)
    IMPLEMENT_DEFAULT_METHOD(ResolvePath)
    IMPLEMENT_DEFAULT_METHOD(StartEndpoint)
    IMPLEMENT_DEFAULT_METHOD(StopEndpoint)
    IMPLEMENT_DEFAULT_METHOD(ListEndpoints)
    IMPLEMENT_DEFAULT_METHOD(KickEndpoint)
    IMPLEMENT_DEFAULT_METHOD(ExecuteAction)

#undef IMPLEMENT_DEFAULT_METHOD

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TCreateHandleRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
    nodeInfo->SetFlags(request.GetFlags());
    nodeInfo->SetMode(request.GetMode());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TDestroyHandleRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
    nodeInfo->SetHandle(request.GetHandle());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TReadDataRequest& request)
{
    auto* rangeInfo = profileLogRequest.AddRanges();
    rangeInfo->SetNodeId(request.GetNodeId());
    rangeInfo->SetHandle(request.GetHandle());
    rangeInfo->SetOffset(request.GetOffset());
    rangeInfo->SetBytes(request.GetLength());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TWriteDataRequest& request)
{
    auto* rangeInfo = profileLogRequest.AddRanges();
    rangeInfo->SetNodeId(request.GetNodeId());
    rangeInfo->SetHandle(request.GetHandle());
    rangeInfo->SetOffset(request.GetOffset());
    rangeInfo->SetBytes(request.GetBuffer().Size());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TAllocateDataRequest& request)
{
    auto* rangeInfo = profileLogRequest.AddRanges();
    rangeInfo->SetNodeId(request.GetNodeId());
    rangeInfo->SetHandle(request.GetHandle());
    rangeInfo->SetOffset(request.GetOffset());
    rangeInfo->SetBytes(request.GetLength());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TTruncateDataRequest& request)
{
    auto* rangeInfo = profileLogRequest.AddRanges();
    rangeInfo->SetNodeId(request.GetNodeId());
    rangeInfo->SetHandle(request.GetHandle());
    rangeInfo->SetBytes(request.GetLength());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TAcquireLockRequest& request)
{
    InitProfileLogLockRequestInfo(profileLogRequest, request);
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TReleaseLockRequest& request)
{
    InitProfileLogLockRequestInfo(profileLogRequest, request);
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TTestLockRequest& request)
{
    InitProfileLogLockRequestInfo(profileLogRequest, request);
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TCreateNodeRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNewParentNodeId(request.GetNodeId());
    nodeInfo->SetNewNodeName(request.GetName());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TUnlinkNodeRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TRenameNodeRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
    nodeInfo->SetNewParentNodeId(request.GetNewParentId());
    nodeInfo->SetNewNodeName(request.GetNewName());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TAccessNodeRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
    nodeInfo->SetFlags(request.GetMask());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TListNodesRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TReadLinkRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TSetNodeAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetHandle(request.GetHandle());
    nodeInfo->SetFlags(request.GetFlags());
    nodeInfo->SetMode(request.GetUpdate().GetMode());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TGetNodeAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
    nodeInfo->SetHandle(request.GetHandle());
    nodeInfo->SetFlags(request.GetFlags());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TSetNodeXAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
    nodeInfo->SetNewNodeName(request.GetValue());
    nodeInfo->SetFlags(request.GetFlags());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TGetNodeXAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TListNodeXAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TRemoveNodeXAttrRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetParentNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetName());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TCreateCheckpointRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(request.GetNodeId());
    nodeInfo->SetNodeName(request.GetCheckpointId());
}

template <>
void InitProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TDestroyCheckpointRequest& request)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeName(request.GetCheckpointId());
}

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_DEFAULT_METHOD(name)                                         \
    template <>                                                                \
    void FinalizeProfileLogRequestInfo(                                        \
        NProto::TProfileLogRequestInfo& profileLogRequest,                     \
        const NProto::T##name##Response& response)                             \
    {                                                                          \
        Y_UNUSED(profileLogRequest, response);                                 \
    }                                                                          \
// IMPLEMENT_DEFAULT_METHOD

    IMPLEMENT_DEFAULT_METHOD(Ping)
    IMPLEMENT_DEFAULT_METHOD(CreateFileStore)
    IMPLEMENT_DEFAULT_METHOD(DestroyFileStore)
    IMPLEMENT_DEFAULT_METHOD(AlterFileStore)
    IMPLEMENT_DEFAULT_METHOD(ResizeFileStore)
    IMPLEMENT_DEFAULT_METHOD(DescribeFileStoreModel)
    IMPLEMENT_DEFAULT_METHOD(GetFileStoreInfo)
    IMPLEMENT_DEFAULT_METHOD(ListFileStores)
    IMPLEMENT_DEFAULT_METHOD(CreateSession)
    IMPLEMENT_DEFAULT_METHOD(DestroySession)
    IMPLEMENT_DEFAULT_METHOD(PingSession)
    IMPLEMENT_DEFAULT_METHOD(AddClusterNode)
    IMPLEMENT_DEFAULT_METHOD(RemoveClusterNode)
    IMPLEMENT_DEFAULT_METHOD(ListClusterNodes)
    IMPLEMENT_DEFAULT_METHOD(AddClusterClients)
    IMPLEMENT_DEFAULT_METHOD(RemoveClusterClients)
    IMPLEMENT_DEFAULT_METHOD(ListClusterClients)
    IMPLEMENT_DEFAULT_METHOD(UpdateCluster)
    IMPLEMENT_DEFAULT_METHOD(StatFileStore)
    IMPLEMENT_DEFAULT_METHOD(SubscribeSession)
    IMPLEMENT_DEFAULT_METHOD(GetSessionEvents)
    IMPLEMENT_DEFAULT_METHOD(ResetSession)
    IMPLEMENT_DEFAULT_METHOD(CreateCheckpoint)
    IMPLEMENT_DEFAULT_METHOD(DestroyCheckpoint)
    IMPLEMENT_DEFAULT_METHOD(ResolvePath)
    IMPLEMENT_DEFAULT_METHOD(UnlinkNode)
    IMPLEMENT_DEFAULT_METHOD(RenameNode)
    IMPLEMENT_DEFAULT_METHOD(AccessNode)
    IMPLEMENT_DEFAULT_METHOD(ReadLink)
    IMPLEMENT_DEFAULT_METHOD(RemoveNodeXAttr)
    IMPLEMENT_DEFAULT_METHOD(DestroyHandle)
    IMPLEMENT_DEFAULT_METHOD(AcquireLock)
    IMPLEMENT_DEFAULT_METHOD(ReleaseLock)
    IMPLEMENT_DEFAULT_METHOD(ReadData)
    IMPLEMENT_DEFAULT_METHOD(WriteData)
    IMPLEMENT_DEFAULT_METHOD(AllocateData)
    IMPLEMENT_DEFAULT_METHOD(StartEndpoint)
    IMPLEMENT_DEFAULT_METHOD(StopEndpoint)
    IMPLEMENT_DEFAULT_METHOD(ListEndpoints)
    IMPLEMENT_DEFAULT_METHOD(KickEndpoint)
    IMPLEMENT_DEFAULT_METHOD(ExecuteAction)

#undef IMPLEMENT_DEFAULT_METHOD

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TCreateHandleResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(response.GetNodeAttr().GetId());
    nodeInfo->SetHandle(response.GetHandle());
    nodeInfo->SetSize(response.GetNodeAttr().GetSize());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TTestLockResponse& response)
{
    auto* lockInfo = profileLogRequest.MutableLockInfo();
    lockInfo->SetConflictedOwner(response.GetOwner());
    lockInfo->SetConflictedOffset(response.GetOffset());
    lockInfo->SetConflictedLength(response.GetLength());
    if (response.HasLockType()) {
        lockInfo->SetConflictedLockType(response.GetLockType());
    }
    if (response.HasPid()) {
        lockInfo->SetConflictedPid(response.GetPid());
    }
    if (response.HasIncompatibleLockOrigin()) {
        lockInfo->SetOrigin(response.GetIncompatibleLockOrigin());
    }
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TCreateNodeResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(response.GetNode().GetId());
    nodeInfo->SetMode(response.GetNode().GetMode());
    nodeInfo->SetSize(response.GetNode().GetSize());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TListNodesResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetSize(response.GetNames().size());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TSetNodeAttrResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(response.GetNode().GetId());
    nodeInfo->SetSize(response.GetNode().GetSize());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TGetNodeAttrResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNodeId(response.GetNode().GetId());
    nodeInfo->SetMode(response.GetNode().GetMode());
    nodeInfo->SetSize(response.GetNode().GetSize());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TSetNodeXAttrResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetSize(response.GetVersion());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TGetNodeXAttrResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetNewNodeName(response.GetValue());
    nodeInfo->SetSize(response.GetVersion());
}

template <>
void FinalizeProfileLogRequestInfo(
    NProto::TProfileLogRequestInfo& profileLogRequest,
    const NProto::TListNodeXAttrResponse& response)
{
    auto* nodeInfo = profileLogRequest.MutableNodeInfo();
    nodeInfo->SetSize(response.GetNames().size());
}

}   // namespace NCloud::NFileStore