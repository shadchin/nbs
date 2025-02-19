#pragma once

#include <contrib/ydb/core/base/blobstorage.h>

#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TChannelMonInfo
{
    TString PoolKind;
    TString DataKind;
    bool Writable = false;
    bool SystemWritable = false;
};

using TGetMonitoringYDBGroupUrl = std::function<TString(
    ui32 groupId,
    const TString& storagePool)>;

using TBuildReassignChannelButton = std::function<void(
    IOutputStream& out,
    ui64 hiveTabletId,
    ui64 tabletId,
    ui32 channel)>;

void DumpChannels(
    IOutputStream& out,
    const TVector<TChannelMonInfo>& channelInfos,
    const NKikimr::TTabletStorageInfo& storage,
    const TGetMonitoringYDBGroupUrl& getGroupUrl,
    const TBuildReassignChannelButton& buildReassignButton,
    ui64 hiveTabletId);

}   // namespace NCloud::NStorage
