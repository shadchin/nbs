#include "service.h"
#include "service_private.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/base/hive.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestProfileLog
    : public IProfileLog
{
public:
    TMap<ui32, TVector<TRecord>> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void Write(TRecord record) override
    {
        UNIT_ASSERT(record.Request.HasRequestType());
        Requests[record.Request.GetRequestType()].push_back(std::move(record));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceTest)
{
    Y_UNIT_TEST(ShouldCreateFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(response.GetFileSystemId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetCloudId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetFolderId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlocksCount(), 1'000);
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(response.GetConfigVersion(), 1);

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadIops(), 100);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostPercentage(), 400);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetDefaultPostponedRequestWeight(), 4_KB);

        service.DestroyFileStore("test");
        service.AssertGetFileStoreInfoFailed("test");
    }

    Y_UNIT_TEST(ShouldAlterFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        service.AlterFileStore("test", "yyyy", "zzzz");

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(response.GetFileSystemId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetCloudId(), "yyyy");
        UNIT_ASSERT_VALUES_EQUAL(response.GetFolderId(), "zzzz");
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlocksCount(), 1'000);
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(response.GetConfigVersion(), 2);

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadIops(), 100);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostPercentage(), 400);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetDefaultPostponedRequestWeight(), 4_KB);
    }

    Y_UNIT_TEST(ShouldResizeFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        service.ResizeFileStore("test", 100'000'000);

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(response.GetFileSystemId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetCloudId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetFolderId(), "test");
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlocksCount(), 100'000'000);
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(response.GetConfigVersion(), 2);

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadIops(), 200);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteIops(), 600);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxReadBandwidth(), 60_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteBandwidth(), 60_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBoostPercentage(), 200);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile.GetDefaultPostponedRequestWeight(), 4_KB);

        service.AssertResizeFileStoreFailed("test", 1'000);
        service.AssertResizeFileStoreFailed("test", 0);
    }

    Y_UNIT_TEST(ShouldResizeFileStoreWithCustomPerformanceProfile)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const char* fsId = "test";
        const auto initialBlockCount = 1'000;
        const auto blockCount = 100'000'000;
        const auto customMaxReadIops = 111;
        const auto customMaxWriteIops = 222;
        service.CreateFileStore("test", initialBlockCount);
        auto resizeRequest = service.CreateResizeFileStoreRequest(
            "test",
            blockCount);
        resizeRequest->Record.MutablePerformanceProfile()->SetMaxReadIops(
            customMaxReadIops);
        service.SendRequest(MakeStorageServiceId(), std::move(resizeRequest));
        auto resizeResponse = service.RecvResizeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        auto response = service.GetFileStoreInfo(fsId)->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsId, response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, response.GetBlocksCount());

        auto profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        // autocalculated
        UNIT_ASSERT_VALUES_EQUAL(600, profile.GetMaxWriteIops());
        // custom
        UNIT_ASSERT_VALUES_EQUAL(customMaxReadIops, profile.GetMaxReadIops());

        resizeRequest = service.CreateResizeFileStoreRequest(
            "test",
            blockCount);
        resizeRequest->Record.MutablePerformanceProfile()->SetMaxWriteIops(
            customMaxWriteIops);

        service.SendRequest(MakeStorageServiceId(), std::move(resizeRequest));
        resizeResponse = service.RecvResizeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        response = service.GetFileStoreInfo(fsId)->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsId, response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, response.GetBlocksCount());

        profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        // custom
        UNIT_ASSERT_VALUES_EQUAL(customMaxWriteIops, profile.GetMaxWriteIops());
        // autocalculated
        UNIT_ASSERT_VALUES_EQUAL(200, profile.GetMaxReadIops());
    }

    Y_UNIT_TEST(ShouldResizeFileStoreAndAddChannels)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        ui32 createChannelsCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateFileStore)
                        {
                            const auto& request = msg->ModifyScheme.GetCreateFileStore();
                            const auto& config = request.GetConfig();
                            createChannelsCount = config.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);
        UNIT_ASSERT(createChannelsCount > 0);

        ui32 alterChannelsCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpAlterFileStore)
                        {
                            const auto& request = msg->ModifyScheme.GetAlterFileStore();
                            const auto& config = request.GetConfig();
                            alterChannelsCount = config.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        service.ResizeFileStore("test", 4_TB / DefaultBlockSize);
        UNIT_ASSERT(alterChannelsCount > 0);
        UNIT_ASSERT(alterChannelsCount > createChannelsCount);
    }

    Y_UNIT_TEST(ShouldFailAlterIfDescribeFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateFileStore("test", 1000);

        auto error = MakeError(E_ARGUMENT, "Error");
        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.AssertAlterFileStoreFailed("test", "xxxx", "yyyy");
    }

    Y_UNIT_TEST(ShouldDescribeModel)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const auto size1 = 1_GB/DefaultBlockSize;

        auto response1 = service.DescribeFileStoreModel(size1);
        auto& model1 = response1->Record.GetFileStoreModel();
        UNIT_ASSERT_VALUES_EQUAL(model1.GetBlocksCount(), size1);
        UNIT_ASSERT_VALUES_EQUAL(model1.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(model1.GetChannelsCount(), 7);

        auto& profile1 = model1.GetPerformanceProfile();
        UNIT_ASSERT(!profile1.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxReadIops(), 100);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxReadBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostPercentage(), 400);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetDefaultPostponedRequestWeight(), 4_KB);

        const auto size2 = 4_TB/DefaultBlockSize;
        auto response2 = service.DescribeFileStoreModel(size2);
        auto& model2 = response2->Record.GetFileStoreModel();
        UNIT_ASSERT_VALUES_EQUAL(model2.GetBlocksCount(), size2);
        UNIT_ASSERT_VALUES_EQUAL(model2.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(model2.GetChannelsCount(), 19);

        auto& profile2 = model2.GetPerformanceProfile();
        UNIT_ASSERT(!profile2.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxReadIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteIops(), 4800);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxReadBandwidth(), 240_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteBandwidth(), 240_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostPercentage(), 25);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetDefaultPostponedRequestWeight(), 4_KB);


        service.AssertDescribeFileStoreModelFailed(0);
        service.AssertDescribeFileStoreModelFailed(1000, 0);
    }

    Y_UNIT_TEST(ShouldCreateSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.PingSession(headers);
        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldReturnFileStoreInfoWhenCreateSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000, DefaultBlockSize, NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);

        auto response = service.CreateSession(THeaders{"test", "client", ""});

        UNIT_ASSERT(response->Record.HasFileStore());
        UNIT_ASSERT_EQUAL(
            NProto::EStorageMediaKind::STORAGE_MEDIA_SSD,
            static_cast<NProto::EStorageMediaKind>(response->Record.GetFileStore().GetStorageMediaKind()));
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfPipeFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        TActorId worker;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        worker = event->Sender;
                        break;
                    }
                    case TEvTabletPipe::EvClientConnected: {
                        if (fail && worker && event->Recipient == worker) {
                            auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                            const_cast<NKikimrProto::EReplyStatus&>(msg->Status) = NKikimrProto::ERROR;
                        } else {
                            fail = false;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfPipeDisconnected)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        worker = event->Sender;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.CreateSession(headers);

        auto msg = std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
            static_cast<ui64>(0),
            TActorId(),
            TActorId());

        runtime.Send(
            new IEventHandle(
                // send back
                worker,
                TActorId(),
                msg.release(),
                0, // flags
                0),
            nodeIdx);

        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTablet::EvCreateSessionRequest)
        };
        env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfCreateFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        fail = false;
                        auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                            MakeError(E_REJECTED, "xxx"));

                        runtime.Send(
                            new IEventHandle(
                                // send back
                                event->Sender,
                                event->Sender,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.AssertCreateSessionFailed(headers);
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldFailIfCreateSessionFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        fail = false;
                        auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                            MakeError(E_REJECTED, "xxx"));

                        runtime.Send(
                            new IEventHandle(
                                // send back
                                event->Sender,
                                event->Sender,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.AssertCreateSessionFailed(headers);
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldCleanUpIfSessionFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        auto& runtime = env.GetRuntime();

        ui64 tabletId = -1;
        TActorId session;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg = event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc = msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                case TEvIndexTablet::EvCreateSessionRequest: {
                    session = event->Sender;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT(headers.SessionId);
        UNIT_ASSERT(tabletId != -1llu);
        UNIT_ASSERT(session);

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        // explicitly fail session actor, proper way is to catch
        // IndexTablet::CreateSession request via observer func and respond w error
        // but for some reason runtime doesn't catch this event during tablet restart
        // though it actually happens and session resotres by the end of restart
        runtime.Send(
            new IEventHandle(
                // send back
                session,
                session,
                new TEvents::TEvPoisonPill(),
                0, // flags
                0),
            nodeIdx);

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.RebootTablet();

        auto response = service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::File(RootNodeId, "aaa"));

        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), (ui32)E_FS_INVALID_SESSION);

        service.CreateSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "aaa"));
    }

    Y_UNIT_TEST(ShouldRestoreClientSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            true        // restoreClientSession
        );
        UNIT_ASSERT_VALUES_UNEQUAL("", headers.SessionId);

        auto headers2 = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            true        // restoreClientSession
        );
        UNIT_ASSERT_VALUES_EQUAL(headers.SessionId, headers2.SessionId);

        auto headers3 = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            false       // restoreClientSession
        );
        UNIT_ASSERT_VALUES_UNEQUAL(headers.SessionId, headers3.SessionId);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldNotPingAndDestroyInvalidSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        THeaders invalidFileSystem = {"xxx", "client", headers.SessionId};
        THeaders invalidClient = {"test", "invalid client", headers.SessionId};
        THeaders invalidSession = {"test", "client", "invalid session"};

        // FIXME
        // service.AssertPingSessionFailed(invalidFileSystem);
        service.AssertPingSessionFailed(invalidClient);
        service.AssertPingSessionFailed(invalidSession);

        service.AssertDestroySessionFailed(invalidFileSystem);
        service.AssertDestroySessionFailed(invalidClient);
        // fail safe
        service.DestroySession(invalidSession);
    }

    Y_UNIT_TEST(ShouldForwardRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        auto request = service.CreateCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file"));

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        service.SendCreateNodeRequest(std::move(request));

        auto response = service.RecvCreateNodeResponse();
        UNIT_ASSERT(response);
        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason().c_str());
    }

    Y_UNIT_TEST(ShouldNotForwardRequestsWithInvalidSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        THeaders invalidFileSystem = {"xxx", "client", headers.SessionId};
        THeaders invalidClient = {"test", "invalid client", headers.SessionId};
        THeaders invalidSession = {"test", "client", "invalid session"};

        auto nodeArgs = TCreateNodeArgs::File(RootNodeId, "file");

        service.AssertCreateNodeFailed(invalidFileSystem, nodeArgs);
        service.AssertCreateNodeFailed(invalidClient, nodeArgs);
        service.AssertCreateNodeFailed(invalidSession, nodeArgs);

        // sanity check
        service.CreateNode(headers, nodeArgs);
    }

    Y_UNIT_TEST(ShouldGetSessionEvents)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.SubscribeSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));

        auto response = service.GetSessionEvents(headers);

        const auto& events = response->Record.GetEvents();
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
    }

    Y_UNIT_TEST(ShouldGetSessionEventsStream)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.SubscribeSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file1"));

        {
            auto response = service.GetSessionEventsStream(headers);

            const auto& events = response->Record.GetEvents();
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        }

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file2"));

        {
            auto response = service.RecvResponse<TEvService::TEvGetSessionEventsResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());

            const auto& events = response->Record.GetEvents();
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldListFileStores)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        TServiceClient service(env.GetRuntime(), nodeIdx);

        TVector<TString> expected = {"dir/fs1", "dir/fs2", "dir1/fs", "dir2/fs"};
        for (const auto& id : expected) {
            service.CreateFileStore(id, 1000);
        }

        auto response = service.ListFileStores();
        const auto& proto = response->Record.GetFileStores();

        TVector<TString> filestores;
        Copy(proto.begin(), proto.end(), std::back_inserter(filestores));
        Sort(filestores);

        UNIT_ASSERT_VALUES_EQUAL(filestores, expected);

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service")
            ->FindSubgroup("request", "ListFileStores");
        counters->OutputPlainText(Cerr);
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());
        UNIT_ASSERT_EQUAL(0, counters->GetCounter("InProgress")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldFailListFileStoresIfDescribeSchemeFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("fs1", 10000);
        service.CreateFileStore("fs2", 10000);

        auto error = MakeError(E_ARGUMENT, "Error");

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = service.AssertListFileStoresFailed();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldProfileRequests)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();
        TTestEnv env({}, {}, {}, profileLog);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        profileLog->Start();

        service.CreateFileStore("test", 1'000);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.AlterFileStore("test", "yyyy", "zzzz");
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.ResizeFileStore("test", 100'000'000);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.DescribeFileStoreModel(1_GB/DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.ListFileStores();
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.PingSession(headers);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));
        UNIT_ASSERT_VALUES_EQUAL(1, profileLog->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog->Requests[static_cast<ui32>(EFileStoreRequest::CreateNode)].size());

        service.ListNodes(headers, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog->Requests[static_cast<ui32>(EFileStoreRequest::ListNodes)].size());

        service.DestroySession(headers);
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());

        service.DestroyFileStore("test");
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());

        profileLog->Stop();
    }

    Y_UNIT_TEST(ShouldSupportInterHostMigration)
    {
        TTestEnvConfig cfg;
        cfg.StaticNodes = 1;
        cfg.DynamicNodes = 2;
        TTestEnv env(cfg);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx1 = env.CreateNode("nfs");
        ui32 nodeIdx2 = env.CreateNode("nfs");

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx2);

        service1.CreateFileStore("test", 1'000);
        auto headers1 = service1.InitSession("test", "client");
        service1.PingSession(headers1);

        service1.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service1.ListNodes(headers1, 1);

        auto headers2 = service2.InitSession("test", "client", "", false, 1, true);
        service2.PingSession(headers2);

        headers2 = service2.InitSession("test", "client", "", true, 1);
        service2.PingSession(headers2);

        service2.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service2.ListNodes(headers2, 1);

        service1.DestroySession(headers1);

        service2.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
        service2.DestroySession(headers2);
    }

    Y_UNIT_TEST(ShouldSupportIntraHostMigration)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);
        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);

        service.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers1, 1);

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        headers2 = service.InitSession("test", "client", "", true, 1);
        service.PingSession(headers2);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service.ListNodes(headers2, 1);

        service.DestroySession(headers1);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldProperlyDeleteSubsessions)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);

        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);
        service.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers1, 1);

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        service.DestroySession(headers1);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldProperlyDeleteCounters)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        auto counters = env.GetCounters();
        counters = counters->FindSubgroup("component", "service_fs");
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);

        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        service.DestroySession(headers1);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        service.DestroySession(headers2);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(!counter);
        }
    }

    Y_UNIT_TEST(ShouldRejectParallelCreateOrDestroyRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        TAutoPtr<IEventHandle> resp;
        bool fail = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        worker = event->Sender;
                        break;
                    }
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        if (!resp && fail) {
                            resp = event;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        auto response = service.CreateSession(headers);
        auto sessionId = response->Record.GetSession().GetSessionId();
        headers.SessionId = sessionId;

        fail = true;
        service.SendCreateSessionRequest(headers);

        service.AssertDestroySessionFailed(headers);

        runtime.Send(resp.Release(), nodeIdx);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldNotDestroyWholeSessionIfSubSessionFailes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        bool fail = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        if (fail) {
                            auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                                MakeError(E_REJECTED, "xxx"));
                            fail = false;
                            runtime.Send(
                                new IEventHandle(
                                    // send back
                                    event->Recipient,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        auto response = service.CreateSession(headers);
        auto sessionId = response->Record.GetSession().GetSessionId();
        headers.SessionId = sessionId;

        service.PingSession(headers);

        fail = true;
        headers.SessionSeqNo = 1;
        service.AssertCreateSessionFailed(headers, "", true, 1);
        service.AssertPingSessionFailed(headers);

        headers.SessionSeqNo = 0;
        service.PingSession(headers);
    }

    Y_UNIT_TEST(ShouldUpdateSessionStateWhenRestoringSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");
        service.PingSession(headers);

        service.ResetSession(headers, "123");

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers, 1);

        THeaders headers1;
        auto response1 = service.InitSession(headers1, "test", "client", "", true, 1, true);
        UNIT_ASSERT_VALUES_EQUAL(response1->Record.GetSession().GetSessionState(), "123");
        service.PingSession(headers1);

        THeaders headers2;
        auto response2 = service.InitSession(headers2, "test", "client", "", true, 1, false);
        UNIT_ASSERT_VALUES_EQUAL(response2->Record.GetSession().GetSessionState(), "123");
        service.PingSession(headers2);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service.ListNodes(headers2, 1);

        service.DestroySession(headers);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldGetStorageConfigValues)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        NProtoPrivate::TGetStorageConfigFieldsRequest request;
        request.SetFileSystemId("test");
        request.AddStorageConfigFields("Unknown");
        request.AddStorageConfigFields("SSDBoostTime");
        request.AddStorageConfigFields("CompactionThreshold");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("getstorageconfigfields", buf);
        NProtoPrivate::TGetStorageConfigFieldsResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        const auto& storageValues = response.GetStorageConfigFieldsToValues();

        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("SSDBoostTime"),
            "Default");
        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("Unknown"),
            "Not found");
        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("CompactionThreshold"),
            "1000");
    }

    NProtoPrivate::TChangeStorageConfigResponse ExecuteChangeStorageConfig(
        NProto::TStorageConfig config,
        TServiceClient& service,
        bool mergeWithConfig = false)
    {
        NProtoPrivate::TChangeStorageConfigRequest request;
        request.SetFileSystemId("test");

        *request.MutableStorageConfig() = std::move(config);
        request.SetMergeWithStorageConfigFromTabletDB(mergeWithConfig);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto jsonResponse = service.ExecuteAction(
            "changestorageconfig", buf);
        NProtoPrivate::TChangeStorageConfigResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());
        return response;
    }

    void CheckStorageConfigValues(
        TVector<TString> names,
        THashMap<TString, TString> answer,
        TServiceClient& service)
    {

        NProtoPrivate::TGetStorageConfigFieldsRequest request;
        request.SetFileSystemId("test");
        for (const auto& name: names) {
            request.AddStorageConfigFields(name);
        }

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("getstorageconfigfields", buf);
        NProtoPrivate::TGetStorageConfigFieldsResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        auto storageValues = response.GetStorageConfigFieldsToValues();

        for (const auto& [name, value] : answer) {
            UNIT_ASSERT_VALUES_EQUAL(storageValues[name], value);
        }
    }

    Y_UNIT_TEST(ShouldChangeStorageConfig)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure"},
            {{"CleanupThresholdForBackpressure", "Default"}},
            service);

        {
            // Check that new config was set
            NProto::TStorageConfig newConfig;
            newConfig.SetCleanupThresholdForBackpressure(5);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCleanupThresholdForBackpressure(),
                5);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure"},
            {{"CleanupThresholdForBackpressure", "5"}},
            service);

        {
            // Check that configs are merged, when
            // MergeWithStorageConfigFromTabletDB is true
            NProto::TStorageConfig newConfig;
            newConfig.SetCompactionThresholdForBackpressure(10);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, true);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCleanupThresholdForBackpressure(),
                5);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionThresholdForBackpressure(),
                10);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure",
            "CompactionThresholdForBackpressure"},
            {
                {"CleanupThresholdForBackpressure", "5"},
                {"CompactionThresholdForBackpressure", "10"}
            },
            service);

        {
            // Check that configs aren't merged, when
            // MergeWithStorageConfigFromTabletDB is false
            NProto::TStorageConfig newConfig;
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, false);
            UNIT_ASSERT(
                !response.GetStorageConfig().GetCleanupThresholdForBackpressure());
            UNIT_ASSERT(
                !response.GetStorageConfig().GetCompactionThresholdForBackpressure());
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure", "CompactionThresholdForBackpressure"},
            {
                {"CleanupThresholdForBackpressure", "Default"},
                {"CompactionThresholdForBackpressure", "Default"}
            },
            service);
    }

    Y_UNIT_TEST(ShouldDescribeSessions)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        THeaders headers = {"test", "client", "session", 3};
        service.CreateSession(
            headers,
            "", // checkpointId
            false, // restoreClientSession
            headers.SessionSeqNo);
        service.ResetSession(headers, "some_state");

        headers = {"test", "client2", "session2", 4};
        service.CreateSession(
            headers,
            "", // checkpointId
            false, // restoreClientSession
            headers.SessionSeqNo);
        service.ResetSession(headers, "some_state2");

        NProtoPrivate::TDescribeSessionsRequest request;
        request.SetFileSystemId("test");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("describesessions", buf);
        NProtoPrivate::TDescribeSessionsResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        const auto& sessions = response.GetSessions();
        UNIT_ASSERT_VALUES_EQUAL(2, sessions.size());

        UNIT_ASSERT_VALUES_EQUAL("session", sessions[0].GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL("client", sessions[0].GetClientId());
        UNIT_ASSERT_VALUES_EQUAL("some_state", sessions[0].GetSessionState());
        UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxRwSeqNo());

        UNIT_ASSERT_VALUES_EQUAL("session2", sessions[1].GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL("client2", sessions[1].GetClientId());
        UNIT_ASSERT_VALUES_EQUAL("some_state2", sessions[1].GetSessionState());
        UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxRwSeqNo());
    }

    Y_UNIT_TEST(ShouldRunForcedOperation)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        auto headers = service.InitSession("test", "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, "test", nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        service.WriteData(headers, "test", nodeId, handle, 0, TString(1_MB, 'a'));

        NProtoPrivate::TForcedOperationRequest request;
        request.SetFileSystemId("test");
        request.SetOpType(NProtoPrivate::TForcedOperationRequest::E_COMPACTION);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("forcedoperation", buf);
        NProtoPrivate::TForcedOperationResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());
        UNIT_ASSERT_VALUES_EQUAL(4, response.GetRangeCount());

        env.GetRegistry()->Update(env.GetRuntime().GetCurrentTime());

        const auto counters = env.GetRuntime().GetAppData().Counters;
        auto subgroup = counters->FindSubgroup("counters", "filestore");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("component", "storage_fs");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("host", "cluster");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("filesystem", "test");
        UNIT_ASSERT(subgroup);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            subgroup->GetCounter("Compaction.Count")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldValidateBlockSize)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const ui32 blocks = 1024 * 1024;
        service.SendCreateFileStoreRequest("fs", blocks, 2_KB);

        auto response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 256_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 132_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 128_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldProperlyProcessSlowPipeCreation)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        // delaying pipe creation response
        ui64 tabletId = -1;
        bool caughtClientConnected = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg = event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc = msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                case TEvTabletPipe::EvClientConnected: {
                    const auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                    if (msg->TabletId == tabletId) {
                        if (!caughtClientConnected) {
                            runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                            caughtClientConnected = true;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        // creating session
        service.SendCreateSessionRequest(headers);
        auto response = service.RecvCreateSessionResponse();
        headers.SessionId = response->Record.GetSession().GetSessionId();
        // immediately pinging session to signal that it's not idle
        service.PingSession(headers);

        // just checking that we observed the events that we are expecting
        UNIT_ASSERT_VALUES_UNEQUAL(-1llu, tabletId);
        UNIT_ASSERT(caughtClientConnected);

        // no need to intercept those events anymore
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        bool pipeRestored = false;
        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        const auto* msg =
                            event->Get<TEvTabletPipe::TEvClientConnected>();
                        if (msg->TabletId == tabletId) {
                            pipeRestored = true;
                        }

                        break;
                    }
                }

                return false;
            });

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        // rebooting tablet to destroy the pipe
        tablet.RebootTablet();

        // checking that pipe was reestablished successfully
        UNIT_ASSERT(pipeRestored);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldProperlyProcessSlowSessionCreation)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        // delaying session creation response
        bool rescheduled = false;
        ui32 createSessionResponses = 0;
        TActorId createSessionActor;

        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        createSessionActor = event->Recipient;

                        break;
                    }
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        ++createSessionResponses;

                        if (!rescheduled) {
                            runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                            rescheduled = true;
                            return true;
                        }

                        break;
                    }
                }

                return false;
            });

        // creating session
        service.SendCreateSessionRequest(headers);
        auto response = service.RecvCreateSessionResponse();
        headers.SessionId = response->Record.GetSession().GetSessionId();
        // immediately pinging session to signal that it's not idle
        service.PingSession(headers);

        // just checking that we observed the events that we are expecting
        UNIT_ASSERT(rescheduled);
        UNIT_ASSERT_VALUES_EQUAL(1, createSessionResponses);

        // can't call RebootTablet here because it resets our registration
        // observer and thus disables wakeup event scheduling
        auto msg = std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
            static_cast<ui64>(0),
            TActorId(),
            TActorId());

        runtime.Send(
            new IEventHandle(
                createSessionActor,
                runtime.AllocateEdgeActor(nodeIdx),
                msg.release(),
                0, // flags
                0),
            nodeIdx);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // checking that session was recreated
        UNIT_ASSERT_VALUES_EQUAL(2, createSessionResponses);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(UnsuccessfulSessionActorShouldStopWorking)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        ui32 sessionCreated = 0;
        bool rescheduled = false;

        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvCreateSessionResponse: {
                    if (!rescheduled) {
                        auto* msg = event->Get<TEvIndexTablet::TEvCreateSessionResponse>();
                        *msg->Record.MutableError() = MakeError(E_TIMEOUT, "timeout");

                        runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                        rescheduled = true;
                        return true;
                    }

                    break;
                }

                case TEvServicePrivate::EvSessionCreated: {
                    ++sessionCreated;

                    break;
                }
            }

            return false;
        });

        // creating session
        service.SendCreateSessionRequest(headers);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(rescheduled);
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        auto response = service.RecvCreateSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TIMEOUT,
            response->GetStatus(),
            response->GetErrorReason());

        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // we should have observed exactly 1 CreateSessionResponse
        // if we observe more than 1 it means that our CreateSessionActor
        // remained active after the first failure
        UNIT_ASSERT_VALUES_EQUAL(1, sessionCreated);

        // this time session creation should be successful
        service.SendCreateSessionRequest(headers);
        response = service.RecvCreateSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(2, sessionCreated);
    }

    Y_UNIT_TEST(ShouldFillOriginFqdnWhenCreatingSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvCreateSessionRequest: {
                    const auto* msg =
                        event->Get<TEvIndexTablet::TEvCreateSessionRequest>();
                    UNIT_ASSERT_VALUES_UNEQUAL("", GetOriginFqdn(msg->Record));
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        THeaders headers = {"test", "client", "", 0};
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldPerformTwoStageReads)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetTwoStageReadEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        // fresh bytes
        auto data = TString(100, 'x') + TString(200, 'y') + TString(300, 'z');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        // fresh blocks - adding multiple adjacent blocks is important here to
        // catch some subtle bugs
        data = TString(8_KB, 'a');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        // blobs
        data = TString(1_MB, 'b');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        readDataResult = service.ReadData(
            headers,
            fs,
            nodeId,
            handle,
            DefaultBlockSize,
            data.Size() - DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(
            readDataResult->Record.GetBuffer(),
            data.substr(DefaultBlockSize));

        // mix
        auto patch = TString(4_KB, 'c');
        const ui32 patchOffset = 20_KB;
        service.WriteData(headers, fs, nodeId, handle, patchOffset, patch);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        memcpy(data.begin() + patchOffset, patch.Data(), patch.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", fs)
            ->FindSubgroup("client", "client");
        {
            auto subgroup = counters->FindSubgroup("request", "DescribeData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                5,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                5,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadBlob");
            UNIT_ASSERT(subgroup);
            // 1MB = 4 blobs of 256KB. Read is performed thrice
            UNIT_ASSERT_VALUES_EQUAL(
                12,
                subgroup->GetCounter("Count")->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldFallbackToReadDataIfDescribeDataFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        NProto::TError error;
        error.SetCode(E_REJECTED);
        ui32 describeDataResponses = 0;
        ui32 readDataResponses = 0;

        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvDescribeDataResponse: {
                    using TResponse = TEvIndexTablet::TEvDescribeDataResponse;
                    auto* msg = event->template Get<TResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    ++describeDataResponses;
                    return false;
                }

                case TEvService::EvReadDataResponse: {
                    ++readDataResponses;
                    return false;
                }
            }

            return false;
        });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetTwoStageReadEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        TString data(4_KB, 'A');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        UNIT_ASSERT_VALUES_EQUAL(2, describeDataResponses);
        UNIT_ASSERT_VALUES_EQUAL(4, readDataResponses);
    }

    Y_UNIT_TEST(ShouldFallbackToReadDataIfEvGetFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        ui32 evGets = 0;
        ui32 describeDataResponses = 0;
        ui32 readDataResponses = 0;

        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    using TResponse = TEvBlobStorage::TEvGetResult;
                    auto* msg = event->template Get<TResponse>();
                    ui32 bytes = 0;
                    for (size_t i = 0; i < msg->ResponseSz; ++i) {
                        const auto& response = msg->Responses[i];
                        bytes += response.Buffer.GetSize();
                    }
                    if (bytes == 256_KB) {
                        if (evGets == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        ++evGets;
                    }
                    return false;
                }

                case TEvIndexTablet::EvDescribeDataResponse: {
                    ++describeDataResponses;
                    return false;
                }

                case TEvService::EvReadDataResponse: {
                    ++readDataResponses;
                    return false;
                }
            }

            return false;
        });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetTwoStageReadEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        TString data(1_MB, 'A');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        UNIT_ASSERT_VALUES_EQUAL(2, describeDataResponses);
        UNIT_ASSERT_VALUES_EQUAL(8, evGets);
        UNIT_ASSERT_VALUES_EQUAL(4, readDataResponses);
    }

    Y_UNIT_TEST(ShouldReassignTablet)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        ui64 tabletId = 0;
        ui64 reassignedTabletId = 0;
        TVector<ui32> reassignedChannels;
        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg =
                        event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc =
                        msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    break;
                }

                case NKikimr::TEvHive::EvReassignTablet: {
                    const auto* msg =
                        event->Get<NKikimr::TEvHive::TEvReassignTablet>();
                    reassignedTabletId = msg->Record.GetTabletID();
                    reassignedChannels = {
                        msg->Record.GetChannels().begin(),
                        msg->Record.GetChannels().end()};

                    break;
                }
            }

            return false;
        });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT(headers.SessionId);
        UNIT_ASSERT(tabletId);

        NProtoPrivate::TReassignTabletRequest request;
        request.SetTabletId(tabletId);
        request.AddChannels(1);
        request.AddChannels(4);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("reassigntablet", buf);
        NProtoPrivate::TReassignTabletResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(2, reassignedChannels.size());
        UNIT_ASSERT_VALUES_EQUAL(1, reassignedChannels[0]);
        UNIT_ASSERT_VALUES_EQUAL(4, reassignedChannels[1]);
    }

    TString GenerateValidateData(ui32 size)
    {
        TString data(size, 0);
        for (ui32 i = 0; i < size; ++i) {
            data[i] = 'A' + (i % ('Z' - 'A' + 1));
        }
        return data;
    }

    Y_UNIT_TEST(ShouldPerformThreeStageWrites)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            newConfig.SetThreeStageWriteThreshold(1);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteEnabled(),
                true);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteThreshold(),
                1);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        ui32 putRequestCount = 0;
        TActorId worker;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvPut: {
                        if (event->Sender == worker &&
                            event->Recipient.IsService() &&
                            event->Recipient.ServiceId().StartsWith("bsproxy"))
                        {
                            ++putRequestCount;
                        }
                        break;
                    }
                }
                return false;
            });

        auto& runtime = env.GetRuntime();

        auto validateWriteData =
            [&](ui64 offset, ui64 size, ui32 expectedPutCount)
        {
            auto data = GenerateValidateData(size);

            service.WriteData(headers, fs, nodeId, handle, offset, data);
            auto readDataResult =
                service
                    .ReadData(headers, fs, nodeId, handle, offset, data.Size());
            // clang-format off
            UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
            UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest));
            UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvAddDataRequest));
            UNIT_ASSERT_VALUES_EQUAL(1, runtime.GetCounter(TEvIndexTabletPrivate::EvAddBlobRequest));
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTabletPrivate::EvWriteBlobRequest));
            UNIT_ASSERT_VALUES_EQUAL(1, runtime.GetCounter(TEvService::EvWriteDataResponse));
            UNIT_ASSERT_VALUES_EQUAL(expectedPutCount, putRequestCount);
            // clang-format on
            runtime.ClearCounters();
            putRequestCount = 0;
            worker = TActorId();
        };

        validateWriteData(0, DefaultBlockSize, 1);
        validateWriteData(DefaultBlockSize, DefaultBlockSize, 1);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize, 1);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize * 2, 2);
        validateWriteData(
            DefaultBlockSize,
            DefaultBlockSize * BlockGroupSize * 10,
            11);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize * 3, 3);
        // Currently the data is written from 0th to (1 + BlockGroupSize * 10) = 641th block
        // Therefore, the next write should fail

        auto data =
            GenerateValidateData(DefaultBlockSize * 360);

        auto response =
            service.AssertWriteDataFailed(headers, fs, nodeId, handle, DefaultBlockSize * 641, data);
        auto error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL((ui32)NProto::E_FS_NOSPC, error);

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", fs)
            ->FindSubgroup("client", "client");
        {
            auto subgroup = counters->FindSubgroup("request", "GenerateBlobIds");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                7,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "AddData");
            UNIT_ASSERT(subgroup);
            // Out of 7 writes, only the last one must have failed
            UNIT_ASSERT_VALUES_EQUAL(
                6,
                subgroup->GetCounter("Count")->GetAtomic());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                subgroup->GetCounter("Errors")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                7,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteBlob");
            UNIT_ASSERT(subgroup);
            // Total number of put requests should have been 1 + 1 + 1 + 2 + 11
            // + 3 + ceil(360 / 64) = 25
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                subgroup->GetCounter("Count")->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldNotUseThreeStageWriteForSmallOrUnalignedRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        auto& runtime = env.GetRuntime();

        auto validateWriteData = [&](ui64 offset, ui64 size)
        {
            auto data = GenerateValidateData(size);

            service.WriteData(headers, fs, nodeId, handle, offset, data);
            auto readDataResult =
                service
                    .ReadData(headers, fs, nodeId, handle, offset, data.Size());
            // clang-format off
            UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest));
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvAddDataRequest));
            UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataRequest));
            // clang-format on
            runtime.ClearCounters();
        };

        validateWriteData(0, 4_KB);
        validateWriteData(4_KB, 4_KB);
        validateWriteData(1, 128_KB);
    }

    Y_UNIT_TEST(ShouldFallbackThreeStageWriteToSimpleWrite)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        NProto::TError error;
        error.SetCode(E_REJECTED);

        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsResponse: {
                        auto* msg = event->template Get<
                            TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                        msg->Record.MutableError()->CopyFrom(error);
                        break;
                    }
                }
                return false;
            });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        // GenerateBlobIdsResponse fails
        TString data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        auto& runtime = env.GetRuntime();
        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        // clang-format on
        runtime.ClearCounters();

        // AddDataResponse fails
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvAddDataResponse: {
                        auto* msg = event->template Get<
                            TEvIndexTablet::TEvAddDataResponse>();
                        msg->Record.MutableError()->CopyFrom(error);
                        break;
                    }
                }
                return false;
            });
        data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvAddDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        // clang-format on

        // TEvGet fails

        runtime.ClearCounters();

        TActorId worker;
        ui32 evPuts = 0;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvPutResult: {
                        auto* msg =
                            event->template Get<TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker) {
                            if (evPuts == 0) {
                                msg->Status = NKikimrProto::ERROR;
                            }
                            ++evPuts;
                        }
                        break;
                    }
                }

                return false;
            });

        data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.Size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvAddDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(1, evPuts);
        // clang-format on
    }
}

}   // namespace NCloud::NFileStore::NStorage
