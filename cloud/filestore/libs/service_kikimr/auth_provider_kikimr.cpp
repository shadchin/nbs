#include "auth_provider_kikimr.h"

#include <cloud/filestore/libs/service/auth_provider.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/api/authorizer.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>

namespace NCloud::NFileStore {

namespace {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NStorage;
using namespace NCloud::NFileStore::NStorage;

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

class TRequestActor final
    : public TActorBootstrapped<TRequestActor>
{
    using TThis = TRequestActor;
    using TBase = TActorBootstrapped<TThis>;

private:
    TPermissionList Permissions;
    TString AuthToken;
    TPromise<NProto::TError> Response;
    TCallContextPtr CallContext;

    const TDuration RequestTimeout;

    bool RequestCompleted = false;

public:
    TRequestActor(
            const TPermissionList& permissions,
            TString authToken,
            TPromise<NProto::TError> response,
            TCallContextPtr callContext,
            TDuration requestTimeout)
        : Permissions(permissions)
        , AuthToken(std::move(authToken))
        , Response(std::move(response))
        , CallContext(std::move(callContext))
        , RequestTimeout(requestTimeout)
    {
        TThis::ActivityType = TFileStoreActivities::SERVICE_PROXY;
    }

    ~TRequestActor() override
    {
        if (!RequestCompleted) {
            auto error = NProto::TError();
            error.SetCode(E_REJECTED);

            try {
                Response.SetValue(std::move(error));
            } catch (...) {
                // no way to log error message
            }

            RequestCompleted = true;
        }
    }

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);

        AuthorizeRequest(ctx);
    }

private:
    void AuthorizeRequest(const TActorContext& ctx)
    {
        auto request = std::make_unique<TEvAuth::TEvAuthorizationRequest>(
            std::move(AuthToken),
            std::move(Permissions));

        LWTRACK(
            AuthRequestSent_Proxy,
            CallContext->LWOrbit,
            GetFileStoreRequestName(CallContext->RequestType),
            CallContext->RequestId);

        NCloud::Send(
            ctx,
            MakeAuthorizerServiceId(),
            std::move(request));

        if (RequestTimeout && RequestTimeout != TDuration::Max()) {
            ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
        }
    }

    void CompleteRequest(const TActorContext& ctx, NProto::TError response)
    {
        try {
            Response.SetValue(std::move(response));
        } catch (...) {}

        RequestCompleted = true;
        TThis::Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvAuth::TEvAuthorizationResponse, HandleAuthResponse);
            HFunc(TEvents::TEvWakeup, HandleTimeout);

            default:
                HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_PROXY);
                break;
        }
    }

    void HandleAuthResponse(
        const TEvAuth::TEvAuthorizationResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        LWTRACK(
            AuthResponseReceived_Proxy,
            CallContext->LWOrbit,
            GetFileStoreRequestName(CallContext->RequestType),
            CallContext->RequestId);

        CompleteRequest(ctx, msg->Error);
    }

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        NProto::TError error;
        error.SetCode(E_REJECTED);  // TODO: E_TIMEOUT
        error.SetMessage("Timeout");

        CompleteRequest(ctx, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAuthProvider final
    : public IAuthProvider
{
private:
    const IActorSystemPtr ActorSystem;

public:
    TAuthProvider(IActorSystemPtr actorSystem)
        : ActorSystem(std::move(actorSystem))
    {}

    bool NeedAuth(
        NCloud::NProto::ERequestSource requestSource,
        const TPermissionList& permissions) override
    {
        // Data channel does not need IAM authorization:
        // all requests are allowed if authorized with mount tokens.
        return !IsDataChannel(requestSource) && !permissions.Empty();
    }

    TFuture<NProto::TError> CheckRequest(
        TCallContextPtr callContext,
        TPermissionList permissions,
        TString authToken,
        TDuration requestTimeout) override
    {
        auto response = NewPromise<NProto::TError>();

        ActorSystem->Register(std::make_unique<TRequestActor>(
            std::move(permissions),
            std::move(authToken),
            response,
            std::move(callContext),
            requestTimeout));

        return response.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IAuthProviderPtr CreateKikimrAuthProvider(IActorSystemPtr actorSystem)
{
    return std::make_shared<TAuthProvider>(std::move(actorSystem));
}

}   // namespace NCloud::NFileStore