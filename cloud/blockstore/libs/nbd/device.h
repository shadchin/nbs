#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IDevice
    : public IStartable
{
};

////////////////////////////////////////////////////////////////////////////////

struct IDeviceFactory
{
    virtual ~IDeviceFactory() = default;

    virtual IDevicePtr Create(
        TNetworkAddress connectAddress,
        TString deviceName) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout);

IDevicePtr CreateDeviceStub();

IDeviceFactoryPtr CreateDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NBD
