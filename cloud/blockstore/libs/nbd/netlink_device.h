#pragma once

#include "device.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure,
    bool disconnect);

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure,
    bool disconnect);

}   // namespace NCloud::NBlockStore::NBD
