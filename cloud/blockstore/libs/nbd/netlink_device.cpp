#include "client_handler.h"
#include "netlink.h"
#include "netlink_device.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NBD {

namespace {

#undef NLA_PUT
#define NLA_PUT(msg, attrtype, attrlen, data)                          \
    do {                                                               \
        if (nla_put(msg, attrtype, attrlen, data) < 0) {               \
            throw TServiceError(E_FAIL) << "unable to put " #attrtype; \
        }                                                              \
    } while (0)

////////////////////////////////////////////////////////////////////////////////

class TNetlinkSocket
{
private:
    nl_sock* Socket;
    int Family_;

public:
    TNetlinkSocket()
    {
        Socket = nl_socket_alloc();

        if (Socket == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate netlink socket";
        }

        if (genl_connect(Socket)) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to connect generic netlink socket";
        }

        Family_ = genl_ctrl_resolve(Socket, "nbd");
        if (Family_ < 0) {
            throw TServiceError(E_FAIL)
                << "unable to resolve nbd netlink "
                   "family, make sure nbd module is loaded";
        }
    }

    ~TNetlinkSocket()
    {
        nl_socket_free(Socket);
    }

    operator nl_sock*() const
    {
        return Socket;
    }

    int Family() const
    {
        return Family_;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final: public IDevice
{
private:
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DeviceName;
    const TDuration Timeout;
    const TDuration DeadConnectionTimeout;
    const bool Reconfigure;
    const bool Disconnect;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    ui32 DeviceIndex;

    TAtomic ShouldStop = 0;

public:
    TNetlinkDevice(
            ILoggingServicePtr logging,
            TNetworkAddress connectAddress,
            TString deviceName,
            TDuration timeout,
            TDuration deadConnectionTimeout,
            bool reconfigure,
            bool disconnect)
        : Logging(std::move(logging))
        , ConnectAddress(std::move(connectAddress))
        , DeviceName(std::move(deviceName))
        , Timeout(timeout)
        , DeadConnectionTimeout(deadConnectionTimeout)
        , Reconfigure(reconfigure)
        , Disconnect(disconnect)
    {
        Log = Logging->CreateLog("BLOCKSTORE_NBD");

        if (sscanf(DeviceName.c_str(), "/dev/nbd%u", &DeviceIndex) != 1) {
            throw TServiceError(E_FAIL) << "invalid nbd device target";
        }
    }

    ~TNetlinkDevice()
    {
        Stop();
    }

    void Start() override
    {
        ConnectSocket();
        ConnectDeviceAsync();
    }

    void Stop() override
    {
        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return;
        }

        if (Disconnect) {
            DisconnectDevice();
            DisconnectSocket();
        }
    }

private:
    void ConnectSocket();
    void DisconnectSocket();

    void ConnectDevice(bool connected);
    void ConnectDeviceAsync();
    void DisconnectDevice();

    static int StatusHandler(nl_msg* msg, void* arg);
};

////////////////////////////////////////////////////////////////////////////////

void TNetlinkDevice::ConnectSocket()
{
    STORAGE_DEBUG("connect socket");

    TSocket socket(ConnectAddress);
    if (IsTcpAddress(ConnectAddress)) {
        socket.SetNoDelay(true);
    }

    TSocketInput in(socket);
    TSocketOutput out(socket);

    Handler = CreateClientHandler(Logging);
    Y_ENSURE(Handler->NegotiateClient(in, out));

    Socket = socket;
}

void TNetlinkDevice::DisconnectSocket()
{
    STORAGE_DEBUG("disconnect socket");

    Socket.Close();
}

void TNetlinkDevice::ConnectDevice(bool connected)
{
    auto cmd = NBD_CMD_CONNECT;

    if (connected) {
        if (!Reconfigure) {
            throw TServiceError(E_FAIL) << DeviceName << " is busy";
        }
        STORAGE_INFO("reconfigure " << DeviceName);
        cmd = NBD_CMD_RECONFIGURE;
    } else {
        STORAGE_INFO("connect " << DeviceName);
    }

    TNetlinkSocket nlsocket;

    auto* msg = nlmsg_alloc();
    if (msg == nullptr) {
        throw TServiceError(E_FAIL) << "unable to allocate netlink message";
    }
    genlmsg_put(
        msg,
        NL_AUTO_PORT,
        NL_AUTO_SEQ,
        nlsocket.Family(),
        0,
        0,
        cmd,
        0);

    const auto& info = Handler->GetExportInfo();
    if (DeviceIndex >= 0) {
        NLA_PUT_U32(msg, NBD_ATTR_INDEX, DeviceIndex);
    }
    NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, info.Size);
    NLA_PUT_U64(msg, NBD_ATTR_BLOCK_SIZE_BYTES, info.MinBlockSize);
    NLA_PUT_U64(msg, NBD_ATTR_SERVER_FLAGS, info.Flags);
    if (Timeout) {
        NLA_PUT_U64(msg, NBD_ATTR_TIMEOUT, Timeout.Seconds());
    }
    if (DeadConnectionTimeout) {
        NLA_PUT_U64(
            msg,
            NBD_ATTR_DEAD_CONN_TIMEOUT,
            DeadConnectionTimeout.Seconds());
    }

    auto* attr = nla_nest_start(msg, NBD_ATTR_SOCKETS);
    if (!attr) {
        throw TServiceError(E_FAIL) << "unable to nest NBD_ATTR_SOCKETS";
    }
    auto* opt = nla_nest_start(msg, NBD_SOCK_ITEM);
    if (!opt) {
        throw TServiceError(E_FAIL) << "unable to nest NBD_SOCK_ITEM";
    }
    NLA_PUT_U32(msg, NBD_SOCK_FD, static_cast<ui32>(Socket));
    nla_nest_end(msg, opt);
    nla_nest_end(msg, attr);

    if (nl_send_sync(nlsocket, msg) < 0) {
        throw TServiceError(E_FAIL) << "failed to setup device, check dmesg";
    }
}

void TNetlinkDevice::DisconnectDevice()
{
    STORAGE_INFO("disconnect " << DeviceName);

    TNetlinkSocket nlsocket;

    auto* msg = nlmsg_alloc();
    if (!msg) {
        throw TServiceError(E_FAIL) << "unable to allocate netlink message";
    }
    genlmsg_put(
        msg,
        NL_AUTO_PORT,
        NL_AUTO_SEQ,
        nlsocket.Family(),
        0,
        0,
        NBD_CMD_DISCONNECT,
        0);

    NLA_PUT_U32(msg, NBD_ATTR_INDEX, DeviceIndex);

    if (nl_send_sync(nlsocket, msg) < 0) {
        throw TServiceError(E_FAIL) << "unable to to disconnect device";
    }
}

// queries device status and registers callback that will connect
// or reconfigure (if Reconfigure == true) specified device
void TNetlinkDevice::ConnectDeviceAsync()
{
    TNetlinkSocket nlsocket;

    nl_socket_modify_cb(
        nlsocket,
        NL_CB_VALID,
        NL_CB_CUSTOM,
        TNetlinkDevice::StatusHandler,
        this);

    auto* msg = nlmsg_alloc();
    if (msg == nullptr) {
        throw TServiceError(E_FAIL) << "unable to allocate netlink message";
    }
    genlmsg_put(
        msg,
        NL_AUTO_PORT,
        NL_AUTO_SEQ,
        nlsocket.Family(),
        0,
        0,
        NBD_CMD_STATUS,
        0);

    if (DeviceIndex >= 0) {
        NLA_PUT_U32(msg, NBD_ATTR_INDEX, DeviceIndex);
    }

    if (nl_send_sync(nlsocket, msg) < 0) {
        throw TServiceError(E_FAIL)
            << "failed to configure device, check dmesg";
    }
}

int TNetlinkDevice::StatusHandler(nl_msg* msg, void* arg)
{
    auto* header = static_cast<genlmsghdr*>(nlmsg_data(nlmsg_hdr(msg)));
    auto* conn = static_cast<TNetlinkDevice*>(arg);
    auto Log = conn->Log;
    nlattr* attr[NBD_ATTR_MAX + 1];
    nlattr* deviceItem[NBD_DEVICE_ITEM_MAX + 1];
    nlattr* device[NBD_DEVICE_ATTR_MAX + 1];

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc99-designator"
    nla_policy deviceItemPolicy[NBD_DEVICE_ITEM_MAX + 1] = {
        [NBD_DEVICE_ITEM] = {.type = NLA_NESTED},
    };
    nla_policy devicePolicy[NBD_DEVICE_ATTR_MAX + 1] = {
        [NBD_DEVICE_INDEX] = {.type = NLA_U32},
        [NBD_DEVICE_CONNECTED] = {.type = NLA_U8},
    };
#pragma clang diagnostic pop

    if (nla_parse(
            attr,
            NBD_ATTR_MAX,
            genlmsg_attrdata(header, 0),
            genlmsg_attrlen(header, 0),
            NULL))
    {
        STORAGE_ERROR("unable to parse NBD_CMD_STATUS response");
        return NL_OK;
    }

    if (!attr[NBD_ATTR_DEVICE_LIST]) {
        STORAGE_ERROR("did not receive NBD_ATTR_DEVICE_LIST");
        return NL_OK;
    }

    if (nla_parse_nested(
            deviceItem,
            NBD_DEVICE_ITEM_MAX,
            attr[NBD_ATTR_DEVICE_LIST],
            deviceItemPolicy))
    {
        STORAGE_ERROR("unable to parse NBD_ATTR_DEVICE_LIST");
        return NL_OK;
    }

    if (!deviceItem[NBD_DEVICE_ITEM]) {
        STORAGE_ERROR("did not receive NBD_DEVICE_ITEM");
        return NL_OK;
    }

    if (nla_parse_nested(
            device,
            NBD_DEVICE_ATTR_MAX,
            deviceItem[NBD_DEVICE_ITEM],
            devicePolicy))
    {
        STORAGE_ERROR("unable to parse NBD_DEVICE_ITEM");
        return NL_OK;
    }

    if (!device[NBD_DEVICE_CONNECTED]) {
        STORAGE_ERROR("did not receive NBD_DEVICE_CONNECTED");
        return NL_OK;
    }

    conn->ConnectDevice(nla_get_u32(device[NBD_DEVICE_CONNECTED]));

    return NL_OK;
}

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration Timeout;
    const TDuration DeadConnectionTimeout;
    const bool Reconfigure;
    const bool Disconnect;

public:
    TNetlinkDeviceFactory(
            ILoggingServicePtr logging,
            TDuration timeout,
            TDuration deadConnectionTimeout,
            bool reconfigure,
            bool disconnect)
        : Logging(std::move(logging))
        , Timeout(std::move(timeout))
        , DeadConnectionTimeout(std::move(deadConnectionTimeout))
        , Reconfigure(reconfigure)
        , Disconnect(disconnect)
    {}

    IDevicePtr Create(
        TNetworkAddress connectAddress,
        TString deviceName) override
    {
        return CreateNetlinkDevice(
            Logging,
            std::move(connectAddress),
            std::move(deviceName),
            Timeout,
            DeadConnectionTimeout,
            Reconfigure,
            Disconnect);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure,
    bool disconnect)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        std::move(deviceName),
        timeout,
        deadConnectionTimeout,
        reconfigure,
        disconnect);
}

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure,
    bool disconnect)
{
    return std::make_shared<TNetlinkDeviceFactory>(
        std::move(logging),
        std::move(timeout),
        std::move(deadConnectionTimeout),
        reconfigure,
        disconnect);
}

}   // namespace NCloud::NBlockStore::NBD
