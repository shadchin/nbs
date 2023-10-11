package client

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	sdk_client_config "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type WaitableClient interface {
	WaitOperation(
		ctx context.Context,
		operationID string,
		callback func(context.Context, *operation.Operation) error,
	) (*operation.Operation, error)
}

func WaitOperation(
	ctx context.Context,
	client WaitableClient,
	operationID string,
) error {

	return WaitResponse(ctx, client, operationID, nil)
}

func WaitResponse(
	ctx context.Context,
	client WaitableClient,
	operationID string,
	response proto.Message,
) error {

	logging.Debug(ctx, "Waiting for operation %v", operationID)

	o, err := client.WaitOperation(
		ctx,
		operationID,
		func(context.Context, *operation.Operation) error { return nil },
	)
	if err != nil {
		return err
	}

	if !o.Done {
		return errors.NewNonRetriableErrorf(
			"operation %v should be finished",
			o,
		)
	}

	switch result := o.Result.(type) {
	case *operation.Operation_Error:
		return grpc_status.ErrorProto(result.Error)
	case *operation.Operation_Response:
		if response != nil {
			err = ptypes.UnmarshalAny(result.Response, response)
			return err
		}

		logging.Debug(ctx, "Operation %v finished", operationID)
		return nil
	default:
		return errors.NewNonRetriableErrorf(
			"unknown operation result type %v",
			result,
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

func GetOperationMetadata(
	ctx context.Context,
	client sdk_client.Client,
	operationID string,
	metadata proto.Message,
) error {

	o, err := client.GetOperation(
		ctx,
		&disk_manager.GetOperationRequest{
			OperationId: operationID,
		},
	)
	if err != nil {
		return err
	}

	if o.Metadata != nil {
		return ptypes.UnmarshalAny(o.Metadata, metadata)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type PrivateClient interface {
	WaitOperation(
		ctx context.Context,
		operationID string,
		callback func(context.Context, *operation.Operation) error,
	) (*operation.Operation, error)

	// Used for testing.
	ScheduleBlankOperation(ctx context.Context) (*operation.Operation, error)

	RebaseOverlayDisk(
		ctx context.Context,
		req *api.RebaseOverlayDiskRequest,
	) (*operation.Operation, error)

	RetireBaseDisk(
		ctx context.Context,
		req *api.RetireBaseDiskRequest,
	) (*operation.Operation, error)

	RetireBaseDisks(
		ctx context.Context,
		req *api.RetireBaseDisksRequest,
	) (*operation.Operation, error)

	OptimizeBaseDisks(ctx context.Context) (*operation.Operation, error)

	ConfigurePool(
		ctx context.Context,
		req *api.ConfigurePoolRequest,
	) (*operation.Operation, error)

	DeletePool(
		ctx context.Context,
		req *api.DeletePoolRequest,
	) (*operation.Operation, error)

	ListDisks(
		ctx context.Context,
		req *api.ListDisksRequest,
	) (*api.ListDisksResponse, error)

	ListImages(
		ctx context.Context,
		req *api.ListImagesRequest,
	) (*api.ListImagesResponse, error)

	ListSnapshots(
		ctx context.Context,
		req *api.ListSnapshotsRequest,
	) (*api.ListSnapshotsResponse, error)

	ListFilesystems(
		ctx context.Context,
		req *api.ListFilesystemsRequest,
	) (*api.ListFilesystemsResponse, error)

	ListPlacementGroups(
		ctx context.Context,
		req *api.ListPlacementGroupsRequest,
	) (*api.ListPlacementGroupsResponse, error)

	GetAliveNodes(
		ctx context.Context,
	) (*api.GetAliveNodesResponse, error)

	Close() error
}

type privateClient struct {
	operationPollPeriod    time.Duration
	operationServiceClient disk_manager.OperationServiceClient
	privateServiceClient   api.PrivateServiceClient
	conn                   *grpc.ClientConn
}

func (c *privateClient) WaitOperation(
	ctx context.Context,
	operationID string,
	callback func(context.Context, *operation.Operation) error,
) (*operation.Operation, error) {

	for {
		o, err := c.operationServiceClient.Get(ctx, &disk_manager.GetOperationRequest{
			OperationId: operationID,
		})
		if err != nil {
			return nil, err
		}

		if callback != nil {
			err = callback(ctx, o)
			if err != nil {
				return nil, err
			}
		}

		if o.Done {
			return o, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.operationPollPeriod):
		}
	}
}

func (c *privateClient) ScheduleBlankOperation(
	ctx context.Context,
) (*operation.Operation, error) {

	return c.privateServiceClient.ScheduleBlankOperation(ctx, &empty.Empty{})
}

func (c *privateClient) RebaseOverlayDisk(
	ctx context.Context,
	req *api.RebaseOverlayDiskRequest,
) (*operation.Operation, error) {

	return c.privateServiceClient.RebaseOverlayDisk(ctx, req)
}

func (c *privateClient) RetireBaseDisk(
	ctx context.Context,
	req *api.RetireBaseDiskRequest,
) (*operation.Operation, error) {

	return c.privateServiceClient.RetireBaseDisk(ctx, req)
}

func (c *privateClient) RetireBaseDisks(
	ctx context.Context,
	req *api.RetireBaseDisksRequest,
) (*operation.Operation, error) {

	return c.privateServiceClient.RetireBaseDisks(ctx, req)
}

func (c *privateClient) OptimizeBaseDisks(
	ctx context.Context,
) (*operation.Operation, error) {

	return c.privateServiceClient.OptimizeBaseDisks(ctx, &empty.Empty{})
}

func (c *privateClient) ConfigurePool(
	ctx context.Context,
	req *api.ConfigurePoolRequest,
) (*operation.Operation, error) {

	return c.privateServiceClient.ConfigurePool(ctx, req)
}

func (c *privateClient) DeletePool(
	ctx context.Context,
	req *api.DeletePoolRequest,
) (*operation.Operation, error) {

	return c.privateServiceClient.DeletePool(ctx, req)
}

func (c *privateClient) ListDisks(
	ctx context.Context,
	req *api.ListDisksRequest,
) (*api.ListDisksResponse, error) {

	return c.privateServiceClient.ListDisks(ctx, req)
}

func (c *privateClient) ListImages(
	ctx context.Context,
	req *api.ListImagesRequest,
) (*api.ListImagesResponse, error) {

	return c.privateServiceClient.ListImages(ctx, req)
}

func (c *privateClient) ListSnapshots(
	ctx context.Context,
	req *api.ListSnapshotsRequest,
) (*api.ListSnapshotsResponse, error) {

	return c.privateServiceClient.ListSnapshots(ctx, req)
}

func (c *privateClient) ListFilesystems(
	ctx context.Context,
	req *api.ListFilesystemsRequest,
) (*api.ListFilesystemsResponse, error) {

	return c.privateServiceClient.ListFilesystems(ctx, req)
}

func (c *privateClient) ListPlacementGroups(
	ctx context.Context,
	req *api.ListPlacementGroupsRequest,
) (*api.ListPlacementGroupsResponse, error) {

	return c.privateServiceClient.ListPlacementGroups(ctx, req)
}

func (c *privateClient) GetAliveNodes(
	ctx context.Context,
) (*api.GetAliveNodesResponse, error) {

	return c.privateServiceClient.GetAliveNodes(ctx, new(empty.Empty))
}

func (c *privateClient) Close() error {
	return c.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////

func getDialOptions(config *client_config.ClientConfig) ([]grpc.DialOption, error) {
	options := make([]grpc.DialOption, 0)

	if config.GetInsecure() {
		return append(options, grpc.WithInsecure()), nil
	}

	cp, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	if len(config.GetServerCertFile()) != 0 {
		bytes, err := os.ReadFile(config.GetServerCertFile())
		if err != nil {
			return nil, errors.NewNonRetriableError(err)
		}

		if !cp.AppendCertsFromPEM(bytes) {
			return nil, errors.NewNonRetriableErrorf(
				"failed to append certificate %v",
				config.GetServerCertFile(),
			)
		}
	}

	return append(
		options,
		grpc.WithTransportCredentials(
			credentials.NewClientTLSFromCert(cp, ""),
		),
	), nil
}

func getEndpoint(config *client_config.ClientConfig) (string, error) {
	host := config.GetHost()
	if len(host) == 0 {
		var err error
		host, err = os.Hostname()
		if err != nil {
			return "", errors.NewNonRetriableError(err)
		}
	}

	return fmt.Sprintf("%v:%v", host, config.GetPort()), nil
}

////////////////////////////////////////////////////////////////////////////////

func NewClient(
	ctx context.Context,
	config *client_config.ClientConfig,
) (sdk_client.Client, error) {

	options, err := getDialOptions(config)
	if err != nil {
		return nil, err
	}

	endpoint, err := getEndpoint(config)
	if err != nil {
		return nil, err
	}

	interceptor := func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			logging.Warn(ctx, "%v", err)
		}

		return err
	}

	options = append(options, grpc.WithChainUnaryInterceptor(interceptor))

	maxRetryAttempts := config.GetMaxRetryAttempts()
	perRetryTimeout := config.GetPerRetryTimeout()
	backoffTimeout := config.GetBackoffTimeout()
	operationPollPeriod := config.GetOperationPollPeriod()

	client, err := sdk_client.NewClient(
		ctx,
		&sdk_client_config.Config{
			Endpoint:            &endpoint,
			MaxRetryAttempts:    &maxRetryAttempts,
			PerRetryTimeout:     &perRetryTimeout,
			BackoffTimeout:      &backoffTimeout,
			OperationPollPeriod: &operationPollPeriod,
		},
		options...,
	)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return client, nil
}

func NewPrivateClient(
	ctx context.Context,
	config *sdk_client_config.Config,
	options ...grpc.DialOption,
) (PrivateClient, error) {

	perRetryTimeout, err := time.ParseDuration(config.GetPerRetryTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	backoffTimeout, err := time.ParseDuration(config.GetBackoffTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	operationPollPeriod, err := time.ParseDuration(config.GetOperationPollPeriod())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	interceptor := grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithMax(uint(config.GetMaxRetryAttempts())),
		grpc_retry.WithPerRetryTimeout(perRetryTimeout),
		grpc_retry.WithBackoff(func(attempt uint) time.Duration { return backoffTimeout }),
	)
	options = append(options, grpc.WithChainUnaryInterceptor(interceptor))

	conn, err := grpc.DialContext(ctx, config.GetEndpoint(), options...)
	if err != nil {
		return nil, err
	}

	return &privateClient{
		operationPollPeriod:    operationPollPeriod,
		operationServiceClient: disk_manager.NewOperationServiceClient(conn),
		privateServiceClient:   api.NewPrivateServiceClient(conn),
		conn:                   conn,
	}, nil
}

// TODO: naming should be better, because this function is used not only in CLI.
func NewPrivateClientForCLI(
	ctx context.Context,
	config *client_config.ClientConfig,
) (PrivateClient, error) {

	options, err := getDialOptions(config)
	if err != nil {
		return nil, err
	}

	endpoint, err := getEndpoint(config)
	if err != nil {
		return nil, err
	}

	maxRetryAttempts := config.GetMaxRetryAttempts()
	perRetryTimeout := config.GetPerRetryTimeout()
	backoffTimeout := config.GetBackoffTimeout()
	operationPollPeriod := config.GetOperationPollPeriod()

	return NewPrivateClient(
		ctx,
		&sdk_client_config.Config{
			Endpoint:            &endpoint,
			MaxRetryAttempts:    &maxRetryAttempts,
			PerRetryTimeout:     &perRetryTimeout,
			BackoffTimeout:      &backoffTimeout,
			OperationPollPeriod: &operationPollPeriod,
		},
		options...,
	)
}