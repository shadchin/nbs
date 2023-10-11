package nbs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type multiZoneClient struct {
	srcZoneClient *client
	dstZoneClient *client
	metrics       *clientMetrics
}

////////////////////////////////////////////////////////////////////////////////

func (c *multiZoneClient) Clone(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) (err error) {

	defer c.metrics.StatRequest("Clone")(&err)

	retries := 0
	for {
		err = c.clone(ctx, diskID, fillGeneration)
		if err != nil {
			if !isAbortedError(err) {
				return err
			}

			if fillGeneration > 1 {
				volume, err := c.dstZoneClient.nbs.DescribeVolume(ctx, diskID)
				if err != nil {
					return wrapError(err)
				}

				if volume.IsFillFinished {
					return errors.NewNonRetriableErrorf(
						"can't replace disk %v because filling is finished",
						diskID,
					)
				}

				err = c.dstZoneClient.DeleteWithFillGeneration(
					ctx,
					diskID,
					fillGeneration-1,
				)
				if err != nil {
					return err
				}
			}

			if retries == maxConsecutiveRetries {
				return errors.NewRetriableError(err)
			}

			retries++
			continue
		}

		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *multiZoneClient) clone(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) (err error) {

	volume, err := c.srcZoneClient.nbs.DescribeVolume(ctx, diskID)
	if err != nil {
		return wrapError(err)
	}

	err = c.dstZoneClient.nbs.CreateVolume(
		ctx,
		volume.DiskId,
		volume.BlocksCount,
		&nbs_client.CreateVolumeOpts{
			// TODO: Now we make every disk non-overlay after migration. Will be optimized in NBS-4222
			BaseDiskId:              "",
			BaseDiskCheckpointId:    "",
			BlockSize:               volume.BlockSize,
			StorageMediaKind:        volume.StorageMediaKind,
			CloudId:                 volume.CloudId,
			FolderId:                volume.FolderId,
			TabletVersion:           volume.TabletVersion,
			PlacementGroupId:        volume.PlacementGroupId,
			PlacementPartitionIndex: volume.PlacementPartitionIndex,
			PartitionsCount:         volume.PartitionsCount,
			IsSystem:                volume.IsSystem,
			ProjectId:               volume.ProjectId,
			ChannelsCount:           volume.ChannelsCount,
			EncryptionSpec: &protos.TEncryptionSpec{
				Mode: volume.EncryptionDesc.Mode,
				KeyParam: &protos.TEncryptionSpec_KeyHash{
					KeyHash: volume.EncryptionDesc.KeyHash,
				},
			},
			// TODO: NBS-3679: provide these parameters correctly
			StoragePoolName: "",
			AgentIds:        []string{},
			FillGeneration:  fillGeneration,
		},
	)
	return wrapError(err)
}