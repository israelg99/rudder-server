package warehouse

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
)

type warehousegrpc struct {
	proto.UnimplementedWarehouseServer
}

func (w *warehousegrpc) GetWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.WHUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:     request.WorkspaceId,
		SourceID:        request.SourceId,
		DestinationID:   request.DestinationId,
		DestinationType: request.DestinationType,
		Status:          request.Status,
		Limit:           request.Limit,
		Offset:          request.Offset,
		API:             UploadAPI,
	}
	res, err := uploadsReq.GetWhUploads()
	return res, err
}

func (w *warehousegrpc) TriggerWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:   request.WorkspaceId,
		SourceID:      request.SourceId,
		DestinationID: request.DestinationId,
		API:           UploadAPI,
	}
	res, err := uploadsReq.TriggerWhUploads()
	return res, err
}

func (w *warehousegrpc) GetWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.WHUploadResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.GetWHUpload()
	return res, err
}

func (w *warehousegrpc) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return wrapperspb.Bool(UploadAPI.enabled), nil
}

func (w *warehousegrpc) TriggerWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.TriggerWHUpload()
	return res, err
}

func (w *warehousegrpc) Validate(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	handleT := configuration_testing.CTHandleT{}
	return handleT.Validating(req)
}

func (w *warehousegrpc) RetryWHUploads(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	retryReq := &RetryRequest{
		WorkspaceID:     req.WorkspaceId,
		SourceID:        req.SourceId,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		IntervalInHours: req.IntervalInHours,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		API:             UploadAPI,
	}
	r, err := retryReq.RetryWHUploads(ctx)
	response = &proto.RetryWHUploadsResponse{
		Message:    r.Message,
		StatusCode: r.StatusCode,
	}
	return
}

func (w *warehousegrpc) CountWHUploadsToRetry(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.CountWHUploadsToRetryResponse, err error) {
	retryReq := &RetryRequest{
		WorkspaceID:     req.WorkspaceId,
		SourceID:        req.SourceId,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		IntervalInHours: req.IntervalInHours,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		API:             UploadAPI,
	}
	r, err := retryReq.UploadsToRetry(ctx)
	response = &proto.CountWHUploadsToRetryResponse{
		Count:      r.Count,
		Message:    r.Message,
		StatusCode: r.StatusCode,
	}
	return
}
