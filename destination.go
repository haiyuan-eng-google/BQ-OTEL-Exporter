// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"

	bq "cloud.google.com/go/bigquery"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// destinationAdmin is the narrow control-plane boundary used during Start.
// It deliberately excludes row writes so provisioning cannot accidentally
// become part of the data path.
type destinationAdmin interface {
	DatasetMetadata(context.Context) (*bq.DatasetMetadata, error)
	CreateDataset(context.Context, *bq.DatasetMetadata) error
	TableMetadata(context.Context, string) (*bq.TableMetadata, error)
	CreateTable(context.Context, string, *bq.TableMetadata) error
	Close() error
}

type bigQueryDestinationAdmin struct {
	client   *bq.Client
	httpBase *stdhttp.Transport
	dataset  *bq.Dataset
}

func (a *bigQueryDestinationAdmin) DatasetMetadata(ctx context.Context) (*bq.DatasetMetadata, error) {
	return a.dataset.Metadata(ctx)
}

func (a *bigQueryDestinationAdmin) CreateDataset(ctx context.Context, md *bq.DatasetMetadata) error {
	return a.dataset.Create(ctx, md)
}

func (a *bigQueryDestinationAdmin) TableMetadata(ctx context.Context, id string) (*bq.TableMetadata, error) {
	return a.dataset.Table(id).Metadata(ctx)
}

func (a *bigQueryDestinationAdmin) CreateTable(ctx context.Context, id string, md *bq.TableMetadata) error {
	return a.dataset.Table(id).Create(ctx, md)
}

func (a *bigQueryDestinationAdmin) Close() error {
	err := a.client.Close()
	a.httpBase.CloseIdleConnections()
	return err
}

var newBigQueryHTTPClient = func(
	ctx context.Context, opts ...option.ClientOption,
) (*stdhttp.Client, *stdhttp.Transport, error) {
	defaultTransport, ok := stdhttp.DefaultTransport.(*stdhttp.Transport)
	if !ok {
		return nil, nil, errors.New("default HTTP transport is not configurable")
	}
	base := defaultTransport.Clone()
	authenticated, err := htransport.NewTransport(ctx, base, opts...)
	if err != nil {
		base.CloseIdleConnections()
		return nil, nil, err
	}
	return &stdhttp.Client{Transport: authenticated}, base, nil
}

var newDestinationAdmin = func(
	ctx context.Context, project, dataset string, opts ...option.ClientOption,
) (destinationAdmin, error) {
	httpClient, httpBase, err := newBigQueryHTTPClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client, err := bq.NewClient(ctx, project, option.WithHTTPClient(httpClient))
	if err != nil {
		httpBase.CloseIdleConnections()
		return nil, err
	}
	return &bigQueryDestinationAdmin{
		client: client, httpBase: httpBase, dataset: client.Dataset(dataset),
	}, nil
}

func (e *signalExporter) prepareDestination(ctx context.Context, admin destinationAdmin) error {
	if err := e.ensureDataset(ctx, admin); err != nil {
		return err
	}

	tableMD, tableErr := admin.TableMetadata(ctx, e.table)
	if tableErr != nil && isContextError(tableErr) {
		return fmt.Errorf("reading destination table metadata for %s: %w", e.tableID(), tableErr)
	}
	createTable := false
	switch {
	case tableErr == nil:
		if err := e.validateTableMetadata(tableMD); err != nil {
			return err
		}
	case isNotFound(tableErr):
		if !e.cfg.AutoCreate.Tables {
			return fmt.Errorf(
				"destination table %s does not exist; create it or enable auto_create.tables",
				e.tableID())
		}
		createTable = true
	default:
		if !e.cfg.AutoCreate.Tables {
			e.logger.Warn("table metadata is unreadable; deferring authorization to the real append",
				zap.String("destination", e.tableID()), zap.Error(tableErr))
			return nil
		}
		e.logger.Warn("table metadata is unreadable; attempting idempotent creation because auto_create.tables is enabled",
			zap.String("destination", e.tableID()), zap.Error(tableErr))
		createTable = true
	}

	if !e.cfg.AutoCreate.Tables {
		return nil
	}

	createMD, viewQuery, err := schema.DestinationMetadata(
		e.schema, e.cfg.Project, e.cfg.Dataset, e.table)
	if err != nil {
		return fmt.Errorf("deriving destination metadata: %w", err)
	}
	if createTable {
		if err := e.createDestinationTable(ctx, admin, createMD); err != nil {
			return err
		}
	}
	return e.createDedupView(ctx, admin, viewQuery)
}

func (e *signalExporter) ensureDataset(ctx context.Context, admin destinationAdmin) error {
	_, datasetErr := admin.DatasetMetadata(ctx)
	if datasetErr != nil && isContextError(datasetErr) {
		return fmt.Errorf("reading destination dataset metadata for %s.%s: %w",
			e.cfg.Project, e.cfg.Dataset, datasetErr)
	}
	switch {
	case datasetErr == nil:
		return nil
	case isNotFound(datasetErr):
		if !e.cfg.AutoCreate.Dataset {
			return fmt.Errorf(
				"destination dataset %s.%s does not exist; create it or enable auto_create.dataset",
				e.cfg.Project, e.cfg.Dataset)
		}
		if err := admin.CreateDataset(ctx, &bq.DatasetMetadata{Location: e.cfg.Location}); err != nil && !isAlreadyExists(err) {
			return fmt.Errorf("creating destination dataset %s.%s: %w", e.cfg.Project, e.cfg.Dataset, err)
		}
		return nil
	default:
		e.logger.Warn("dataset metadata is unreadable; deferring authorization to the real append",
			zap.String("dataset", e.cfg.Project+"."+e.cfg.Dataset), zap.Error(datasetErr))
		return nil
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (e *signalExporter) createDestinationTable(
	ctx context.Context, admin destinationAdmin, md *bq.TableMetadata,
) error {
	if err := admin.CreateTable(ctx, e.table, md); err != nil {
		if !isAlreadyExists(err) {
			return fmt.Errorf("creating destination table %s: %w", e.tableID(), err)
		}
		winner, metadataErr := admin.TableMetadata(ctx, e.table)
		if metadataErr != nil {
			return fmt.Errorf(
				"validating concurrently created destination table %s: %w", e.tableID(), metadataErr)
		}
		return e.validateTableMetadata(winner)
	}
	return nil
}

func (e *signalExporter) createDedupView(
	ctx context.Context, admin destinationAdmin, query string,
) error {
	viewID := e.table + "_dedup"
	md := &bq.TableMetadata{ViewQuery: query, UseLegacySQL: false}
	// Table.Create is intentionally create-only. An existing view produces
	// AlreadyExists and is never replaced or mutated by exporter startup.
	if err := admin.CreateTable(ctx, viewID, md); err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("creating deduplication view %s.%s.%s: %w",
			e.cfg.Project, e.cfg.Dataset, viewID, err)
	}
	return nil
}

func (e *signalExporter) validateTableMetadata(md *bq.TableMetadata) error {
	if md == nil {
		return errors.New("destination table returned nil metadata")
	}
	have, err := schema.FromBigQuery(md.Schema)
	if err != nil {
		return fmt.Errorf("destination table %s has an unreadable schema: %w", e.tableID(), err)
	}
	if differences := schema.CompatibilityDifferences(e.schema, have); len(differences) > 0 {
		return fmt.Errorf(
			"destination table %s does not match schema %s: %v — create or migrate the table before starting",
			e.tableID(), schemaVersion, differences)
	}
	return nil
}

func isNotFound(err error) bool {
	return errorCodeIs(err, stdhttp.StatusNotFound, codes.NotFound)
}

func isAlreadyExists(err error) bool {
	return errorCodeIs(err, stdhttp.StatusConflict, codes.AlreadyExists)
}

func errorCodeIs(err error, httpCode int, grpcCode codes.Code) bool {
	var googleErr *googleapi.Error
	if errors.As(err, &googleErr) && googleErr.Code == httpCode {
		return true
	}
	if status.Code(err) == grpcCode {
		return true
	}
	apiErr, ok := apierror.FromError(err)
	return ok && (apiErr.HTTPCode() == httpCode || apiErr.GRPCStatus().Code() == grpcCode)
}
