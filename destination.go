// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"

	bq "cloud.google.com/go/bigquery"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
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
	CreateView(context.Context, string, string) error
	Close() error
}

type bigQueryDestinationAdmin struct {
	client  *bq.Client
	dataset *bq.Dataset
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

func (a *bigQueryDestinationAdmin) CreateView(ctx context.Context, id, query string) error {
	// Table.Create is intentionally create-only. An existing view produces
	// AlreadyExists and is never replaced or mutated by exporter startup.
	return a.dataset.Table(id).Create(ctx, &bq.TableMetadata{ViewQuery: query, UseLegacySQL: false})
}

func (a *bigQueryDestinationAdmin) Close() error { return a.client.Close() }

var newDestinationAdmin = func(
	ctx context.Context, project, dataset string, opts ...option.ClientOption,
) (destinationAdmin, error) {
	client, err := bq.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, err
	}
	return &bigQueryDestinationAdmin{client: client, dataset: client.Dataset(dataset)}, nil
}

func (e *signalExporter) prepareDestination(ctx context.Context, admin destinationAdmin) error {
	_, datasetErr := admin.DatasetMetadata(ctx)
	switch {
	case datasetErr == nil:
	case isNotFound(datasetErr):
		if !e.cfg.AutoCreate.Dataset {
			return fmt.Errorf(
				"destination dataset %s.%s does not exist; create it or enable auto_create.dataset",
				e.cfg.Project, e.cfg.Dataset)
		}
		if err := admin.CreateDataset(ctx, &bq.DatasetMetadata{Location: e.cfg.Location}); err != nil && !isAlreadyExists(err) {
			return fmt.Errorf("creating destination dataset %s.%s: %w", e.cfg.Project, e.cfg.Dataset, err)
		}
	default:
		e.logger.Warn("dataset metadata is unreadable; deferring authorization to the real append",
			zap.String("dataset", e.cfg.Project+"."+e.cfg.Dataset), zap.Error(datasetErr))
	}

	tableMD, tableErr := admin.TableMetadata(ctx, e.table)
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
	case tableErr != nil:
		e.logger.Warn("table metadata is unreadable; deferring authorization to the real append",
			zap.String("destination", e.tableID()), zap.Error(tableErr))
		return nil
	}

	if !e.cfg.AutoCreate.Tables {
		return nil
	}

	createMD, viewQuery, err := schema.DestinationMetadata(
		e.schema, e.cfg.Project, e.cfg.Dataset, e.table)
	if err != nil {
		return fmt.Errorf("deriving destination metadata: %w", err)
	}
	if isNotFound(tableErr) {
		if err := admin.CreateTable(ctx, e.table, createMD); err != nil {
			if !isAlreadyExists(err) {
				return fmt.Errorf("creating destination table %s: %w", e.tableID(), err)
			}
			winner, metadataErr := admin.TableMetadata(ctx, e.table)
			if metadataErr != nil {
				return fmt.Errorf(
					"validating concurrently created destination table %s: %w", e.tableID(), metadataErr)
			}
			if err := e.validateTableMetadata(winner); err != nil {
				return err
			}
		}
	}
	viewID := e.table + "_dedup"
	if err := admin.CreateView(ctx, viewID, viewQuery); err != nil && !isAlreadyExists(err) {
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
	return errorCodeIs(err, 404, codes.NotFound)
}

func isAlreadyExists(err error) bool {
	return errorCodeIs(err, 409, codes.AlreadyExists)
}

func errorCodeIs(err error, httpCode int, grpcCode codes.Code) bool {
	if status.Code(err) == grpcCode {
		return true
	}
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == httpCode
}
