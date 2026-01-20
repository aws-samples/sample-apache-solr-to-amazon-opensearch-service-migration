"""Main entry point for Solr to OpenSearch migration."""
import os
import sys

import boto3
import opensearchpy
import pysolr
import toml

from migrate.config import get_custom_logger
from migrate.solr2os_migrate import Solr2OSMigrate
from migrate.opensearch.opensearch_client import OpenSearchClient
from migrate.solr.solr_client import SolrClient

logger = get_custom_logger("main")

if __name__ == '__main__':
    config = toml.load("migrate.toml")
    migration_config = config['migration']
    data_migration_config = config.get('data_migration', {})
    if migration_config.get('migrate_schema', False):
        create_pkg = migration_config['create_package']
        expand_files = migration_config['expand_files_array']
        if create_pkg is True and expand_files is True:
            logger.error("create_package and expand_files_array are mutually exclusive")
            sys.exit()

    # Validate data migration configuration if enabled
    if data_migration_config.get('migrate_data', False):
        if not data_migration_config.get('s3_export_bucket'):
            logger.error("s3_export_bucket must be specified when migrate_data is enabled")
            sys.exit()

        # Verify AWS credentials are available
        try:
            region = data_migration_config.get('region', 'us-east-1')
            boto3.client('sts', region_name=region).get_caller_identity()
        except Exception as e:
            logger.error("AWS credentials not properly configured: %s", str(e))
            logger.error("Please configure AWS credentials for S3 access")
            sys.exit()

    try:
        solrclient = SolrClient(config['solr'])
        opensearch_client = OpenSearchClient(config['opensearch'])
        FILE_PATH = os.path.join("migration_schema", config['solr']['collection'])

        # Initialize the migration object
        migrator = Solr2OSMigrate(
            solrclient,
            opensearch_client,
            config['migration'],
            data_migration_config
        )
        logger.info("Migration object initialized")
        # Handle schema migration if enabled
        if migration_config.get('migrate_schema', False):
            logger.info("Starting schema migration")
            migrator.migrate_schema(FILE_PATH)
            logger.info("Schema migration completed")
        else:
            logger.info("Schema migration is disabled")

        # Handle data migration if enabled
        if data_migration_config.get('migrate_data', False):
            logger.info("Starting data export")
            migrator.export_data(FILE_PATH)
            bucket = data_migration_config['s3_export_bucket']
            logger.info("Data export completed. Check S3 bucket: %s", bucket)

    except pysolr.SolrError as e:
        logger.error("Solr error: %s", str(e))
        sys.exit()
    except opensearchpy.exceptions.OpenSearchException as e:
        logger.error("OpenSearch error: %s", str(e))
        sys.exit()
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        sys.exit()
