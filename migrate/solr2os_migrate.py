"""Solr to OpenSearch migration module."""
import json
import os
import re

import boto3
import requests

from migrate.reports.report import Report
from migrate.helpers.copy_field.copy_field_helper import CopyFieldHelper
from migrate.helpers.dynamic_field.dynamic_field_helper import DynamicFieldHelper
from migrate.helpers.fields.field_helper import FieldHelper, FieldMappingException
from migrate.helpers.fieldtype.field_type_helper import FieldTypeHelper, FieldTypeMappingException
from migrate.utils import write_json_file_data
from migrate.config import get_custom_logger

logger = get_custom_logger("migrate.solr2os_migrate")


class Solr2OSMigrate:
    """Main class for migrating Solr schema and data to OpenSearch."""

    def __init__(
            self,
            solrclient,
            opensearch_client,
            schema_config,
            data_config):
        self._schema_config = schema_config
        self._data_config = data_config
        self._solr_client = solrclient
        self._opensearch_client = opensearch_client
        self._field_type_service = FieldTypeHelper(
            self._solr_client, self._opensearch_client, self._schema_config
        )
        self._field_service = FieldHelper(
            self._solr_client,
            self._opensearch_client,
            self._field_type_service)
        self._dynamic_field_service = DynamicFieldHelper(
            self._solr_client, self._opensearch_client, self._field_type_service)
        self._report = Report()
        self._s3_client = None
        if self._data_config.get('migrate_data', False):
            region = self._data_config['region']
            # Explicitly set the region for the S3 client
            session = boto3.session.Session(region_name=region)
            self._s3_client = session.client('s3')

    def _migrate_field_types(self):
        """
        Migrate field types
        """
        for solr_field_type in self._solr_client.read_schema()["fieldTypes"]:
            self._report.field_types_solr = self._report.field_types_solr + 1
            field_type_name = solr_field_type.get('name', 'unknown')

            try:
                analyzers = self._field_type_service.map_field_type_analyzer(
                    solr_field_type)
                for a in analyzers:
                    self._opensearch_client.add_analyzer(a)
                self._report.field_types_os = self._report.field_types_os + 1

                # Track successful text analysis components
                for analyzer_type in [
                    'analyzer',
                    'indexAnalyzer',
                        'queryAnalyzer']:
                    if solr_field_type.get(analyzer_type):
                        analyzer_config = solr_field_type[analyzer_type]

                        # Track successful tokenizer
                        if analyzer_config.get('tokenizer'):
                            tokenizer_config = analyzer_config['tokenizer']
                            tokenizer_name = tokenizer_config.get('name') or tokenizer_config.get(
                                'class', '').split('.')[-1].replace('TokenizerFactory', '')
                            self._report.add_tokenizer_detail(
                                tokenizer_name, tokenizer_name, 'mapped', 'success')

                        # Track successful filters
                        if analyzer_config.get('filters'):
                            for filter_config in analyzer_config['filters']:
                                filter_name = filter_config.get('name') or filter_config.get(
                                    'class', '').split('.')[-1].replace('TokenFilterFactory', '')
                                self._report.add_filter_detail(
                                    filter_name, filter_name, 'mapped', 'success')

                        # Track successful char filters
                        if analyzer_config.get('charFilters'):
                            for char_filter_config in analyzer_config['charFilters']:
                                char_filter_name = char_filter_config.get('name') or char_filter_config.get(
                                    'class', '').split('.')[-1].replace('CharFilterFactory', '')
                                self._report.add_char_filter_detail(
                                    char_filter_name, char_filter_name, 'mapped', 'success')
            except FieldTypeMappingException as e:
                # Check if we should continue mapping fields despite analyzer
                # failure
                if self._schema_config.get(
                        'map_fields_on_analyzer_failure', False):
                    # Field type mapped without analyzer, still count as
                    # success
                    self._report.field_types_os = self._report.field_types_os + 1
                else:
                    self._report.field_types_error = self._report.field_types_error + 1

                self._report.field_type_exception_list.append(e)

                # Extract text analysis errors from FieldTypeMappingException
                if hasattr(e, 'analyzer_exception') and e.analyzer_exception:
                    analyzer_ex = e.analyzer_exception

                    # Track tokenizer errors
                    if hasattr(
                            analyzer_ex,
                            'tokenizer_exception') and analyzer_ex.tokenizer_exception:
                        tok_ex = analyzer_ex.tokenizer_exception
                        self._report.add_tokenizer_detail(
                            getattr(tok_ex, 'name', 'unknown'),
                            getattr(tok_ex, 'name', 'unknown'),
                            'N/A', 'error', str(tok_ex)
                        )

                    # Track filter errors
                    if hasattr(
                            analyzer_ex,
                            'filter_exception') and analyzer_ex.filter_exception:
                        filter_ex = analyzer_ex.filter_exception
                        self._report.add_filter_detail(
                            getattr(filter_ex, 'name', 'unknown'),
                            getattr(filter_ex, 'name', 'unknown'),
                            'N/A', 'error', str(filter_ex)
                        )

                    # Track char filter errors
                    if hasattr(
                            analyzer_ex,
                            'char_filter_exception') and analyzer_ex.char_filter_exception:
                        char_filter_ex = analyzer_ex.char_filter_exception
                        self._report.add_char_filter_detail(
                            getattr(char_filter_ex, 'name', 'unknown'),
                            getattr(char_filter_ex, 'name', 'unknown'),
                            'N/A', 'error', str(char_filter_ex)
                        )

    def _migrate_fields(self):
        """
        Migrate fields
        """
        for solr_field in self._solr_client.read_schema()["fields"]:
            self._report.field_solr = self._report.field_solr + 1
            try:
                name, opensearch_field = self._field_service.map_field(
                    solr_field)
                if opensearch_field is None:
                    continue
                self._opensearch_client.add_field(name, opensearch_field)
                self._report.field_os = self._report.field_os + 1
            except FieldMappingException as e:
                self._report.field_exception_list.append(e)
                self._report.field_error = self._report.field_error + 1

    def _migrate_dynamic_fields(self):
        """
        Migrate dynamic fields
        """
        dynamic_field_service = self._dynamic_field_service

        for solr_dynamic_field in self._solr_client.read_schema()[
                "dynamicFields"]:
            self._report.dynamic_field_solr = self._report.dynamic_field_solr + 1
            try:
                opensearch_dynamic_field = dynamic_field_service.map_dynamic_field(
                    solr_dynamic_field)
                self._opensearch_client.add_dynamic_field(
                    opensearch_dynamic_field)
                self._report.dynamic_field_os = self._report.dynamic_field_os + 1
            except Exception as e:
                self._report.dynamic_field_exception_list.append(e)
                self._report.dynamic_field_error = self._report.dynamic_field_error + 1

    def _migrate_copy_fields(self, all_fields):
        """
        Migrate copy fields
        :param all_fields:
        """
        copy_field_service = CopyFieldHelper(all_fields)

        for solr_copy_field in self._solr_client.read_schema()["copyFields"]:
            self._report.copy_field_solr = self._report.copy_field_solr + 1
            try:
                src, src_def, dst, dst_def = copy_field_service.map_copy_field(
                    solr_copy_field
                )
                self._opensearch_client.add_field(src, src_def)
                self._opensearch_client.add_field(dst, dst_def)
                self._report.copy_field_os = self._report.copy_field_os + 1
            except Exception as e:
                self._report.copy_field_exception_list.append(e)
                self._report.copy_field_error = self._report.copy_field_error + 1

    def _get_binary_fields(self):
        """Get list of binary field names from schema"""
        binary_fields = []
        try:
            schema = self._solr_client.read_schema()
            binary_field_types = set()
            for field_type in schema.get('fieldTypes', []):
                if field_type.get('class', '').endswith('BinaryField'):
                    name = field_type.get('name') or field_type.get('class')
                    if name:
                        binary_field_types.add(name)

            for field in schema.get('fields', []):
                field_type = field.get('type')
                field_name = field.get('name')
                if field_type in binary_field_types and field_name:
                    binary_fields.append(field_name)
        except Exception as e:
            error_msg = "Error identifying binary fields: %s"
            logger.error(error_msg, str(e))
            self._report.add_data_migration_error(error_msg % str(e))
        return binary_fields

    def _fix_binary_fields_in_json(self, response_text, binary_fields):
        """Fix unquoted binary field values in JSON response"""
        if binary_fields:
            for field in binary_fields:
                pattern = rf'"{field}":([^",:}}\s]+)'
                replacement = rf'"{field}":"\1"'
                response_text = re.sub(pattern, replacement, response_text)
        return response_text

    def _export_data_to_s3(self):
        """
        Export Solr data to S3
        """
        if not self._data_config.get('migrate_data', False):
            logger.info("Skipping data export as migrate_data is set to false")
            return

        logger.info("Starting Solr data export to S3 using two-query approach")
        self._export_regular_data()

    def _export_regular_data(self):
        """
        Regular export for non-nested documents with binary field support
        """
        solr_config = self._solr_client.get_config()
        rows_per_page = self._data_config.get('rows_per_page', 500)
        max_rows = self._data_config.get('max_rows', 100000)
        s3_bucket = self._data_config.get('s3_export_bucket')
        s3_prefix = self._data_config.get('s3_export_prefix', 'solr-data/')

        query_url = (
            f"{solr_config['host']}:{solr_config['port']}/solr/"
            f"{solr_config['collection']}/select"
        )
        auth = None
        if solr_config.get('username') and solr_config.get('password'):
            auth = (solr_config['username'], solr_config['password'])

        binary_fields = self._get_binary_fields()
        logger.info("Identified binary fields: %s", binary_fields)

        # Get total document count
        params = {'q': '*:*', 'rows': 0, 'wt': 'json'}
        response = requests.get(
            query_url,
            params=params,
            auth=auth,
            timeout=30)
        response.raise_for_status()
        total_docs = response.json()['response']['numFound']

        logger.info("Found %s documents", total_docs)

        exported_docs = 0
        batch_count = 0
        cursor_mark = "*"

        while exported_docs < min(total_docs, max_rows):
            batch_count += 1
            logger.info(
                "Processing batch %s with cursor %s",
                batch_count,
                cursor_mark)

            try:
                params = {
                    'q': '{!parent which="*:* -_nest_path_:*"}',
                    'fl': '*,[child]',
                    'sort': 'id asc',
                    'cursorMark': cursor_mark,
                    'rows': rows_per_page,
                    'wt': 'json'
                }

                response = requests.get(
                    query_url, params=params, auth=auth, timeout=300)
                response.raise_for_status()

                # Handle JSON parsing with binary field support
                try:
                    response_text = self._fix_binary_fields_in_json(
                        response.text, binary_fields)
                    batch_data = json.loads(response_text)

                except json.JSONDecodeError as e:
                    error_msg = "JSON parsing error in batch %s: %s"
                    logger.error(error_msg, batch_count, str(e))
                    self._report.add_data_migration_error(
                        error_msg % (batch_count, str(e)))
                    continue

                docs = batch_data['response']['docs']

                if not docs:
                    break

                # Export batch to S3
                s3_key = f"{s3_prefix}{solr_config['collection']}_batch_{batch_count}.json"
                self._s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=json.dumps(docs),
                    ContentType='application/json'
                )

                exported_docs += len(docs)
                logger.info(
                    "Exported %s documents in batch %s",
                    len(docs),
                    batch_count)

                next_cursor_mark = batch_data.get('nextCursorMark')
                if next_cursor_mark == cursor_mark:
                    break
                cursor_mark = next_cursor_mark

            except Exception as e:
                error_msg = "Error processing batch %s: %s"
                logger.error(error_msg, batch_count, str(e))
                self._report.add_data_migration_error(
                    error_msg % (batch_count, str(e)))
                break

        # Update final report
        self._report.update_data_migration_stats(
            enabled=True,
            total=total_docs,
            exported=exported_docs,
            batches=batch_count
        )

        logger.info(
            "Completed regular data export: %s documents",
            exported_docs)
        print("\n=== DATA MIGRATION COMPLETE ===")
        print(f"Total documents exported: {exported_docs}")
        print("================================\n")

    def migrate_schema(self, file_path_prefix="migration_schema"):
        """
        Method to migrate schema: field_types, fields, dynamic fields, copy fields
        """
        self._migrate_field_types()
        self._migrate_fields()
        self._migrate_dynamic_fields()
        self._migrate_copy_fields(self._opensearch_client.get_all_fields())

        index_path = os.path.join(file_path_prefix, "index.json")
        report_path = os.path.join(file_path_prefix, "schema_migration_report.html")
        write_json_file_data(
            self._opensearch_client.get_index_json(),
            index_path)
        self._report.report(report_path)

        if self._schema_config['create_index']:
            self._opensearch_client.create_index()

        return self._opensearch_client.get_index_json()

    def export_data(self, file_path_prefix="migration_schema"):
        """
        Method to export data to S3
        """
        if not self._data_config.get('migrate_data', False):
            logger.info("Skipping data export as migrate_data is set to false")
            return False

        try:
            self._export_data_to_s3()

            # Generate separate data migration report
            data_report_path = os.path.join(file_path_prefix, "data_migration_report.html")
            self._report.data_migration_report(data_report_path)
            logger.info(
                "Data migration report generated at: %s",
                data_report_path)

            return True
        except Exception as e:
            logger.error("Error exporting data to S3: %s", str(e))
            self._report.add_data_migration_error(str(e))

            # Generate report even if there was an error
            data_report_path = os.path.join(file_path_prefix, "data_migration_report.html")
            self._report.data_migration_report(data_report_path)
            logger.info(
                "Data migration report with errors generated at: %s",
                data_report_path)

            return False

    def migrate(self, file_path_prefix="migration_schema"):
        """Execute complete migration process including schema and data."""
        report_path = os.path.join(file_path_prefix, "schema_migration_report.html")

        # Perform schema migration if enabled
        if self._schema_config.get('migrate_schema', True):
            self.migrate_schema(file_path_prefix)
            logger.info(
                "Schema migration report generated at: %s",
                report_path)
        else:
            logger.info("Schema migration is disabled")

        if self._data_config and self._data_config.get('migrate_data', False):
            data_migrated = self.export_data(file_path_prefix)
            data_report_path = os.path.join(file_path_prefix, "data_migration_report.html")
            logger.info(
                "Data migration report available at: %s",
                data_report_path)
            print(f"Data migration report available at: {data_report_path}")

            # Print summary statistics
            print("\nData Migration Summary:")
            print(
                f"  - Total documents in Solr: {self._report.data_migration_docs_total}")
            print(
                f"  - Documents successfully exported: {self._report.data_migration_docs_exported}")
            if self._report.data_migration_docs_total > 0:
                if self._report.data_migration_errors == 0:
                    print("  - Success rate: 100.0%")
                else:
                    total = self._report.data_migration_docs_total
                    errors = self._report.data_migration_errors
                    success_rate = round((total - errors) / total * 100, 2)
                    print(f"  - Success rate: {success_rate}%")
            print(
                f"  - Errors encountered: {self._report.data_migration_errors}")

        return self._opensearch_client.get_index_json()
