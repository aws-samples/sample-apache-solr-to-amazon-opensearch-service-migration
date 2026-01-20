"""Report generation module for Solr to OpenSearch migration."""
import os
from datetime import datetime

import jinja2
from jinja2 import FileSystemLoader

from migrate.config import get_custom_logger

logger = get_custom_logger("reports.report")


class Report:
    """Report class for tracking migration statistics and generating HTML reports."""

    def __init__(self):
        self.field_types_solr = 0
        self.field_types_os = 0
        self.field_types_error = 0
        self.field_solr = 0
        self.field_os = 0
        self.field_error = 0
        self.dynamic_field_solr = 0
        self.dynamic_field_os = 0
        self.dynamic_field_error = 0
        self.copy_field_solr = 0
        self.copy_field_os = 0
        self.copy_field_error = 0
        self.data_migration_enabled = False
        self.data_migration_docs_total = 0
        self.data_migration_docs_exported = 0
        self.data_migration_batches = 0
        self.data_migration_errors = 0
        self.field_type_exception_list = []
        self.field_exception_list = []
        self.dynamic_field_exception_list = []
        self.copy_field_exception_list = []
        self.data_migration_error_list = []
        self.tokenizer_details = []
        self.filter_details = []
        self.char_filter_details = []

    def add_data_migration_error(self, error_msg):
        """Add data migration error to the report"""
        self.data_migration_errors += 1
        self.data_migration_error_list.append(str(error_msg))

    def update_data_migration_stats(
            self,
            enabled=False,
            total=0,
            exported=0,
            batches=0):
        """Update data migration statistics"""
        self.data_migration_enabled = enabled
        self.data_migration_docs_total = total
        self.data_migration_docs_exported = exported
        self.data_migration_batches = batches

    def add_tokenizer_detail(
            self,
            name,
            solr_type,
            opensearch_type,
            status,
            error=None):
        """Add tokenizer processing details"""
        self.tokenizer_details.append({
            'name': name,
            'solr_type': solr_type,
            'opensearch_type': opensearch_type,
            'status': status,
            'error': error
        })

    def add_filter_detail(
            self,
            name,
            solr_type,
            opensearch_type,
            status,
            error=None):
        """Add filter processing details"""
        self.filter_details.append({
            'name': name,
            'solr_type': solr_type,
            'opensearch_type': opensearch_type,
            'status': status,
            'error': error
        })

    def add_char_filter_detail(
            self,
            name,
            solr_type,
            opensearch_type,
            status,
            error=None):
        """Add char filter processing details"""
        self.char_filter_details.append({
            'name': name,
            'solr_type': solr_type,
            'opensearch_type': opensearch_type,
            'status': status,
            'error': error
        })

    def report(self, file):
        """Generate HTML migration report."""
        environment = jinja2.Environment(loader=FileSystemLoader(
            "migrate/reports/templates/"), autoescape=True)
        template = environment.get_template("schema_migration_report.html")

        total_components = (self.field_types_solr + self.field_solr +
                            self.dynamic_field_solr + self.copy_field_solr)
        total_mapped = (self.field_types_os + self.field_os +
                        self.dynamic_field_os + self.copy_field_os)
        total_errors = (self.field_types_error + self.field_error +
                        self.dynamic_field_error + self.copy_field_error)

        summary = {
            'field_types': {
                "total": self.field_types_solr,
                "mapped": self.field_types_os,
                "error": self.field_types_error,
                "exception_list": self.field_type_exception_list
            },
            'fields': {
                "total": self.field_solr,
                "mapped": self.field_os,
                "error": self.field_error,
                "exception_list": self.field_exception_list
            },
            'dynamic_fields': {
                "total": self.dynamic_field_solr,
                "mapped": self.dynamic_field_os,
                "error": self.dynamic_field_error,
                "exception_list": self.dynamic_field_exception_list
            },
            'copy_fields': {
                "total": self.copy_field_solr,
                "mapped": self.copy_field_os,
                "error": self.copy_field_error,
                "exception_list": self.copy_field_exception_list
            },
            'overall': {
                "total": total_components,
                "mapped": total_mapped,
                "error": total_errors
            }
        }

        text_analysis = {
            'tokenizers': {
                'total': len(self.tokenizer_details),
                'successful': len([t for t in self.tokenizer_details if t['status'] == 'success']),
                'failed': len([t for t in self.tokenizer_details if t['status'] == 'error']),
                'details': self.tokenizer_details
            },
            'filters': {
                'total': len(self.filter_details),
                'successful': len([f for f in self.filter_details if f['status'] == 'success']),
                'failed': len([f for f in self.filter_details if f['status'] == 'error']),
                'details': self.filter_details
            },
            'char_filters': {
                'total': len(self.char_filter_details),
                'successful': len([c for c in self.char_filter_details if c['status'] == 'success']),
                'failed': len([c for c in self.char_filter_details if c['status'] == 'error']),
                'details': self.char_filter_details
            }
        }

        context = {
            "summary": summary,
            "text_analysis": text_analysis,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        content = template.render(context)

        os.makedirs(os.path.dirname(file) if os.path.dirname(
            file) else '.', exist_ok=True)

        with open(file, mode="w", encoding="utf-8") as message:
            message.write(content)

    def data_migration_report(self, file):
        """Generate a separate report for data migration"""
        environment = jinja2.Environment(loader=FileSystemLoader(
            "migrate/reports/templates/"), autoescape=True)
        template = environment.get_template("data_migration_report.html")

        data_migration = {
            "enabled": self.data_migration_enabled,
            "total": self.data_migration_docs_total,
            "exported": self.data_migration_docs_exported,
            "batches": self.data_migration_batches,
            "errors": self.data_migration_errors,
            "error_list": self.data_migration_error_list
        }

        context = {
            "data_migration": data_migration,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        content = template.render(context)

        os.makedirs(os.path.dirname(file) if os.path.dirname(
            file) else '.', exist_ok=True)

        with open(file, mode="w", encoding="utf-8") as message:
            message.write(content)
