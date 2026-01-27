"""Custom exceptions for Solr to OpenSearch migration."""

# Error messages
MAPPING_NOT_FOUND = "Implementation not available."
OPENSEARCH_CONNECTION_FAILED = "OpenSearchConnection failed"
OPENSEARCH_INDEX_CREATION_FAILED = "OpenSearchIndex creation failed"
OPENSEARCH_PACKAGE_CREATION_FAILED = "OpenSearch package creation failed"
OPENSEARCH_PACKAGE_UPDATE_FAILED = "OpenSearch package update failed"
OPENSEARCH_PACKAGE_ASSOCIATION_FAILED = "OpenSearch package association failed"
OPENSEARCH_PACKAGE_DISSOCIATION_FAILED = "OpenSearch package dissociation failed"
OPENSEARCH_PACKAGE_LIST_FAILED = "OpenSearch package list failed"
OPENSEARCH_PACKAGE_DESCRIBE_FAILED = "OpenSearch package describe failed"
S3_BUCKET_ACCESS_DENIED = "S3Bucket access denied"
S3_BUCKET_NOT_FOUND = "S3Bucket not found"
S3_UPLOAD_FAILED = "S3 upload failed"


class MigrationException(Exception):
    """Base exception for migration-related errors."""
    pass


class TokenizerMappingException(MigrationException):
    """Exception for tokenizer mapping errors."""

    def __init__(self, name, message):
        self.name = name
        super().__init__(message)


class FilterMappingException(MigrationException):
    """Exception for filter mapping errors."""

    def __init__(self, name, message):
        self.name = name
        super().__init__(message)


class CharFilterMappingException(MigrationException):
    """Exception for character filter mapping errors."""

    def __init__(self, name, message):
        self.name = name
        super().__init__(message)


class DynamicFieldMappingException(MigrationException):
    """Exception for dynamic field mapping errors."""

    def __init__(self, name, message, field_type=None):
        self.name = name
        self.field_type = field_type
        super().__init__(message)


class FieldMappingException(MigrationException):
    """Exception for field mapping errors."""

    def __init__(self, name, message, field_type=None):
        self.name = name
        self.field_type = field_type
        super().__init__(message)


class FieldTypeMappingException(MigrationException):
    """Exception for field type mapping errors."""

    def __init__(self, name, message, analyzer_exception=None):
        self.name = name
        self.analyzer_exception = analyzer_exception
        super().__init__(message)


class CopyFieldMappingException(MigrationException):
    def __init__(self, name, message, src_field=None):
        self.name = name
        self.src_field = src_field
        super().__init__(message)


class OpenSearchMappingException(MigrationException):
    """Exception for OpenSearch mapping errors."""

    def __init__(self, name, message):
        self.name = name
        super().__init__(message)


class AnalyzerMappingException(MigrationException):
    def __init__(
            self,
            name,
            filter_exception=None,
            tokenizer_exception=None,
            char_filter_exception=None):
        self.name = name
        self.filter_exception = filter_exception
        self.char_filter_exception = char_filter_exception
        self.tokenizer_exception = tokenizer_exception

        # Create meaningful error message
        message = f"Analyzer '{name}' failed"
        if tokenizer_exception:
            message += f" - Tokenizer: {tokenizer_exception}"
        if filter_exception:
            message += f" - Filter: {filter_exception}"
        if char_filter_exception:
            message += f" - CharFilter: {char_filter_exception}"

        super().__init__(message)
