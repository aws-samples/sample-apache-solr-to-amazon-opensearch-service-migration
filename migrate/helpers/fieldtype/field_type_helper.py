"""Field type mapping helper for Solr to OpenSearch migration."""
import os

from migrate.config import get_custom_logger
from migrate.helpers.analyzer import AnalyzerHelper, AnalyzerMappingException
from migrate.exceptions import FieldTypeMappingException
from migrate.utils import read_json_file_data

logger = get_custom_logger("migrate.field_type")


class FieldTypeHelper:
    """Helper class for mapping Solr field types to OpenSearch field types."""
    def __init__(self, solrclient, opensearch_client, migration_config):
        self._solrclient = solrclient
        self._opensearch_client = opensearch_client
        self._migration_config = migration_config
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self._field_data_types_mapping = read_json_file_data(
            os.path.join(current_dir, "field_data_types.json")
        )

        self._field_types_map = {}
        self._analyzer_helper = AnalyzerHelper(solrclient, opensearch_client, migration_config)

    def get_field_type(self, field_type):
        """Get mapped field type."""
        return self._field_types_map.get(field_type)

    def _map_field_data_type(self, solr_field_type):
        """Map Solr field type to OpenSearch data type."""
        return solr_field_type["name"], self._field_data_types_mapping.get(solr_field_type["class"])

    def map_field_type_analyzer(self, solr_field_type):
        """Map Solr field type and analyzer to OpenSearch equivalents."""
        field_type_name = solr_field_type['name']
        try:
            (field_type_name, field_type_data_type) = self._map_field_data_type(solr_field_type)
            field_type_element_analyzer = self._analyzer_helper.map_analyzer(solr_field_type)
            self._field_types_map[field_type_name] = field_type_data_type
            return field_type_element_analyzer
        except AnalyzerMappingException as e:
            if self._migration_config.get('skip_text_analysis_failure', False):
                # Map field type without analyzer when flag is enabled
                (field_type_name, field_type_data_type) = self._map_field_data_type(solr_field_type)
                self._field_types_map[field_type_name] = field_type_data_type

            raise FieldTypeMappingException(field_type_name, str(e), e) from e
