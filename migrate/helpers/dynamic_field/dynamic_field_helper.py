"""Dynamic field mapping helper for Solr to OpenSearch migration."""
import os

from migrate.config import get_custom_logger
from migrate.exceptions import DynamicFieldMappingException, MAPPING_NOT_FOUND
from migrate.utils import read_json_file_data

logger = get_custom_logger("migrate.dynamic_field")


class DynamicFieldHelper:
    """Helper class for mapping Solr dynamic fields to OpenSearch dynamic templates."""
    def __init__(self, solrclient, opensearchclient, field_type_helper):
        self._solrclient = solrclient
        self._opensearch_client = opensearchclient
        self._field_type_helper = field_type_helper
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self._attributes_mapping = read_json_file_data(os.path.join(current_dir, "../fields/attributes.json"))
        self._field_types_map = {}

    def map_dynamic_field(self, solr_dynamic_field):
        """Map Solr dynamic field configuration to OpenSearch dynamic template."""
        pattern = solr_dynamic_field["name"]
        field_type = solr_dynamic_field["type"]
        dynamic_field_type = self._field_type_helper.get_field_type(field_type)
        if dynamic_field_type is None:
            raise DynamicFieldMappingException(pattern, MAPPING_NOT_FOUND, field_type=field_type)
        try:
            known_attrs = set(self._attributes_mapping.keys())
            extra_attrs = set(solr_dynamic_field.keys()).difference(known_attrs)
            if extra_attrs:
                logger.info("Unknown attrs: %s, %s", pattern, extra_attrs)

            attrs = {}
            for k, v in solr_dynamic_field.items():
                if k in self._attributes_mapping:
                    attrs[self._attributes_mapping[k]] = v

            index_analyzer = (
                f"{field_type}" if (
                    f"{field_type}_index" in self._opensearch_client.get_all_analyzers() or
                    field_type in self._opensearch_client.get_all_analyzers()
                ) else None
            )
            query_analyzer = (
                f"{field_type}s_query" if
                f"{field_type}s_query" in self._opensearch_client.get_all_analyzers()
                else None
            )

            mapping = {
                "type": dynamic_field_type,
                **attrs,
            }

            if index_analyzer:
                mapping["analyzer"] = index_analyzer
            if query_analyzer:
                mapping["search_analyzer"] = query_analyzer

            return {pattern: {"match": pattern, "mapping": mapping}}
        except DynamicFieldMappingException:
            raise
        except Exception as e:
            raise DynamicFieldMappingException(pattern, str(e), field_type=field_type) from e
