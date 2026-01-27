"""Field mapping helper for Solr to OpenSearch migration."""
import os

from migrate.config import get_custom_logger
from migrate.exceptions import FieldMappingException, MAPPING_NOT_FOUND
from migrate.utils import read_json_file_data

logger = get_custom_logger("migrate.field")


class FieldHelper:
    """Helper class for mapping Solr fields to OpenSearch fields."""
    def __init__(self, solrclient, opensearch_client, field_type_helper):
        self._solrclient = solrclient
        self._opensearch_client = opensearch_client
        self._field_type_helper = field_type_helper
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self._attributes_mapping = read_json_file_data(os.path.join(current_dir, "attributes.json"))

    def _setup_analyzers(self, attrs, field_field_type):
        """Setup analyzers for the field."""
        all_analyzers = self._opensearch_client.get_all_analyzers()
        analyzer = field_field_type if field_field_type in all_analyzers else None
        index_analyzer = (
            f"{field_field_type}_index" if f"{field_field_type}_index" in all_analyzers else None
        )
        query_analyzer = (
            f"{field_field_type}_query" if f"{field_field_type}_query" in all_analyzers else None
        )

        if analyzer:
            attrs["analyzer"] = analyzer
        if index_analyzer:
            attrs["analyzer"] = index_analyzer
        if query_analyzer:
            attrs["search_analyzer"] = query_analyzer

    def _cleanup_field_attrs(self, attrs, field_type):
        """Remove unsupported attributes for specific field types."""
        if field_type in ["nested", "geo_shape"]:
            attrs.pop("index", None)
            attrs.pop("store", None)
        if field_type == "geo_shape":
            attrs.pop("doc_values", None)

    def map_field(self, solr_field):
        """Map Solr field configuration to OpenSearch field mapping."""
        name = solr_field["name"]
        field_field_type = solr_field["type"]
        field_type = self._field_type_helper.get_field_type(field_field_type)

        logger.info("Starting mapping for field %s", name)

        if field_type is None:
            logger.info("Mapping for field %s not Found", name)
            raise FieldMappingException(name, MAPPING_NOT_FOUND, field_type=solr_field["type"])

        try:
            known_attrs = set(self._attributes_mapping.keys())
            extra_attrs = set(solr_field.keys()).difference(known_attrs)
            if extra_attrs:
                logger.info("Unknown attrs: %s, %s", name, extra_attrs)

            attrs = {
                self._attributes_mapping[k]: v for k, v in solr_field.items()
                if k in self._attributes_mapping
            }
            attrs["type"] = field_type

            self._setup_analyzers(attrs, field_field_type)
            self._cleanup_field_attrs(attrs, field_type)

            return name, attrs
        except FieldMappingException:
            logger.info("Mapping for field %s failed FieldException", name)
            raise
        except Exception as e:
            logger.info(e)
            raise FieldMappingException(name, str(e), field_type=field_type) from e
