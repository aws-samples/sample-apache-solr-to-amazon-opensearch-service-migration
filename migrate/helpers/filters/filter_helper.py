"""Filter mapping helper for Solr to OpenSearch migration."""
import os

from opensearchpy import token_filter
from opensearchpy import char_filter

from migrate.config import get_custom_logger
from migrate.exceptions import FilterMappingException, CharFilterMappingException, MAPPING_NOT_FOUND
from migrate.utils import read_json_file_data, get_hash
from migrate.opensearch.opensearch_client import OpenSearchMappingException

logger = get_custom_logger("migrate.filters")


class FilterHelper:
    """Helper class for mapping Solr filters to OpenSearch filters."""

    def __init__(self, solrclient, opensearch_client, migration_config):
        self._solrclient = solrclient
        self._opensearch_client = opensearch_client
        self._migration_config = migration_config
        current_dir = os.path.dirname(os.path.abspath(__file__))

        self._filter_mapping = read_json_file_data(os.path.join(current_dir, "filter_mapping.json"))

        self._filter_mapping = {k.lower(): v for k, v in self._filter_mapping.items()}

        logger.info("Loaded mapping for %s filters", len(self._filter_mapping.keys()))
        self._char_filter_mapping = read_json_file_data(os.path.join(current_dir, "char_filters_mapping.json"))
        self._char_filter_mapping = {k.lower(): v for k, v in self._char_filter_mapping.items()}
        logger.info("Loaded mapping for %s char filters", len(self._char_filter_mapping.keys()))

        self._filter_map = {}
        self._char_filter_map = {}

        self._packages_map = {}

        collection = self._solrclient.get_collection()
        self._solr_collection_dir = f"solr/{collection}"
        self._opensearch_collection_dir = f"migration_schema/{collection}"
        self._opensearch_package_dir = f"{self._opensearch_collection_dir}/packages"

        if not os.path.exists(self._opensearch_package_dir):
            os.makedirs(self._opensearch_package_dir)

        self.opensearch_packages_file = (
            f"{self._opensearch_collection_dir}/opensearch_packages.json"
        )

    def _get_filter_name(self, filter_config):
        name = filter_config.get("name")
        if name is None:
            name = filter_config.get("class")
            name = name.split(".")[1]
            if "TokenFilterFactory" in name:
                name = name.split("TokenFilterFactory")[0].lower()
            elif "CharFilterFactory" in name:
                name = name.split("CharFilterFactory")[0].lower()
            elif "FilterFactory" in name:
                name = name.split("FilterFactory")[0].lower()
        else:
            name = name.lower()

        return name

    def _process_filter_mapping_key(self, ctx):
        """Process individual filter mapping key."""
        key, filter_mapping, solr_filter, custom_filter_def, solr_filter_name = ctx
        if "valueFrom" in filter_mapping[key]:
            custom_filter_def[key] = solr_filter.get(filter_mapping[key]['valueFrom'])
            if custom_filter_def[key] is None:
                custom_filter_def[key] = filter_mapping[key]['default']
        elif "valueFromFile" in filter_mapping[key]:
            filename = solr_filter.get(filter_mapping[key]['valueFromFile'])
            if ("create_package" in filter_mapping[key] and
                    self._migration_config['create_package']):
                filter_attrib_value = self._handle_packages(
                    f"filter_{solr_filter_name}", filename
                )
                custom_filter_def[key] = filter_attrib_value
            elif self._migration_config['expand_files_array']:
                logger.info(
                    "retrieve data from file %s for filter %s", filename, solr_filter_name
                )
                custom_filter_def[key] = self._get_file_data(solr_filter_name, filename)
            else:
                custom_filter_def[key] = []
        else:
            custom_filter_def[key] = filter_mapping[key]['default']

    def _process_char_filter_mapping_key(self, ctx):
        """Process individual char filter mapping key."""
        key, char_filter_mapping, solr_char_filter, custom_filter_def, solr_filter_name = ctx
        if "valueFrom" in char_filter_mapping[key]:
            value_from = char_filter_mapping[key]['valueFrom']
            custom_filter_def[key] = solr_char_filter.get(value_from)
            if custom_filter_def[key] is None:
                custom_filter_def[key] = char_filter_mapping[key]['default']
        elif "valueFromFile" in char_filter_mapping[key]:
            filename = solr_char_filter.get(char_filter_mapping[key]['valueFromFile'])
            if ("create_package" in char_filter_mapping[key] and
                    self._migration_config['create_package']):
                filter_attrib_value = self._handle_packages(
                    f"char_filter{solr_filter_name}", filename
                )
                custom_filter_def[key] = filter_attrib_value
            elif self._migration_config['expand_files_array']:
                logger.info(
                    "retrieve data from file %s for CharFilter %s", filename, solr_filter_name
                )
                custom_filter_def[key] = self._get_file_data(solr_filter_name, filename)
            else:
                custom_filter_def[key] = []
        else:
            logger.info("Setting default value")
            custom_filter_def[key] = char_filter_mapping[key]['default']

    def _create_package_file(self, file_path_old, file_path_new, solr_file, filter_name):
        """Create package file for filter."""
        logger.info("%s,%s,%s,%s", file_path_old, file_path_new, solr_file, filter_name)
        file_data = self._get_file_data(filter_name, solr_file)

        with open(file_path_new, "w", encoding="utf-8") as f:
            f.write('\n'.join(file_data))

    def _get_file_data(self, filter_name, filename):
        data = self._solrclient.get_solr_file_data(filename)
        res = []

        for line in data.split("\n"):
            line = line.strip()

            if (not line.startswith("#") and not line.startswith("|") and
                    line.strip() and line.strip() != "|"):
                if filter_name.lower().startswith("stemmeroverride"):
                    line = line.replace('\t', ' => ')
                res.append(line)
        return res

    def _map_filter(self, solr_filter):

        solr_filter_name = self._get_filter_name(solr_filter)

        try:
            hashed_solr_filter_name = solr_filter_name + get_hash(solr_filter)

            logger.info("mapping filter with name %s", solr_filter_name)

            filter_mapping = self._filter_mapping.get(solr_filter_name)
            if filter_mapping is None:
                logger.warning("Filter mapping not found for name %s", solr_filter_name)
                raise FilterMappingException(solr_filter_name, MAPPING_NOT_FOUND)

            if self._filter_map.get(hashed_solr_filter_name) is not None:
                logger.info("returning from map")
                return self._filter_map.get(hashed_solr_filter_name)

            custom_filter_def = {}
            for key in filter_mapping.keys():

                if key == "type":
                    continue

                self._process_filter_mapping_key(
                    (key, filter_mapping, solr_filter, custom_filter_def, solr_filter_name)
                )

            tf = token_filter(
                hashed_solr_filter_name, filter_mapping['type'], **custom_filter_def
            )
            self._filter_map[hashed_solr_filter_name] = tf
            logger.info("mapping filter with name %s completed", solr_filter_name)
            return tf

        except FilterMappingException:
            logger.warning("mapping filter with name %s failed", solr_filter_name)
            raise
        except OpenSearchMappingException as e:
            logger.warning("mapping filter with name %s failed", solr_filter_name)
            raise FilterMappingException(solr_filter_name, str(e)) from e

    def _map_char_filter(self, solr_char_filter):

        solr_filter_name = self._get_filter_name(solr_char_filter)

        try:
            logger.info("mapping CharFilter with name %s", solr_filter_name)

            char_filter_mapping = self._char_filter_mapping.get(solr_filter_name)
            if char_filter_mapping is None:
                logger.warning("CharFilter mapping not found for name %s", solr_filter_name)
                raise CharFilterMappingException(solr_filter_name, MAPPING_NOT_FOUND)

            custom_filter_def = {}
            for key in char_filter_mapping.keys():
                if key == "type":
                    continue

                self._process_char_filter_mapping_key(
                    (key, char_filter_mapping, solr_char_filter, custom_filter_def, solr_filter_name)
                )

            solr_filter_name = solr_filter_name + get_hash(custom_filter_def)
            self._filter_map[solr_filter_name] = custom_filter_def

            cf = char_filter(
                solr_filter_name, char_filter_mapping['type'], **custom_filter_def
            )
            logger.info("mapping CharFilter with name %s completed", solr_filter_name)
            return cf
        except OpenSearchMappingException as e:
            logger.warning("mapping Char filter with name %s failed", solr_filter_name)
            raise CharFilterMappingException(solr_filter_name, str(e)) from e
        except CharFilterMappingException:
            logger.warning("mapping Char filter with name %s failed", solr_filter_name)
            raise

    def _handle_packages(self, solr_filter_name, filename):
        solr_file_path = f"{self._solr_collection_dir}/{filename}"
        p_name = filename.replace("/", "-").replace(".", "-").replace("_", "-")
        collection = self._solrclient.get_collection()
        package_name = f"p-{collection}-{p_name}".lower()
        package_file = f"{self._opensearch_package_dir}/{package_name}"
        package_key = solr_filter_name + "_" + package_file
        if package_key in self._packages_map:
            logger.info(
                "retrieve package details from existing map for filter %s", solr_filter_name
            )
            filter_attrib_value = self._packages_map[package_key]["filter_attrib_value"]
        else:
            logger.info("create package details for filter %s", solr_filter_name)
            self._create_package_file(solr_file_path, package_file, filename, solr_filter_name)
            package_id, _ = self._opensearch_client.create_and_associate_package(
                package_name, package_file
            )
            filter_attrib_value = f"analyzers/{package_id}"

            self._packages_map[package_key] = {
                "package_name": package_name,
                "package_attrib": solr_filter_name,
                "filter_attrib_value": filter_attrib_value,
            }
            logger.info(
                "saving package details from existing map for Char filter %s", solr_filter_name
            )

        return filter_attrib_value

    def map_filters(self, solr_filters):
        """Map list of Solr filters to OpenSearch filters."""
        my_filters = []
        try:
            # Ensure all filters have mapping defined. Else break.
            # This avoids creating unnecessary filters which have package requirements.
            for solr_filter in solr_filters:
                solr_filter_name = self._get_filter_name(solr_filter)

                filter_mapping = self._filter_mapping.get(solr_filter_name)
                if filter_mapping is None:
                    logger.warning(
                        "Pre Check Filter mapping not found for name %s", solr_filter_name
                    )
                    raise FilterMappingException(solr_filter_name, MAPPING_NOT_FOUND)

            # Start mapping filters have mapping defined. Else break.
            for solr_filter in solr_filters:
                f = self._map_filter(solr_filter)
                my_filters.append(f)

            return my_filters
        except FilterMappingException as e:
            logger.warning("An error occurred during filter mapping: %s", str(e))
            raise

    def map_char_filters(self, solr_char_filters):
        """Map list of Solr char filters to OpenSearch char filters."""
        my_char_filters = []
        try:
            for solr_char_filter in solr_char_filters:
                solr_char_filter_name = self._get_filter_name(solr_char_filter)

                filter_mapping = self._char_filter_mapping.get(solr_char_filter_name)
                if filter_mapping is None:
                    logger.warning(
                        "Pre Check CharFilter mapping not found for name %s", solr_char_filter_name
                    )
                    raise CharFilterMappingException(solr_char_filter_name, MAPPING_NOT_FOUND)

            for solr_char_filter in solr_char_filters:
                f = self._map_char_filter(solr_char_filter)
                my_char_filters.append(f)

            return my_char_filters
        except CharFilterMappingException as e:
            logger.exception("An error occurred during char filter mapping: %s", str(e))
            raise
