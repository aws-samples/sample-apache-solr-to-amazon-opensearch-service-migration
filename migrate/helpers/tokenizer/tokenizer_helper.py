"""Tokenizer mapping helper for Solr to OpenSearch migration."""
import os
from opensearchpy import tokenizer
from migrate.utils import read_json_file_data, get_hash
from migrate.config import get_custom_logger
from migrate.exceptions import TokenizerMappingException, MAPPING_NOT_FOUND

logger = get_custom_logger("migrate.tokenizer")


class TokenizerHelper:
    """Helper class for mapping Solr tokenizers to OpenSearch tokenizers."""

    def __init__(self, solrclient, opensearch_client):
        self._solrclient = solrclient
        self._opensearch_client = opensearch_client

        # Get the correct path to the mapping file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(current_dir, "tokenizer_mapping.json")

        logger.info("Current directory: %s", current_dir)
        logger.info("Loading mapping file from: %s", mapping_file)

        self._tokenizer_mapping = read_json_file_data(mapping_file)
        logger.debug("Raw mapping content: %s", self._tokenizer_mapping)

        self._tokenizer_mapping = {k.lower(): v for k, v in self._tokenizer_mapping.items()}
        logger.debug("Processed mappings: %s", self._tokenizer_mapping)
        logger.debug("Available tokenizer types: %s", list(self._tokenizer_mapping.keys()))
        self._tokenizer_map = {}

    def _get_tokenizer_name(self, solr_tokenizer_config):
        """Extract tokenizer name from configuration."""
        name = solr_tokenizer_config.get("name")
        if name is None:
            name = solr_tokenizer_config.get("class").split(".")[1]
            if "TokenizerFactory" in name:
                name = name.split("TokenizerFactory")[0].lower()
        else:
            name = name.lower()
        return name

    def map_tokenizer(self, solr_tokenizer):
        """Map Solr tokenizer configuration to OpenSearch tokenizer."""

        tokenizer_name = self._get_tokenizer_name(solr_tokenizer)

        try:
            logger.info("mapping tokenizer with name %s", tokenizer_name)
            tokenizer_mapping = self._tokenizer_mapping.get(tokenizer_name)
            if tokenizer_mapping is None:
                logger.error("Tokenizer mapping not found for name %s", tokenizer_name)
                raise TokenizerMappingException(
                    tokenizer_name, MAPPING_NOT_FOUND)
            tokenizer_def = {}
            for key in tokenizer_mapping.keys():
                if key == "type":
                    continue
                if "valueFrom" in tokenizer_mapping[key].keys():
                    tokenizer_def[key] = solr_tokenizer.get(tokenizer_mapping[key]['valueFrom'])
                    if tokenizer_def[key] is None:
                        tokenizer_def[key] = tokenizer_mapping[key]['default']
                else:
                    tokenizer_def[key] = tokenizer_mapping[key]['default']

            tokenizer_name = tokenizer_name + get_hash(tokenizer_def)
            self._tokenizer_map[tokenizer_name] = tokenizer_name

            opensearch_tokenizer = tokenizer(
                tokenizer_name, tokenizer_mapping['type'], **tokenizer_def
            )
            logger.info("mapping Tokenizer with name %s completed", tokenizer_name)
            return opensearch_tokenizer
        except TokenizerMappingException:
            raise
        except Exception as e:
            logger.info("Mapping Tokenizer with name %s failed", tokenizer_name)
            raise TokenizerMappingException(
                tokenizer_name, f"failed: {e}") from e
