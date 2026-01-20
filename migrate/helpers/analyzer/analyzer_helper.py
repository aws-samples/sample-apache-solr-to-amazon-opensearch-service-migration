from opensearchpy import analyzer

from migrate.exceptions import AnalyzerMappingException
from migrate.helpers.filters.filter_helper import FilterHelper, FilterMappingException, CharFilterMappingException
from migrate.helpers.tokenizer.tokenizer_helper import TokenizerHelper, TokenizerMappingException
from migrate.config import get_custom_logger

logger = get_custom_logger("migrate.analyzer")

class AnalyzerHelper(object):

    def __init__(self, solrclient, opensearch_client, migration_config):
        self._filter_helper = FilterHelper(solrclient, opensearch_client, migration_config)
        self._tokenizer_helper = TokenizerHelper(solrclient, opensearch_client)

    def _map_analyzer(self, analyzer_name, solr_analyzer):

        mapped_filters = []
        mapped_char_filters = []
        mapped_tokenizer = None
        tokenizer_exception = None
        filter_exception = None
        char_filter_exception = None
        logger.info("Starting analysis for fieldType %s", analyzer_name)
        try:
            if solr_analyzer.get("tokenizer") is not None:
                mapped_tokenizer = self._tokenizer_helper.map_tokenizer(solr_analyzer.get("tokenizer"))
        except TokenizerMappingException as e:
            tokenizer_exception = e

        try:
            if solr_analyzer.get("filters") is not None:
                mapped_filters = self._filter_helper.map_filters(solr_analyzer.get("filters"))
        except FilterMappingException as e:
            filter_exception = e

        try:
            if solr_analyzer.get("charFilters") is not None:
                mapped_char_filters = self._filter_helper.map_char_filters(solr_analyzer.get("charFilters"))
        except CharFilterMappingException as e:
            char_filter_exception = e

        logger.info("Completing analysis for fieldType %s", analyzer_name)
        if filter_exception is not None or tokenizer_exception is not None or char_filter_exception is not None:
            raise AnalyzerMappingException(name=analyzer_name, filter_exception=filter_exception,
                                           tokenizer_exception=tokenizer_exception,
                                           char_filter_exception=char_filter_exception)
        else:
            opensearch_analyzer = analyzer(analyzer_name, tokenizer=mapped_tokenizer, filter=mapped_filters,
                                           char_filter=mapped_char_filters)
            return opensearch_analyzer

    def map_analyzer(self, field_type):
        opensearch_analyzer = []
        try:
            if field_type.get("analyzer") is not None:
                solr_analyzer = field_type.get("analyzer")
                analyzer_name = field_type.get("name")
                opensearch_analyzer.append(self._map_analyzer(analyzer_name, solr_analyzer))

            if field_type.get("indexAnalyzer") is not None:
                solr_analyzer = field_type.get("indexAnalyzer")
                analyzer_name = field_type.get("name") + "_index"
                self._map_analyzer(analyzer_name, solr_analyzer)
                opensearch_analyzer.append(self._map_analyzer(analyzer_name, solr_analyzer))

            if field_type.get("queryAnalyzer") is not None:
                solr_analyzer = field_type.get("queryAnalyzer")
                analyzer_name = field_type.get("name") + "_query"
                opensearch_analyzer.append(self._map_analyzer(analyzer_name, solr_analyzer))

            return opensearch_analyzer

        except AnalyzerMappingException as e:
            raise e
