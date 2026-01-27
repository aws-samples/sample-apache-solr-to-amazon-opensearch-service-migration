"""Solr client for connecting to and interacting with Apache Solr."""
from typing import Dict, Any

import pysolr
from migrate.config import get_custom_logger

logger = get_custom_logger("solr.solr_client")


class SolrClient:
    """Client for interacting with Apache Solr instance."""
    def __init__(self, solr_config: Dict[str, Any]):
        # Store the config for later use
        self._config = solr_config

        # Keep original URL construction with explicit str() conversion
        url = solr_config['host'] + ":" + str(solr_config['port']) + "/solr/"
        self._client = pysolr.Solr(url=url)
        self._collection = solr_config['collection']
        self._schema_url = url + self._collection + "/schema?wt=json"
        self._file_endpoint = url + f"{self._collection}/admin/file"

        self._auth = None
        try:
            self._auth = (solr_config['username'], solr_config['password'])
        except KeyError:
            logger.error("Skipping auth as no username password found in config")

        logger.info("Initializing solr client with url %s", self._schema_url)

        response = self._client.get_session().get(
            url=self._schema_url, auth=self._auth
        )
        if response.status_code == 200:
            logger.info(
                "Successfully initialized solr client with url %s", self._schema_url
            )
        else:
            logger.warning(
                "Initialized failed for solr client with url %s due to status code: %s",
                self._schema_url, response.status_code
            )
            raise pysolr.SolrError(
                "Could not initialize the solr client due to error code: ", 
                response.status_code
            )

    def get_collection(self) -> str:
        """Get the Solr collection name."""
        return self._collection

    def read_schema(self) -> Dict[str, Any]:
        """
        It reads the schema from the solr instance and returns the Json Dictionary
        :rtype: object
        """
        response = self._client.get_session().get(url=self._schema_url, auth=self._auth)
        if response.status_code == 200:
            schema = response.json()["schema"]
            logger.debug(schema)
            return schema
        raise pysolr.SolrError(
            "Could not download the Schema due error code: ", response.status_code
        )

    def get_solr_file_data(self, file: str) -> str:
        """
        It reads the file passed as an input and returns the text results
        :rtype: object
        """
        file_url = self._file_endpoint + "?file=" + file
        try:
            response = self._client.get_session().get(file_url, auth=self._auth)
            logger.info("Downloaded the file %s", file)
        except pysolr.SolrError as e:
            logger.warning("could not get the file data")
            raise e
        return response.text

    def get_config(self) -> Dict[str, Any]:
        """
        Returns the Solr configuration that was used to initialize this client
        :return: Dictionary containing Solr configuration
        """
        return self._config
