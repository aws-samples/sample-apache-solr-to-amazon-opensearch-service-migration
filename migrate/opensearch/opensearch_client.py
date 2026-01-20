"""OpenSearch client for Solr to OpenSearch migration."""
import time
from typing import Tuple
import hashlib

import boto3
from botocore.exceptions import ClientError
import opensearchpy
from opensearchpy import (
    OpenSearch, Index, RequestError, RequestsHttpConnection,
    RequestsAWSV4SignerAuth, Field
)
from opensearchpy.helpers.mapping import Mapping

from migrate.config import get_custom_logger
from migrate.exceptions import (OpenSearchMappingException, OPENSEARCH_PACKAGE_CREATION_FAILED,
                                OPENSEARCH_PACKAGE_UPDATE_FAILED, OPENSEARCH_PACKAGE_ASSOCIATION_FAILED,
                                OPENSEARCH_PACKAGE_DISSOCIATION_FAILED, OPENSEARCH_PACKAGE_LIST_FAILED,
                                OPENSEARCH_PACKAGE_DESCRIBE_FAILED, S3_BUCKET_ACCESS_DENIED,
                                S3_BUCKET_NOT_FOUND, S3_UPLOAD_FAILED)

logger = get_custom_logger("opensearch.opensearch_client")


class XYPointField(Field):
    """Custom XY Point field for OpenSearch."""
    name = 'xy_point'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._param_defs = {'type': 'xy_point'}


class OpenSearchClient:
    """Client for interacting with OpenSearch service and managing packages."""

    def __init__(self, config):
        # Store config directly to reduce instance attributes
        self._config = config
        self._domain = config['domain']
        self._bucket = config['bucket']

        # Initialize AWS clients
        region = config['region']
        self._opensearch_client_boto3 = boto3.client("opensearch", region_name=region)
        self._s3_client_boto3 = boto3.client("s3", region_name=region)

        # Setup authentication
        if config['use_aws_auth_sigv4']:
            session = boto3.Session()
            credentials = session.get_credentials()
            auth = RequestsAWSV4SignerAuth(credentials, region, 'es')
        else:
            auth = (config['username'], config['password'])

        logger.info("Initializing opensearch client with domain %s", self._domain)

        # Initialize OpenSearch client
        self._opensearch_client = OpenSearch(
            hosts=[{"host": config['host'], "port": int(config['port'])}],
            http_auth=auth,
            use_ssl=config['use_ssl'],
            verify_certs=config['verify_certs'],
            ssl_assert_hostname=config['assert_hostname'],
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
        )

        try:
            logger.info("")
        except opensearchpy.exceptions.OpenSearchException as e:
            logger.warning("Initializing failed for opensearch client with domain %s", self._domain)
            raise e

        logger.info("Initializing successful for opensearch client with domain %s", self._domain)

        # Initialize index components
        self._opensearch_index = Index(config['index'])
        self._mapping = Mapping()
        self._dynamic_templates = []

    def add_analyzer(self, analyzer):
        """Add analyzer to the index."""
        self._opensearch_index.analyzer(analyzer)

    def add_field(self, name, field):
        """Add field to the index mapping."""
        self._mapping.field(name, field)
        self._opensearch_index.mapping(self._mapping)

    def add_copy_field(self, name, field):
        """Add copy field to the index mapping."""
        self._mapping.field(name, field)
        self._opensearch_index.mapping(self._mapping)

    def add_dynamic_field(self, dynamic_field):
        """Add dynamic field template to the index."""
        self._dynamic_templates.append(dynamic_field)
        self._mapping.meta("dynamic_templates", self._dynamic_templates)
        self._opensearch_index.mapping(self._mapping)

    def get_all_analyzers(self):
        """Get all analyzers from the index."""
        return (self._opensearch_index.to_dict()
                .get("settings", {})
                .get("analysis", {})
                .get("analyzer", {}))

    def get_all_fields(self):
        """Get all fields from the index mapping."""
        return (self._opensearch_index.to_dict()
                .get("mappings", {})
                .get("properties", {}))

    def get_all_tokenizers(self):
        """Get all tokenizers from the index."""
        return (self._opensearch_index.to_dict()
                .get("settings", {})
                .get("analysis", {})
                .get("tokenizer", {}))

    def get_all_filters(self):
        """Get all filters from the index."""
        return (self._opensearch_index.to_dict()
                .get("settings", {})
                .get("analysis", {})
                .get("filter", {}))

    def get_index_json(self):
        """Get the complete index configuration as JSON."""
        return self._opensearch_index.to_dict()

    def _create_package(self, package_name, bucket, file):

        logger.info("Creating package with name %s", package_name)
        try:
            response = self._opensearch_client_boto3.create_package(
                PackageName=package_name,
                PackageType="TXT-DICTIONARY",
                PackageSource={"S3BucketName": bucket, "S3Key": file},
            )
            package_id = response['PackageDetails']['PackageID']
            self._wait_for_package_available(package_id)
            logger.info("Package created %s", package_id)
        except Exception as e:
            logger.warning("Could not create packages due to %s", e)
            raise OpenSearchMappingException(package_name, OPENSEARCH_PACKAGE_CREATION_FAILED) from e

    def _update_package(self, package_id, bucket, file):
        logger.info("Updating package with id %s", package_id)

        try:
            response = self._opensearch_client_boto3.update_package(
                PackageID=package_id,
                PackageSource={"S3BucketName": bucket, "S3Key": file},
            )
            package_status = response['PackageDetails']['PackageStatus']
            self._wait_for_package_available(package_id)
            logger.info("Package updated, current status %s", package_status)
        except Exception as e:
            logger.warning("Could not update packages due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_UPDATE_FAILED) from e

    def _associate_package(self, package_id):
        try:
            logger.info("Associating package with id %s", package_id)
            response = self._opensearch_client_boto3.associate_package(
                PackageID=package_id, DomainName=self._domain
            )
            package_status = response['DomainPackageDetails']['DomainPackageStatus']
            self._wait_for_association(package_id)
            logger.info("Package associated with status %s", package_status)
        except Exception as e:
            logger.warning("Could not associate packages due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_ASSOCIATION_FAILED) from e

    def _dissociate_package(self, package_id):
        try:
            logger.info("Disassociate package with id %s", package_id)
            response = self._opensearch_client_boto3.dissociate_package(
                PackageID=package_id, DomainName=self._domain
            )
            package_status = response['DomainPackageDetails']['DomainPackageStatus']
            self._wait_for_dissociation(package_id)

            logger.info("Package disassociated with status %s", package_status)

        except Exception as e:
            logger.warning("Could not disassociate packages due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_DISSOCIATION_FAILED) from e

    def _wait_for_association(self, package_id):
        try:
            response = self._opensearch_client_boto3.list_packages_for_domain(
                DomainName=self._domain
            )
            package_details = response["DomainPackageDetailsList"]
            for package in package_details:
                if package["PackageID"] == package_id:
                    status = package["DomainPackageStatus"]
                    if status == "ACTIVE":
                        logger.info("Association successful.")
                        return status
                    if status == "ASSOCIATION_FAILED":
                        logger.error("Association failed. Please try again.")
                        return status
                    time.sleep(10)
                    return self._wait_for_association(package_id)
        except Exception as e:
            logger.warning("Could not associate package for domain due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_ASSOCIATION_FAILED) from e


    def _wait_for_dissociation(self, package_id):
        try:
            response = self._opensearch_client_boto3.list_packages_for_domain(
                DomainName=self._domain
            )
            package_details = response["DomainPackageDetailsList"]
            for package in package_details:
                if package["PackageID"] == package_id:
                    status = package["DomainPackageStatus"]
                    if status == "AVAILABLE":
                        logger.info("Dissociation successful.")
                        return status
                    if status == "DISSOCIATION_FAILED":
                        logger.error("Association failed. Please try again.")
                        return status
                    time.sleep(10)
                    return self._wait_for_dissociation(package_id)
            return None
        except Exception as e:
            logger.warning("Could not disassociate package for domain due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_DISSOCIATION_FAILED) from e

    def _wait_for_package_available(self, package_id):
        """Wait for package to be in AVAILABLE state."""
        try:
            response = self._opensearch_client_boto3.describe_packages(
                Filters=[{"Name": "PackageID", "Value": [package_id]}]
            )
            if response["PackageDetailsList"]:
                package = response["PackageDetailsList"][0]
                status = package["PackageStatus"]
                logger.info("Package %s status: %s", package_id, status)
                
                if status == "AVAILABLE":
                    return status
                elif status in ["COPY_FAILED", "VALIDATION_FAILED", "DELETE_FAILED"]:
                    logger.error("Package %s failed with status: %s", package_id, status)
                    raise Exception(f"Package failed with status: {status}")
                else:
                    logger.info("Waiting for package %s to become available...", package_id)
                    time.sleep(10)
                    return self._wait_for_package_available(package_id)
        except Exception as e:
            logger.warning("Could not check package status due to %s", e)
            raise OpenSearchMappingException(package_id, OPENSEARCH_PACKAGE_DESCRIBE_FAILED) from e

    def _get_all_package_names(self):
        try:
            response = self._opensearch_client_boto3.describe_packages(MaxResults=100)
            package_list = response["PackageDetailsList"]
            package_name_list = [p["PackageName"] for p in package_list]
            return package_name_list
        except Exception as e:
            logger.warning("Could not describe packages due to %s", e)
            raise OpenSearchMappingException("all_packages", OPENSEARCH_PACKAGE_DESCRIBE_FAILED) from e


    def _get_domain_package_names(self):
        try:
            response = self._opensearch_client_boto3.list_packages_for_domain(
                DomainName=self._domain,
            )
            domain_package_list = response["DomainPackageDetailsList"]
            domain_package_name_list = [p["PackageName"] for p in domain_package_list]
            domain_package_name_version_dict = {
                p["PackageName"]: p["PackageVersion"] for p in domain_package_list
            }
            return domain_package_name_list, domain_package_name_version_dict
        except Exception as e:
            logger.warning("Could not list packages for domain due to %s", e)
            raise OpenSearchMappingException("domain_packages", OPENSEARCH_PACKAGE_DESCRIBE_FAILED) from e



    def _get_opensearch_package_by_name(self, package_name):
        try:
            response = self._opensearch_client_boto3.describe_packages(
                Filters=[
                    {"Name": "PackageName", "Value": [package_name]},
                ]
            )
            return response["PackageDetailsList"][0]
        except Exception as e:
            logger.warning("Could not describe packages due to %s", e)
            raise OpenSearchMappingException(None, OPENSEARCH_PACKAGE_DESCRIBE_FAILED) from e

    def _calculate_etag(self, file):
        # Calculate md5 hash
        md5 = hashlib.md5(usedforsecurity=False)
        with open(file, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5.update(chunk)

        # Calculate sha256 hash
        sha256 = hashlib.sha256()
        with open(file, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)

        return '"{}"'.format(md5.hexdigest()), '"{}"'.format(sha256.hexdigest())

    def _check_s3_bucket_access(self, bucket_name: str = None) -> Tuple[bool, str]:
        """Check if the S3 bucket exists and is accessible.

        Args:
            bucket_name: Name of the S3 bucket to check. If None, uses self._bucket.

        Returns:
            Tuple of (is_accessible, error_message).
            is_accessible is True if bucket exists and is accessible, False otherwise.
            error_message contains details if there was an error, empty string otherwise.
        """
        bucket = bucket_name or self._bucket
        try:
            # Check if bucket exists
            self._s3_client_boto3.head_bucket(Bucket=bucket)

            # Test if we can list objects (even if bucket is empty)
            self._s3_client_boto3.list_objects_v2(Bucket=bucket, MaxKeys=1)

            return True, ""

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False, S3_BUCKET_NOT_FOUND
            if error_code == '403':
                return False, S3_BUCKET_ACCESS_DENIED
            return False, f"Error accessing bucket {bucket}: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error accessing bucket {bucket}: {str(e)}"

    def _upload_to_s3(self, file_key, bucket, file_name):
        # Check bucket accessibility before attempting upload
        is_accessible, error_message = self._check_s3_bucket_access(bucket)
        if not is_accessible:
            logger.warning("S3 bucket accessibility check failed: %s", error_message)
            raise OpenSearchMappingException(None, error_message)

        try:
            self._s3_client_boto3.upload_file(file_name, bucket, file_key)
            logger.info("Upload successful - %s-%s-%s", bucket, file_key, file_name)
        except Exception as e:
            logger.warning("Could not Upload file to s3 %s", e)
            raise OpenSearchMappingException(None, S3_UPLOAD_FAILED) from e


    def create_and_associate_package(self, package_name: str, file):
        """Create and associate OpenSearch package with domain."""

        try:
            file_key = file
            package_name_list = self._get_all_package_names()
            if package_name not in package_name_list:
                # New package - upload and create
                self._upload_to_s3(file_key, self._bucket, file)
                self._create_package(package_name, self._bucket, file_key)
                package_details = self._get_opensearch_package_by_name(package_name)
                # Associate package
                self._associate_package(package_details["PackageID"])
                self._wait_for_association(package_details["PackageID"])
            else:
                # Existing package - check if content is different
                try:
                    obj = self._s3_client_boto3.head_object(Bucket=self._bucket, Key=file_key)
                    md5_hash, sha256_hash = self._calculate_etag(file)
                    if obj['ETag'] == md5_hash or obj['ETag'] == sha256_hash:
                        # Files are identical, skip upload and update
                        logger.info(f"File {file_key} is unchanged. Skipping update.")
                        package_details = self._get_opensearch_package_by_name(package_name)

                    else:
                        # Files are different, proceed with update
                        logger.info("File %s is changed. Proceeding with update.", file_key)
                        self._upload_to_s3(file_key, self._bucket, file)
                        package_details = self._get_opensearch_package_by_name(package_name)
                        self._update_package(package_details["PackageID"], self._bucket, file_key)
                        # Associate package
                        self._associate_package(package_details["PackageID"])
                        self._wait_for_association(package_details["PackageID"])

                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File doesn't exist in S3, upload it
                        self._upload_to_s3(file_key, self._bucket, file)
                        package_details = self._get_opensearch_package_by_name(package_name)
                        self._update_package(package_details["PackageID"], self._bucket, file_key)
                        # Associate package
                        self._associate_package(package_details["PackageID"])
                        self._wait_for_association(package_details["PackageID"])
                    else:
                        raise

            return package_details["PackageID"], package_details.get(
                "AvailablePackageVersion"
            ) if package_details.get("AvailablePackageVersion") else package_details.get(
                "PackageVersion"
            )
        except Exception as e:
            logger.warning("Error creating/associating package: %s", str(e))
            raise

    def create_index(self):
        """Create OpenSearch index with configured mappings and settings."""
        index_data = self._opensearch_index.to_dict()
        try:
            self._opensearch_client.indices.create(index=self._config['index'], body=index_data)
        except RequestError as e:
            logger.error(e.error)
