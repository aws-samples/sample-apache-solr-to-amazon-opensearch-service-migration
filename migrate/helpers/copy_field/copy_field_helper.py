from migrate.config import get_custom_logger
from collections import namedtuple

from migrate.exceptions import MigrationException, CopyFieldMappingException, MAPPING_NOT_FOUND

logger = get_custom_logger("migrate.copy_field")

# Define CopyFieldResult namedtuple
CopyFieldResult = namedtuple('CopyFieldResult', ['src', 'src_def', 'dst', 'dst_def'])




class CopyFieldHelper(object):
    def __init__(self, all_fields):
        self._all_fields = all_fields
        self._field_copy_to = {}  # Just to track source -> destinations for array handling

    def map_copy_field(self, solr_copy_field):
        src = solr_copy_field["source"]
        dst = solr_copy_field["dest"]
        logger.debug("Processing copy field from %s to %s", src, dst)
        src_def = self._all_fields.get(src)
        dst_def = self._all_fields.get(dst)
        if src_def is None or dst_def is None:
            raise CopyFieldMappingException(name=src, message=MAPPING_NOT_FOUND, src_field=src)

        try:

            # Handle multiple destinations for same source
            if src not in self._field_copy_to:
                self._field_copy_to[src] = [dst]
                src_def["copy_to"] = dst
            else:
                self._field_copy_to[src].append(dst)
                if "copy_to" in src_def:
                    if isinstance(src_def["copy_to"], list):
                        src_def["copy_to"].append(dst)
                    else:
                        src_def["copy_to"] = [src_def["copy_to"], dst]
                else:
                    src_def["copy_to"] = [dst]

            return CopyFieldResult(src, src_def, dst, dst_def)
        except CopyFieldMappingException as e:
            raise e
        except Exception as e:
            raise CopyFieldMappingException(name=src, message=e, src_field=src)
