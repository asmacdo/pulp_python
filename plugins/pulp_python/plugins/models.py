from gettext import gettext as _
import copy
import hashlib

from mongoengine import BooleanField, IntField, ListField, StringField
from pulp.server.db.model import FileContentUnit

from pulp_python.common import constants


DEFAULT_CHECKSUM_TYPE = 'sha512'


class Package(FileContentUnit):

    # Distribution fields
    _pypi_hidden = BooleanField()
    _pypi_ordering = IntField()
    author = StringField()
    author_email = StringField()
    bugtrack_url = StringField()
    cheesecake_code_kwalitee_id = StringField()
    cheesecake_documentation_id = StringField()
    cheesecake_installability_id = StringField()
    classifiers = ListField()
    description = StringField()
    docs_url = StringField()
    download_url = StringField()
    home_page = StringField()
    keywords = StringField()
    license = StringField()
    maintainer = StringField()
    maintainer_email = StringField()
    name = StringField()
    package_url = StringField()
    # how is this field populated? concat?
    platform = StringField()
    # url for latest release
    release_url = StringField()
    requires_python = StringField()
    summary = StringField()
    version = StringField()

    # Release fields
    comment_text = StringField()
    downloads = IntField()
    filename = StringField()
    has_sig = BooleanField()
    md5_digest = StringField()
    packagetype = StringField()
    packagetype = StringField()
    path = StringField()
    python_version = StringField()
    size = IntField()
    # TODO asmacdo datetime?
    upload_time = StringField()
    url = StringField()

    # Internal fields
    _checksum = StringField()
    _checksum_type = StringField(default=DEFAULT_CHECKSUM_TYPE)
#
    unit_key_fields = ('filename',)

    _ns = StringField(default='units_python_package')
    _content_type_id = StringField(required=True, default=constants.PACKAGE_TYPE_ID)

    meta = {
        'allow_inheritance': False,
        'collection': 'units_python_package',
        'indexes': [],
    }

    @classmethod
    def from_json(cls, package, version, info):
        all_data = copy.deepcopy(info)
        all_data.update(package)
        # Use release version, not distribution version
        all_data['version'] = version
        # TODO asmacdo parse file name for extra data?
        # TODO asmacdo this should validate uniqueness here, but that will be done on save when
        #              this is converted to lazy
        package = cls(**all_data)
        return package

    @staticmethod
    def checksum(path, algorithm=DEFAULT_CHECKSUM_TYPE):
        """
        Return the checksum of the given path using the given algorithm.

        :param path:      A path to aDictField,  file
        :type  path:      basestring
        :param algorithm: The hashlib algorithm you wish to use
        :type  algorithm: basestring
        :return:          The file's checksum
        :rtype:           basestring
        """
        chunk_size = 32 * 1024 * 1024
        hasher = getattr(hashlib, algorithm)()
        with open(path) as file_handle:
            bits = file_handle.read(chunk_size)
            while bits:
                hasher.update(bits)
                bits = file_handle.read(chunk_size)
        return hasher.hexdigest()
