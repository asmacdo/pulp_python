from gettext import gettext as _
import copy
import hashlib

from mongoengine import BooleanField, DictField, IntField, ListField, StringField
from pulp.server.db.model import FileContentUnit
from twine import package


from pulp_python.common import constants


DEFAULT_CHECKSUM_TYPE = 'sha512'


class Package(FileContentUnit):

    # json only fields
    _pypi_hidden = BooleanField()
    _pypi_ordering = IntField()
    bugtrack_url = StringField()
    cheesecake_code_kwalitee_id = StringField()
    cheesecake_documentation_id = StringField()
    cheesecake_installability_id = StringField()
    comment_text = StringField()
    docs_url = StringField()
    downloads = IntField()  # or dict if dist overrides package
    filename = StringField()
    has_sig = BooleanField()
    package_url = StringField()
    packagetype = StringField()
    path = StringField()
    python_version = StringField()
    release_url = StringField()
    size = IntField()
    upload_time = StringField()
    url = StringField()

    author = StringField()
    author_email = StringField()
    classifiers = ListField()
    comment = StringField()
    description = StringField()
    download_url = StringField()
    filetype = StringField()
    home_page = StringField()
    keywords = StringField()
    license = StringField()
    maintainer = StringField()
    maintainer_email = StringField()
    md5_digest = StringField()
    metadata_version = StringField()
    name = StringField(required=True)
    obsoletes = ListField()
    obsoletes_dist = ListField()
    platform = ListField()  # TODO asmacdo string from json?
    project_urls = ListField()
    provides = ListField()
    provides_dist = ListField()
    pyversion = StringField()
    requires = ListField()
    requires_dist = ListField()
    requires_external = ListField()
    requires_python = StringField()
    summary = StringField()
    supported_platform = ListField()
    version = StringField()

    unit_key_fields = ('filename',)

    _filename = StringField()
    _checksum = StringField()
    _checksum_type = StringField(default=DEFAULT_CHECKSUM_TYPE)
    _ns = StringField(default='units_python_package')
    _content_type_id = StringField(required=True, default=constants.PACKAGE_TYPE_ID)

    meta = {
        'allow_inheritance': False,
        'collection': 'units_python_package',
        'indexes': [{'fields': ['-filename'], 'unique': True}],
    }

    @classmethod
    def from_json(cls, package, version, info):
        all_data = copy.deepcopy(info)
        all_data.update(package)

        # Remove fields to be populated by twine
        all_data.pop('platform', None)
        all_data.pop('requires_external', None)
        # Use release version, not distribution version
        all_data['version'] = version
        # TODO asmacdo parse file name for extra data?
        # TODO asmacdo this should validate uniqueness here, but that will be done on save when
        #              this is converted to lazy
        all_data['_filename'] = all_data['filename']
        package = cls(**all_data)
        return package

    @classmethod
    def from_file(cls, path):
        meta_dict = package.PackageFile.from_filename(path, comment='').metadata_dictionary()
        return cls(**meta_dict)

    def update_from_file(self, path):
        meta_dict = package.PackageFile.from_filename(path, comment='').metadata_dictionary()
        for key, value in meta_dict.iteritems():
            setattr(self, key, value)

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
