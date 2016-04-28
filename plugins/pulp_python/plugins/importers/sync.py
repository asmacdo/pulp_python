"""
This module contains the necessary means for a necessary means for syncing packages from PyPI.
"""
import contextlib
from cStringIO import StringIO
from gettext import gettext as _
import json
import logging
import os
from urlparse import urljoin

import mongoengine
from nectar import request
from pulp.common.plugins import importer_constants
from pulp.plugins.util import publish_step
from pulp.server.controllers import repository as repo_controller

from pulp_python.common import constants
from pulp_python.plugins import models


_logger = logging.getLogger(__name__)


class DownloadMetadataStep(publish_step.DownloadStep):
    """
    This DownloadStep subclass contains the code to process the downloaded manifests and determine
    what is available from the feed. It does this as it gets each metadata file to spread the load
    on the database.
    """

    def __init__(self, *args, **kwargs):
        """
        Injects download requests into the call to the parent class's __init__
        """
        kwargs['downloads'] = self.generate_download_requests()
        super(DownloadMetadataStep, self).__init__(*args, **kwargs)

    def download_failed(self, report):
        """
        This method is called by Nectar when we were unable to download the metadata file for a
        particular Python package. It closes the StringIO that we were using to store the download
        to free memory.

        :param report: The report that details the download
        :type  report: nectar.report.DownloadReport
        """
        report.destination.close()

        super(DownloadMetadataStep, self).download_failed(report)

    def download_succeeded(self, report):
        """
        This method is called by Nectar for each package metadata file after it is successfully
        downloaded. It reads the manifest and adds each available unit to the parent step's
        "available_units" attribute.

        It also adds each unit's download URL to the parent step's "unit_urls" attribute. Each key
        is a models.Package object.

        :param report: The report that details the download
        :type  report: nectar.report.DownloadReport
        """
        _logger.info(_('Processing metadata retrieved from %(url)s.') % {'url': report.url})
        with contextlib.closing(report.destination) as destination:
            destination.seek(0)
            self._process_metadata(destination.read())

        super(DownloadMetadataStep, self).download_succeeded(report)

    def generate_download_requests(self):
        """
        Yield a Nectar DownloadRequest for the json metadata of each requested Python distribution.

        :return: A generator that yields DownloadRequests for the metadata files.
        :rtype:  generator
        """
        manifest_urls = [urljoin(self.parent._feed_url, 'pypi/%s/json/' % pn)
                         for pn in self.parent._package_names]
        for u in manifest_urls:
            if self.canceled:
                return
            yield request.DownloadRequest(u, StringIO(), {})

    def _process_metadata(self, metadata):
        """
        This method creates Packages from the json metadata provided by the pypi json api.

        https://wiki.python.org/moin/PyPIJSON

        :param metadata: Describes all available versions and packages for a python distribution
        :type  manifest: basestring in JSON format
        """
        metadict = json.loads(metadata)
        for version, packages in metadict['releases'].items():
            for package in packages:
                unit = models.Package.from_json(package, version, metadict['info'])
                self.parent.available_units.append(unit)


class DownloadPackagesStep(publish_step.DownloadStep):
    """
    This DownloadStep retrieves the packages from the feed, processes each package for its metadata,
    and adds the unit to the repository in Pulp.
    """

    def download_succeeded(self, report):
        """
        This method saves and associates a Package after it has been successfully downloaded.

        This method also ensures that the checksum of the downloaded package matches the checksum
        that was listed in the metadata. If everything checks out, the package is added to the
        repository and moved to the proper storage path.

        :param report: The report that details the download
        :type  report: nectar.report.DownloadReport
        """
        package = report.data
        _logger.info(_('Processing package retrieved from %(url)s.') % {'url': report.url})

        checksum = models.Package.checksum(report.destination, "md5")
        if checksum != package.md5_digest:
            report.state = 'failed'
            report.error_report = {'expected_checksum': package.md5_digest,
                                   'actual_checksum': checksum}
            return self.download_failed(report)

        package.update_from_file(report.destination)
        package.set_storage_path(os.path.basename(report.destination))
        try:
            package.save()
        except Exception, e:
            import rpdb; rpdb.set_trace()
        # TODO asmacdo switch to dup key?
        # except mongoengine.NotUniqueError:

        package.import_content(report.destination)
        repo_controller.associate_single_unit(self.get_repo().repo_obj, package)
        super(DownloadPackagesStep, self).download_succeeded(report)


class SyncStep(publish_step.PluginStep):
    """
    Fake sync step for development.
    """
    def __init__(self, repo, conduit, config, working_dir):
        super(SyncStep, self).__init__('sync_step_main', repo, conduit, config, working_dir,
                                       constants.IMPORTER_TYPE_ID)
        self.description = _('Synchronizing %(id)s repository.') % {'id': repo.id}

        self._feed_url = config.get(importer_constants.KEY_FEED)
        self._package_names = config.get(constants.CONFIG_KEY_PACKAGE_NAMES, [])
        if self._package_names:
            self._package_names = self._package_names.split(',')
        self.available_units = []

        self.add_child(
            DownloadMetadataStep(
                'sync_step_download_metadata',
                repo=repo, config=config, conduit=conduit, working_dir=self.get_working_dir(),
                description=_('Downloading Python metadata.')))

        self.add_child(
            DownloadPackagesStep(
                'sync_step_download_packages', downloads=self.generate_download_requests(),
                repo=repo, config=config, conduit=conduit, working_dir=self.get_working_dir(),
                description=_('Downloading and processing Python packages.')))

    def generate_download_requests(self):
        """
        For each package that wasn't available locally, yield a Nectar
        DownloadRequest for its url attribute.

        :return: A generator that yields DownloadReqests for the Package files.
        :rtype:  generator
        """
        for p in self.available_units:
            destination = os.path.join(self.get_working_dir(), os.path.basename(p.url))
            yield request.DownloadRequest(p.url, destination, p)

    def sync(self):
        """
        Perform the repository synchronization.

        :return: The final sync report.
        :rtype:  pulp.plugins.model.SyncReport
        """
        self.process_lifecycle()
        repo_controller.rebuild_content_unit_counts(self.get_repo().repo_obj)
        return self._build_final_report()
