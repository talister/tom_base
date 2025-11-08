import logging
import mimetypes

import astropy.io.ascii
import numpy as np
from astropy import units as u
from astropy.time import Time, TimezoneInfo
from django.core.files.storage import default_storage

from tom_dataproducts.data_processor import DataProcessor
from tom_dataproducts.exceptions import InvalidFileFormatException

logger = logging.getLogger(__name__)


class ADESProcessor(DataProcessor):
    def data_type_override(self):
        return 'ades_astrometry'

    def process_data(self, data_product):
        """
        Routes an ADES processing call to a method specific to a file-format.


        :param data_product: ADES Astrometry DataProduct or pandas.DataFrame which will be processed into
            the specified format for database ingestion
        :type data_product: DataProduct|pandas.DataFrame

        :returns: python list of 3-tuples, each with a timestamp and corresponding data, and source
        :rtype: list
        """

        try:
            mimetype = mimetypes.guess_type(data_product.data.path)[0]
        except NotImplementedError:
            mimetype = 'text/plain'
        logger.debug(f'Processing ADES data with mimetype {mimetype}')

        if mimetype in self.PLAINTEXT_MIMETYPES:
            astrometry = self._process_astrometry_from_plaintext(data_product)
            return [(datum.pop('timestamp'), datum, datum.pop('source', 'MPC')) for datum in astrometry]
        else:
            raise InvalidFileFormatException('Unsupported file type')

        pass

    def _process_astrometry_from_plaintext(self, data_product):
        """
        Processes the ADES astrometry and photometry data from a plaintext file (in ADES Pipe Separated Value (PSV)
        format) into a list of dicts.
        Details on the ADES standard: https://data.minorplanetcenter.net/mpcops/documentation/ades/

        :param data_product: _description_
        :type data_product: DataProduct
        """
        astrometry = []
        data_file = default_storage.open(data_product.data.name, 'r')
        data = astropy.io.ascii.read(data_file.read())
        if len(data) < 1:
            raise InvalidFileFormatException('Empty table or invalid file type')

        # Mapping between returned quantities and ADES columns
        mapping = {
                    'ra_rmserror': 'rmsRA',
                    'dec_rmserror': 'rmsDec',
                    'magnitude': 'mag',
                    'mag_error': 'rmsMag',

        }
        try:
            utc = TimezoneInfo(utc_offset=0*u.hour)

            for row in data:
                time = Time(row['obsTime'], format='isot', scale='utc')
                time.format = 'datetime'
                value = {
                    'timestamp': time.to_datetime(timezone=utc),
                    'filter': str(row['band']),
                    'telescope': row['stn'],
                }
                value['ra'] = float(row['ra'])
                value['dec'] = float(row['dec'])
                for key, col in mapping.items():
                    value[key] = None
                    if np.ma.is_masked(row[col]) is False:
                        value[key] = float(row[col])
                astrometry.append(value)
        except Exception as e:
            raise InvalidFileFormatException(e)
        return astrometry

    def _process_astrometry_from_df(self, df):
        """


        :param df: ADES pandas.DataFrame which will be processed into a list of dicts for the measurements
        :type df: pandas.DataFrame
        :return: python list containing the astrometric data from the DataFrame
        :rtype: list
        """
        astrometry = []
        return astrometry
