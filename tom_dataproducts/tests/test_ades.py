from unittest.mock import patch
import logging
from datetime import datetime
from pathlib import Path

from astropy.time import Time
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
import pandas as pd

from tom_dataproducts.models import DataProduct
from tom_dataproducts.processors.astrometry_processor import ADESProcessor


from tom_observations.tests.factories import NonSiderealTargetFactory

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestADESProcessor(TestCase):
    """Test the ADESProcessor(DataProcessor) class.
    """

    def setUp(self):
        self.target = NonSiderealTargetFactory.create()
        self.data_product = DataProduct.objects.create(target=self.target)
        self.data_product_filefield_data = SimpleUploadedFile('nonsense.psv', b'somedata')

        self.maxDiff = None

    @patch('tom_dataproducts.processors.astrometry_processor.ADESProcessor._process_astrometry_from_plaintext')
    def test_process_astrometry_with_plaintext_file(self, mocked_method):
        """Test that ADESProcessor.process_data() calls ADESProcessor._process_astrometry_from_plaintext().
        """
        self.data_product.data.save('ades_astrometry.psv', self.data_product_filefield_data)

        # this is the call under test
        ADESProcessor().process_data(self.data_product)
        mocked_method.assert_called_with(self.data_product)

    def test_read(self):
        """Test reading ADES astrometry from PSV.

        The test data is from a query on https://data.minorplanetcenter.net/explorer for '33933'.
        The resulting ADES XML was converted to PSV using `xmltopsv.py` from `iau-ades`
        and trimmed down for the test.
        """
        # read the test data in as a data_product's data
        with open(Path('tom_dataproducts/tests/test_data/test_ades.psv')) as ades_file:
            self.data_product.data.save('test_data.csv', ades_file)

        # this is the call under test
        astrometry = ADESProcessor()._process_astrometry_from_plaintext(self.data_product)

        expected_count = 9  # known a priori from test data in test_ades.psv
        self.assertEqual(expected_count, len(astrometry))
        expected_dt = Time(datetime(1971, 9, 16, 4, 20, 36, int(1e6 * 0.672)))
        expected_mag = expected_magerr = None
        self.assertEqual(expected_dt, astrometry[0]['timestamp'])
        self.assertEqual(expected_mag, astrometry[0]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[0]['mag_error'])

        expected_dt = Time(datetime(2025, 4, 27, 21, 51, 58, int(1e6 * 0.890)))
        expected_mag = 19.39
        expected_magerr = 0.137
        self.assertEqual(expected_dt, astrometry[-1]['timestamp'])
        self.assertAlmostEqual(expected_mag, astrometry[-1]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[-1]['mag_error'])

    def test_read_mpcexplorer(self):
        """Test reading ADES astrometry from PSV with a different format. The MPC's Observations API
        returns the underlying DB table names which aren't camelCase like the ADES standard, so test
        that we can read in that format as well.

        The test data is a query on https://data.minorplanetcenter.net/api/get-obs
        for '2009 DP2' with the `output_format`: `ADES_DF` and the resulting JSON read into
        a pandas DataFrame and then written out to PSV with `ades_df.to_csv('file.psv', sep='|')`
        and heavily trimmed down.
        """
        # read the test data in as a data_product's data
        with open(Path('tom_dataproducts/tests/test_data/test_ades_mpcexplorer.psv')) as ades_file:
            self.data_product.data.save('test_data.csv', ades_file)

        # this is the call under test
        astrometry = ADESProcessor()._process_astrometry_from_plaintext(self.data_product)

        expected_count = 5  # known a priori from test data in test_ades_mpcexplorer.psv
        self.assertEqual(expected_count, len(astrometry))
        expected_dt = Time(datetime(2009, 2, 17, 1, 29, 22, int(1e6 * 0.848)))
        expected_mag = 19.8
        expected_magerr = None
        self.assertEqual(expected_dt, astrometry[0]['timestamp'])
        self.assertEqual(expected_mag, astrometry[0]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[0]['mag_error'])

        expected_dt = Time(datetime(2026, 3, 10, 3, 39, 4, int(1e6 * 0.900)))
        expected_mag = 21.12
        expected_magerr = 0.202
        self.assertEqual(expected_dt, astrometry[-1]['timestamp'])
        self.assertAlmostEqual(expected_mag, astrometry[-1]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[-1]['mag_error'])

    def test_read_df(self):
        """Test that astrometry can be read from a Pandas DataFrame

        The test data is from the following query:
        ```
        response = requests.get("https://data.minorplanetcenter.net/api/get-obs",
        json={"desigs": ["33933"], "ades_version": "2022", "output_format": "ADES_DF"})
        if response.ok:
            ades_df = pd.DataFrame(response.json()[0]['ADES_DF'])
            top_n_tail = pd.concat([ades_df.head(3) , ades_df.tail(3)])
            top_n_tail.to_csv("tom_dataproducts/tests/test_data/test_ades_df.csv")
        ```
        """
        # read the test data in as a Pandas DataFrame
        self.test_ades_df = pd.read_csv('tom_dataproducts/tests/test_data/test_ades_df.csv')

        # this is the call under test
        astrometry = ADESProcessor()._process_astrometry_from_df(self.test_ades_df)

        expected_count = 6  # known a priori from test data in test_ades_df.csv
        self.assertEqual(expected_count, len(astrometry))
        expected_dt = Time(datetime(1971, 9, 16, 4, 20, 36, int(1e6 * 0.672)))
        expected_mag = expected_magerr = None
        self.assertEqual(expected_dt, astrometry[0]['timestamp'])
        self.assertEqual(expected_mag, astrometry[0]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[0]['mag_error'])

        expected_dt = Time(datetime(2025, 4, 27, 21, 51, 58, int(1e6 * 0.890)))
        expected_mag = 19.39
        expected_magerr = 0.137
        self.assertEqual(expected_dt, astrometry[-1]['timestamp'])
        self.assertAlmostEqual(expected_mag, astrometry[-1]['magnitude'])
        self.assertEqual(expected_magerr, astrometry[-1]['mag_error'])
