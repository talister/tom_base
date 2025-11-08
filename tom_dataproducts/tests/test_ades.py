from unittest.mock import patch
import logging
from pathlib import Path

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase  # , override_settings

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

    @patch('tom_dataproducts.processors.astrometry_processor.ADESProcessor._process_astrometry_from_plaintext')
    def test_process_astrometry_with_plaintext_file(self, mocked_method):
        """Test that ADESProcessor.process_data() calls ADESProcessor._process_astrometry_from_plaintext().
        """
        self.data_product.data.save('ades_astrometry.psv', self.data_product_filefield_data)

        # this is the call under test
        ADESProcessor().process_data(self.data_product)
        mocked_method.assert_called_with(self.data_product)

    def test_read(self):
        """Test that Stuff Happens

        The test data is a query on https://data.minorplanetcenter.net/explorer for '33933' with the resulting
           ADES XML converted to PSV with `xmltopsv.py` from `iau-ades` and heavily trimmed down.
        """
        # read the test data in as a data_product's data
        with open(Path('tom_dataproducts/tests/test_data/test_ades.psv')) as ades_file:
            self.data_product.data.save('test_data.csv', ades_file)

        # this is the call under test
        astrometry = ADESProcessor()._process_astrometry_from_plaintext(self.data_product)

        expected_count = 9  # known a priori from test data in test_atlas_fp.csv
        self.assertEqual(expected_count, len(astrometry))
