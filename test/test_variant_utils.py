import pytest
from unittest import mock
from contextlib import contextmanager
from dcicutils import variant_utils
from dcicutils.variant_utils import VariantUtils
from unittest.mock import patch


def create_dummy_keydict():
    return {'cgap-dummy': {
        'key': 'dummy', 'secret': 'dummy',
        'server': 'cgap-test.com'
    }}


class TestVariantUtils:

    class CGAPKeyManager:
        def get_keydict_for_env(self, *, env):
            return create_dummy_keydict()['cgap-dummy']

    @contextmanager
    def mock_key_manager(self):
        with mock.patch.object(variant_utils, 'CGAPKeyManager', new=self.CGAPKeyManager):
            yield

    def test_variant_utils_basic(self):
        """ Tests the instantiation of a VariantUtils object """
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap-dummy')
            assert isinstance(vu, VariantUtils)

    @pytest.mark.parametrize('total_value', [
        100,
        200,
        300,
        400
    ])
    @patch('dcicutils.variant_utils.get_metadata')
    def test_get_total_result_count_from_search(self, mock_get_metadata, total_value):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap-dummy')
            mock_gene = 'GENE'
            mock_get_metadata.return_value = {'total': total_value}
            result = vu.get_total_result_count_from_search(mock_gene)
            expected_result = total_value
            assert result == expected_result
            mock_get_metadata.assert_called_once_with(f'/search/?type=VariantSample&limit=1\
                                                      &variant.genes.genes_most_severe_gene.display_title={mock_gene}',
                                                      key=vu.creds)

    @pytest.mark.parametrize('returned_variants, expected_length', [
        ([{'variant': {'POS': 100000}}], 8),
        ([{'variant': {'POS': 100000}}], 9),
        ([{'variant': {'POS': 100000}}], 10),
        ([{'variant': {'POS': 100000}}], 11),
    ])
    @patch('dcicutils.variant_utils.VariantUtils.get_rare_variants_by_gene')
    def test_create_dict_of_mutations(self, mock_get_rare_variants_by_gene, returned_variants, expected_length):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap-dummy')
            mock_gene = 'GENE'
            mock_get_rare_variants_by_gene.return_value = (returned_variants * expected_length)
            result = vu.create_dict_of_mutations(mock_gene)
            if expected_length >= 10:
                expected_result = {mock_gene: {100000: expected_length}}
            else:
                expected_result = {mock_gene: {}}
            assert result == expected_result
            mock_get_rare_variants_by_gene.assert_called_once_with(gene=mock_gene, sort='variant.ID')

    @patch('dcicutils.variant_utils.VariantUtils.return_json')
    def test_create_list_of_msa_genes(self, mock_return_json):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap-dummy')
            mock_return_json.return_value = [
                {'gene_symbol': 'GENE1', 'gene_summary': '...nerv...'},
                {'gene_symbol': 'GENE2', 'gene_summary': '..........'},
                {'gene_symbol': 'GENE3', 'gene_summary': '...neur...'}
            ]
            result = vu.create_list_of_msa_genes()
            expected_result = ['GENE1', 'GENE3']
            assert result == expected_result
            mock_return_json.assert_called_once_with('gene.json')

    @patch('dcicutils.variant_utils.VariantUtils.get_rare_variants_by_gene')
    def test_find_number_of_sample_ids(self, mock_get_rare_variants_by_gene):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap_dummy')
            mock_gene = 'GENE'
            mock_get_rare_variants_by_gene.return_value = [
                {'CALL_INFO': 'ABC123'},
                {'CALL_INFO': 'ABC123'},
                {'CALL_INFO': 'BCD234'},
                {'CALL_INFO': 'CDE345'}
            ]
            result = vu.find_number_of_sample_ids(mock_gene)
            expected_result = 3
            assert result == expected_result
            mock_get_rare_variants_by_gene.assert_called_once_with(gene=mock_gene, sort='variant.ID')

    @pytest.mark.parametrize('pos', [
        '100000',
        '200000',
        '300000',
        '400000'
    ])
    @patch('dcicutils.variant_utils.VariantUtils.create_dict_from_json_file')
    def test_create_url(self, mock_create_dict_from_json_file, pos):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap_dummy')
            mock_gene = 'GENE'
            mock_create_dict_from_json_file.return_value = {
                'GENE': {pos: 20, '123456': 10},
                'OTHER_GENE': {pos: 10}
            }
            result = vu.create_url(gene=mock_gene)
            expected_result = vu.SEARCH_RARE_VARIANTS_BY_GENE + mock_gene + ('&variant.POS.from={pos}\
                                                                             &variant.POS.to={pos}&sort=-DP')
            assert result == expected_result
            mock_create_dict_from_json_file.assert_called_once_with('10+sorted_msa_genes_and_mutations.json')

    @patch('dcicutils.variant_utils.VariantUtils.return_json')
    def test_create_list_of_als_park_genes(self, mock_return_json):
        with self.mock_key_manager():
            vu = VariantUtils(env_name='cgap-dummy')
            mock_return_json.return_value = [
                {'gene_symbol': 'GENE1', 'gene_summary': '...Parkinson...'},
                {'gene_symbol': 'GENE2', 'gene_summary': '...............'},
                {'gene_symbol': 'GENE3', 'gene_summary': '.....ALS.......'}
            ]
            result = vu.create_list_of_als_park_genes()
            expected_result = ['GENE1', 'GENE3']
            assert result == expected_result
            mock_return_json.assert_called_once_with('gene.json')
