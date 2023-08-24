import json
from dcicutils.ff_utils import get_metadata, search_metadata
from dcicutils.creds_utils import CGAPKeyManager


class VariantUtils:

    SEARCH_VARIANTS_BY_GENE = (f'/search/?type=VariantSample&limit=1'
                               f'&variant.genes.genes_most_severe_gene.display_title=')
    SEARCH_RARE_VARIANTS_BY_GENE = (f'/search/?samplegeno.samplegeno_role=proband&type=VariantSample'
                                    f'&variant.csq_gnomadg_af_popmax.from=0&variant.csq_gnomadg_af_popmax.to=0.001'
                                    f'&variant.genes.genes_most_severe_gene.display_title=')

    def __init__(self, *, env_name) -> None:
        self._key_manager = CGAPKeyManager()
        self.creds = self._key_manager.get_keydict_for_env(env=env_name)
        # Uncomment this if needed
        # self.health = get_health_page(key=self.creds)
        self.base_url = self.creds['server']

    def get_creds(self):
        return self.creds

    def get_rare_variants_by_gene(self, *, gene, sort, addon=''):
        """Searches for rare variants on a particular gene"""
        return search_metadata(f'{self.base_url}/{self.SEARCH_RARE_VARIANTS_BY_GENE}{gene}\
                               &sort=-{sort}{addon}', key=self.creds)

    def find_number_of_sample_ids(self, gene):
        """Returns the number of samples that have a mutation on the specified gene"""
        return len(set(variant.get('CALL_INFO')
                       for variant in self.get_rare_variants_by_gene(gene=gene, sort='variant.ID')))

    def get_total_result_count_from_search(self, gene):
        """Returns total number of variants associated with specified gene"""
        res = get_metadata(self.SEARCH_VARIANTS_BY_GENE + gene, key=self.creds)
        return res['total']

    @staticmethod
    def sort_dict_in_descending_order(unsorted_dict):
        """Sorts dictionary in descending value order"""
        sorted_list = sorted(unsorted_dict.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_list)

    def create_dict_of_mutations(self, gene):
        """Creates dictionary of specified gene and mutations that occur 10+ times in database, in the form:
            {gene: {mutation1 pos: #variants, mutation2 pos: #variants, ...}"""
        mutation_dict = {}
        unique_positions = set()
        for variant in self.get_rare_variants_by_gene(gene=gene, sort='variant.ID'):
            pos = variant['variant']['POS']
            if pos not in unique_positions:
                unique_positions.add(pos)
                mutation_dict[pos] = 1
            else:
                mutation_dict[pos] += 1
        return {gene: self.sort_dict_in_descending_order({k: v for k, v in mutation_dict.items() if v >= 10})}

    @staticmethod
    def return_json(file_name):
        with open(file_name, 'r') as f:
            file_content = json.loads(f)
        return file_content

    @staticmethod
    def create_dict_from_json_file(file_name):
        """Creates dictionary object from specified json file"""
        with open(file_name) as f:
            json_list = f.read()
        return json.loads(json_list)

    def create_list_of_msa_genes(self):
        """Creates list of genes relating to the brain or nervous system
            (determined by whether keywords 'neur' or 'nerv' in summary)"""
        genes = self.return_json('gene.json')
        return [gene['gene_symbol'] for gene in genes
                if 'nerv' in gene.get('gene_summary', '')
                or 'neur' in gene.get('gene_summary', '')]

    def create_url(self, gene):
        """Returns a url to the variants at the most commonly mutated position of specified gene"""
        d = self.create_dict_from_json_file('10+sorted_msa_genes_and_mutations.json')
        pos = list(d[gene].keys())[0]
        return self.SEARCH_RARE_VARIANTS_BY_GENE + gene + f'&variant.POS.from={pos}&variant.POS.to={pos}&sort=-DP'

    def create_list_of_als_park_genes(self):
        """Creates list of genes that relating to Parkinson's or ALS
            (determined by whether keywords 'Parkinson' or 'ALS' in summary)"""
        genes = self.return_json('gene.json')
        return [gene['gene_symbol'] for gene in genes
                if 'Parkinson' in gene.get('gene_summary', '')
                or 'ALS' in gene.get('gene_summary', '')]
