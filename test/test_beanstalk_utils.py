from dcicutils import beanstalk_utils as bs
from unittest import mock


def test_get_beanstalk_prod_url():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as man_not_hot:
        man_not_hot.return_value = 'webprod-1'
        url = bs.get_beanstalk_real_url('webprod-1')
        assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_staging_url():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as man_not_hot:
        man_not_hot.return_value = 'webprod-2'
        url = bs.get_beanstalk_real_url('webprod-1')
        assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_normal_url():
    with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as man_not_hot:
        man_not_hot.return_value = {'CNAME': 'take-of-your-jacket'}
        url = bs.get_beanstalk_real_url('take-of-your-jacket')
        assert url == 'http://take-of-your-jacket'
