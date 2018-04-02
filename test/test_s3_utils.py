from dcicutils.s3_utils import s3Utils


def test_s3Utils_creation():
    util = s3Utils(env='fourfront-mastertest')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-mastertest-system'


def test_s3Utils_creation_staging():
    util = s3Utils(env='staging')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-webprod-system'
    assert util.outfile_bucket == 'elasticbeanstalk-fourfront-webprod-wfoutput'
    assert util.raw_file_bucket == 'elasticbeanstalk-fourfront-webprod-files'
    assert util.url == 'http://staging.4dnucleome.org'


def test_s3Utils_creation_data():
    util = s3Utils(env='data')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-webprod-system'
    assert util.outfile_bucket == 'elasticbeanstalk-fourfront-webprod-wfoutput'
    assert util.raw_file_bucket == 'elasticbeanstalk-fourfront-webprod-files'
    assert util.url == 'https://data.4dnucleome.org'


def test_s3Utils_get_keys_for_data():
    util = s3Utils(env='data')
    keys = util.get_access_keys()
    assert keys['server'] == 'https://data.4dnucleome.org'


def test_s3Utils_get_keys_for_staging():
    util = s3Utils(env='staging')
    keys = util.get_ff_key()
    assert keys['server'] == 'http://staging.4dnucleome.org'


def test_s3Utils_get_higlass_key():
    util = s3Utils(env='staging')
    keys = util.get_higlass_key()
    assert isinstance(keys, dict)
    assert 3 == len(keys.keys())
