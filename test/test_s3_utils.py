from dcicutils.s3_utils import s3Utils


def test_s3Utils_creation():
    util = s3Utils(env='fourfront-mastertest')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-mastertest-system'
