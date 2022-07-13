# Unit tests for the qa_utils MockBoto3Ec2 security group related mocks.

from botocore.exceptions import ClientError as Boto3ClientError
from typing import Optional
from dcicutils.qa_utils import MockBoto3, MockBoto3Ec2


class TestData:
    security_group_name = "some-security-group-name"
    security_group_id = "sg-0561068965d07c4af"
    security_group_rule_id = "sgr-004642a15cf58f9ad"
    security_group_rule_ip_protocol = "icmp"
    security_group_rule_is_egress = True
    security_group_rule_from_port = 4
    security_group_rule_to_port = -1
    security_group_rule_cidr = "0.0.0.0/0"
    security_group_rule_description = "ICMP for sentieon server"

    security_group_rule = {
        "SecurityGroupRuleId": security_group_rule_id,
        "GroupId": security_group_id,
        "IpProtocol": security_group_rule_ip_protocol,
        "IsEgress": security_group_rule_is_egress,
        "FromPort": security_group_rule_from_port,
        "ToPort": security_group_rule_to_port,
        "CidrIpv4": security_group_rule_cidr,
        "Description": security_group_rule_description,
        "Tags": []
    }

    security_group_rule_for_authorize = {
        "IpProtocol": security_group_rule_ip_protocol,
        "FromPort": security_group_rule_from_port,
        "ToPort": security_group_rule_to_port,
        "IpRanges": [{"CidrIp": security_group_rule_cidr, "Description": security_group_rule_description}]
    }


def assert_security_group_rule_exists(mock_boto_ec2: MockBoto3Ec2,
                                      security_group_name: str,
                                      security_group_id: str,
                                      security_group_rule_id: Optional[str],
                                      security_group_rule: Optional[dict],
                                      security_group_rule_is_egress: Optional[bool],
                                      total_existing_groups: int,
                                      total_existing_rules: int) -> None:

    # Check the total number of existing security groups.
    mocked_security_groups = mock_boto_ec2.describe_security_groups()
    mocked_security_group_list = mocked_security_groups["SecurityGroups"]
    assert len(mocked_security_group_list) == total_existing_groups
    mocked_security_group = [group for group in mocked_security_group_list if group["GroupId"] == security_group_id]
    assert len(mocked_security_group) == 1

    # Check the given security group by filter.
    security_group_filter = {"Name": "tag:Name", "Values": [security_group_name]}
    mocked_security_groups = mock_boto_ec2.describe_security_groups(Filters=[security_group_filter])
    mocked_security_group_list = mocked_security_groups["SecurityGroups"]
    assert len(mocked_security_group_list) == 1
    assert mocked_security_group_list[0]["GroupId"] == security_group_id

    # Check BTW a non-existent group by filter.
    security_group_filter = {"Name": "tag:Name", "Values": ["group-name-that-does-not-exist"]}
    mocked_security_groups = mock_boto_ec2.describe_security_groups(Filters=[security_group_filter])
    mocked_security_group_list = mocked_security_groups["SecurityGroups"]
    assert len(mocked_security_group_list) == 0

    # Check the total number of rules for the given security group.
    security_group_filter = {"Name": "group-id", "Values": [security_group_id]}
    existing_security_group_rules = mock_boto_ec2.describe_security_group_rules(Filters=[security_group_filter])
    existing_security_group_rule_list = existing_security_group_rules["SecurityGroupRules"]
    assert len(existing_security_group_rule_list) == total_existing_rules

    # Check the given rule (if given).
    if security_group_rule:
        existing_security_group_rule_list_items = [rule for rule in existing_security_group_rule_list
                                                   if rule["SecurityGroupRuleId"] == security_group_rule_id]
        assert len(existing_security_group_rule_list_items) == 1
        existing_security_group_rule = existing_security_group_rule_list_items[0]
        assert existing_security_group_rule["SecurityGroupRuleId"] == security_group_rule_id
        assert existing_security_group_rule["GroupId"] == security_group_id
        assert existing_security_group_rule["IpProtocol"] == security_group_rule["IpProtocol"]
        assert existing_security_group_rule["IsEgress"] == security_group_rule_is_egress
        assert existing_security_group_rule["FromPort"] == security_group_rule["FromPort"]
        assert existing_security_group_rule["ToPort"] == security_group_rule["ToPort"]
        # Handle both kinds of security group rule formats, either as returned by
        # describe_security_group_rules or as passed to authorize_security_group_ingress/egress.
        if security_group_rule.get("GroupId"):
            assert security_group_rule.get("GroupId") == security_group_id
        if security_group_rule.get("IpRanges"):
            assert existing_security_group_rule["CidrIpv4"] == security_group_rule["IpRanges"][0]["CidrIp"]
        else:
            assert existing_security_group_rule["CidrIpv4"] == security_group_rule["CidrIpv4"]


def test_mock_boto3_ec2_security() -> None:

    mock_boto = MockBoto3()
    mock_boto_ec2 = MockBoto3Ec2()

    assert isinstance(mock_boto, MockBoto3)
    assert isinstance(mock_boto_ec2, MockBoto3Ec2)

    security_group_name = TestData.security_group_name

    # Define a pre-existing security group rule.
    mock_boto_ec2.put_security_group_rule_for_testing(security_group_name, TestData.security_group_rule)

    # Sanity check the pre-existing rule defined above.
    assert_security_group_rule_exists(mock_boto_ec2,
                                      security_group_name=TestData.security_group_name,
                                      security_group_id=TestData.security_group_id,
                                      security_group_rule_id=TestData.security_group_rule_id,
                                      security_group_rule=TestData.security_group_rule,
                                      security_group_rule_is_egress=TestData.security_group_rule_is_egress,
                                      total_existing_groups=1,
                                      total_existing_rules=1)

    # This rule authorize should throw a botocore.exceptions.ClientError exception because it is a duplicate.
    try:
        mock_boto_ec2.authorize_security_group_egress(GroupId=TestData.security_group_id,
                                                      IpPermissions=[TestData.security_group_rule_for_authorize])
        assert False
    except Boto3ClientError as e:
        exception_raised = True
        assert e.response["Error"]["Code"] == "InvalidPermission.Duplicate"
    assert exception_raised

    # This rule authorize should not throw an exception because it is not duplicate (same rule but ingress not egress).
    response = mock_boto_ec2.authorize_security_group_ingress(
                  GroupId=TestData.security_group_id,
                  IpPermissions=[TestData.security_group_rule_for_authorize])
    assert isinstance(response, dict)
    security_group_rule_id = response["SecurityGroupRules"][0]["SecurityGroupRuleId"]
    assert_security_group_rule_exists(mock_boto_ec2,
                                      security_group_name=TestData.security_group_name,
                                      security_group_id=TestData.security_group_id,
                                      security_group_rule_id=security_group_rule_id,
                                      security_group_rule=TestData.security_group_rule_for_authorize,
                                      security_group_rule_is_egress=False,
                                      total_existing_groups=1,
                                      total_existing_rules=2)

    # Try revocation.
    mock_boto_ec2.revoke_security_group_ingress(GroupId=TestData.security_group_id,
                                                SecurityGroupRuleIds=[security_group_rule_id])
    assert_security_group_rule_exists(mock_boto_ec2,
                                      security_group_name=TestData.security_group_name,
                                      security_group_id=TestData.security_group_id,
                                      security_group_rule_id=TestData.security_group_rule_id,
                                      security_group_rule=TestData.security_group_rule,
                                      security_group_rule_is_egress=TestData.security_group_rule_is_egress,
                                      total_existing_groups=1,
                                      total_existing_rules=1)
