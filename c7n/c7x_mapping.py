resource_mapping = {
    "aws.iam-policy": {
        "arn": "Arn",
        "name": "PolicyName"
    },
    "aws.iam-user": {
        "arn": "Arn",
        "name": "UserName"
    },
    "aws.account": {
        "name": "account_id"
    },
    "aws.cloudtrail": {
        "name": "Name",
        "arn": "TrailARN"
    },
    "aws.s3": {
        "name": "Name"
    },
    "aws.security-group": {
        "name": "GroupId"
    },
    "aws.vpc": {
        "name": "VpcId"
    },
    "aws.ec2": {
        "name": "InstanceId"
    },
    "aws.rds": {
        "name": "DBInstanceIdentifier",
        "arn": "DBInstanceArn"
    },
    "aws.vpc-endpoint": {
        "name": "VpcEndpointId"
    },
    "aws.route-table": {
        "name": "RouteTableId"
    },
    "aws.kms-key": {
        "name": "KeyId",
        "arn": "KeyArn"
    },
    "aws.eni": {
        "name": "NetworkInterfaceId"
    },
    "aws.key-pair": {
        "name": "KeyPairId"
    },
    "aws.rds-snapshot": {
        "name": "DBSnapshotIdentifier",
        "arn": "DBSnapshotArn"
    },
    "aws.sns": {
        "name": "TopicArn"
    },
    "aws.efs": {
        "name": "Name",
        "arn": "FileSystemArn"
    }
}

a = [
    "aws.account",
    "aws.rds-snapshot",
    "aws.s3",
    "aws.kms-key",
    "aws.sns",
    "aws.efs",
    "aws.iam-user",
    "aws.key-pair",
    "aws.eni",
    "aws.route-table",
    "aws.rds",
    "aws.security-group",
    "aws.vpc-endpoint",
    "aws.vpc"
]
