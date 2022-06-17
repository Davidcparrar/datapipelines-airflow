""" 
IAC script to create AWS Redshift Cluster
Based on IaC Solution.ipynb notebook from Udacity Data Engineer Nanodegree
"""
import configparser
import json
import sys
import argparse
import boto3
import pandas as pd

# ---------------------------------------------------------------------
# --------------------- LOAD ENVIROMENT VARIABLES ---------------------
# ---------------------------------------------------------------------

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
IAM_ROLE_NAME = config.get("IAM_ROLE", "NAME")

boto3.DEFAULT_SESSION = None


def get_client(
    aws_access_key: str, aws_secret_access_key: str, region_name: str
) -> dict:
    """Get clients needed for cluster administration

    Args:
        aws_access_key (str): AWS access key
        aws_secret_access_key (str): AWS secret access key
        region_name (str): AWS region

    Returns:
        dict: AWS Clients
    """

    kwargs = {
        "region_name": region_name,
        "aws_access_key_id": aws_access_key,
        "aws_secret_access_key": aws_secret_access_key,
    }

    session = boto3.session.Session(**kwargs)

    client = {}
    client["ec2"] = session.resource("ec2")
    client["iam"] = session.client("iam")
    client["redshift"] = session.client("redshift")

    return client


def create_rol(iam, role_name: str) -> str:
    """Create rol for Redshift Cluster"""

    print("Creating a new IAM Role")
    _ = iam.create_role(
        Path="/",
        RoleName=role_name,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "redshift.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            }
        ),
    )

    print("Attaching Policy")

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]

    return role_arn


def create_redshift_cluster(redshift, role_arn: str) -> None:
    """Create AWS Redshift Cluster"""
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[role_arn],
        )
    except Exception as e:
        print(e)


def get_cluster_status(redshift) -> None:
    """Get cluster status and, cluster endpoint and role if status = available"""
    cluster_metadata = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in cluster_metadata.items() if k in keysToShow]

    print(pd.DataFrame(data=x, columns=["Key", "Value"]))
    print()

    if cluster_metadata.get("ClusterStatus") == "available":
        print(f"Cluster Address: {cluster_metadata['Endpoint']['Address']}")
        print(f"Iam role ARN: {cluster_metadata['IamRoles'][0]['IamRoleArn']}")

    return cluster_metadata


def open_tcp_port(ec2, cluster_metadata: dict) -> None:
    """Open TCP Port on default security group"""
    try:
        vpc = ec2.Vpc(id=cluster_metadata["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)
    else:
        print("TCP Port opened")


def delete_cluster(redshift):
    """Delete Redshift Cluster"""
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )


def delete_role(iam):
    """Delete Role"""
    iam.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    print("Role {IAM_ROLE_NAME} detatched")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    print("Role {IAM_ROLE_NAME} deleted")


def parse_args() -> dict:
    """Parses command line arguments

    Returns:
        dict: Parsed arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c",
        "--create",
        help="Create Redshift Cluster",
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--status",
        help="Check Cluster status",
        action="store_true",
    )
    parser.add_argument(
        "-d",
        "--delete",
        help="Delete cluster",
        action="store_true",
    )
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()

    return args


def main(args):
    """Main Function"""

    # Change width
    pd.set_option("display.max_colwidth", None)

    client = get_client(AWS_KEY, AWS_SECRET, AWS_REGION)

    if args.create:
        print("Creating Cluster")
        role_arn = create_rol(client["iam"], IAM_ROLE_NAME)
        create_redshift_cluster(client["redshift"], role_arn)
        cluster_metadata = get_cluster_status(client["redshift"])
        open_tcp_port(client["ec2"], cluster_metadata)

    elif args.delete:
        print("Deleting cluster")
        delete_cluster(client["redshift"])
        delete_role(client["iam"])

    if args.status:
        print("Checking status")
        _ = get_cluster_status(client["redshift"])


if __name__ == "__main__":
    args = parse_args()
    main(args)
