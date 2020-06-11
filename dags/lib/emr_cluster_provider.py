import boto3, json, pprint, requests, textwrap, time, logging, requests
import configparser
from datetime import datetime


class EMRClusterProvider:

    def __init__(self, aws_key, aws_secret, region, key_pair,num_nodes):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.key_pair = key_pair
        self.num_nodes = num_nodes
        self.emr = None
        self.ec2 = None

    def create_client(self):
        self.emr = boto3.client('emr', region_name=self.region, aws_access_key_id=self.aws_key,
                                aws_secret_access_key=self.aws_secret)
        if not self.emr:
            raise ValueError('Not able to initialize Boto3 EMR client')
        else:
            return self.emr

    def get_security_group_id(self, group_name):
        self.ec2 = boto3.client('ec2', region_name=self.region, aws_access_key_id=self.aws_key,
                                aws_secret_access_key=self.aws_secret)
        if not self.ec2:
            raise ValueError('Not able to initialize Boto3 EC2 client')
        else:
            response = self.ec2.describe_security_groups(GroupNames=[group_name])
            return response['SecurityGroups'][0]['GroupId']

    def create_cluster(self, cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.9.0',
                       master_instance_type='m3.xlarge', core_node_instance_type='m3.xlarge'):
        emr_master_security_group_id = self.get_security_group_id('AirflowEMRMasterSG')
        emr_slave_security_group_id = self.get_security_group_id('AirflowEMRSlaveSG')
        cluster_response = self.emr.run_job_flow(
            Name=cluster_name,
            ReleaseLabel=release_label,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance_type,
                        'InstanceCount': 1
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': core_node_instance_type,
                        'InstanceCount': self.num_nodes
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'Ec2KeyName': self.key_pair,
                'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
                'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole', # 'EmrEc2InstanceProfile'
            ServiceRole='EMR_DefaultRole', # 'EmrRole'
            Applications=[
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'livy'},
                {'Name': 'zeppelin'}
            ]
        )
        return cluster_response['JobFlowId']

    def get_cluster_dns(self, cluster_id):
        response = self.emr.describe_cluster(ClusterId=cluster_id)
        return response['Cluster']['MasterPublicDnsName']

    def get_cluster_status(self, cluster_id):
        response = self.emr.describe_cluster(ClusterId=cluster_id)
        return response['Cluster']['Status']['State']

    def get_public_ip(self, cluster_id):
        instances = self.emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
        return instances['Instances'][0]['PublicIpAddress']

    def terminate_cluster(self, cluster_id):
        self.emr.terminate_job_flows(JobFlowIds=[cluster_id])

    def wait_for_cluster_creation(self, cluster_id):
        self.emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)

