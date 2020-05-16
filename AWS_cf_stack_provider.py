from __future__ import division, print_function, unicode_literals
import configparser
import boto3
from botocore.exceptions import ClientError


class TemplateError(Exception):
    pass


class AWSCloudFormationStackProvider:

    def __init__(self, aws_key, aws_secret, key_pair, region, template_url, final_bucket):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.key_pair = key_pair
        self.region = region
        self.template_url = template_url
        self.final_bucket = final_bucket
        self.s3_client = None
        self.cf = None

    def get_s3_client(self):
        self.s3_client = boto3.client('s3', region_name=self.region,
                                      aws_access_key_id=self.aws_key, aws_secret_access_key=self.aws_secret)
        if not self.s3_client:
            raise ValueError('Not able to initialize Boto3 S3 client')
        else:
            return self.s3_client

    def get_cloud_formation_client(self):
        self.cf = boto3.client('cloudformation', region_name=self.region,
                               aws_access_key_id=self.aws_key, aws_secret_access_key=self.aws_secret)
        if not self.cf:
            raise ValueError('Not able to initialize Boto3 cloud-formation client with configuration.')
        else:
            return self.cf

    def get_stack_template(self):
        validated = self.cf.validate_template(TemplateURL=self.template_url)
        if not validated:
            raise TemplateError('Not able to use the Template provided for setting up the cloud formation stack')
        else:
            return validated

    def create_stack(self, stack_name):
        params = {
            'StackName': stack_name,
            'TemplateURL': self.template_url,
            'Parameters':[{
                'ParameterKey':'KeyName',
                'ParameterValue':self.key_pair},
                {
                    'ParameterKey':'S3BucketName',
                    'ParameterValue': self.final_bucket
                }],
            'Capabilities':['CAPABILITY_NAMED_IAM']
        }
        try:
            if self.stack_exists(stack_name):
                print('Updating {}'.format(stack_name))
                stack_result = self.cf.update_stack(**params)
                waiter = self.cf.get_waiter('stack_update_complete')
            else:
                print('Creating {}'.format(stack_name))
                stack_result = self.cf.create_stack(**params)
                waiter = self.cf.get_waiter('stack_create_complete')
                print("...waiting for stack to be ready...")
                waiter.wait(StackName=stack_name)
                print(r"Stack {} is created in AWS successfully".format(stack_name))
        except ClientError as ex:
            error_message = ex.response['Error']['Message']
            if error_message == 'No updates are to be performed.':
                print("No changes")
            else:
                raise

    def stack_exists(self,stack_name):
        stacks = self.cf.list_stacks()['StackSummaries']
        for stack in stacks:
            if stack['StackStatus'] == 'DELETE_COMPLETE':
                continue
            if stack_name == stack['StackName']:
                return True
        return False


'''
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('./dwh.cfg')

    AWS_ACCESS_KEY = config.get('AWS', 'AWS_KEY_ID')
    AWS_SECRET = config.get('AWS', 'AWS_SECRET')
    AWS_REGION = config.get('AWS', 'REGION')
    AWS_EC2_KEY_PAIR = config.get('AWS','AWS_EC2_KEY_PAIR')
    TEMPLATE_URL = config.get('S3', 'CF_TEMPLATE_URL')
    FINAL_DATA_BUCKET = config.get('S3', 'FINAL_DATA_BUCKET')
    aws_stack_provider = AWSCloudFormationStackProvider(aws_key=AWS_ACCESS_KEY, aws_secret=AWS_SECRET,
                                                        key_pair=AWS_EC2_KEY_PAIR,region=AWS_REGION,
                                                        template_url=TEMPLATE_URL, final_bucket=FINAL_DATA_BUCKET)
    cf_client = aws_stack_provider.get_cloud_formation_client()
    aws_s3_client = aws_stack_provider.get_s3_client()
    valid_stack_template = aws_stack_provider.get_stack_template()
    if valid_stack_template:
        aws_stack_provider.create_stack(stack_name='Test-STACK')

'''
