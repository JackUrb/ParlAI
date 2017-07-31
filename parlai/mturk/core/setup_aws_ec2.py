# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.
import os
import sys
import shutil
from subprocess import call
import zipfile
import boto3
import botocore
import time
import json
import webbrowser
import hashlib
import getpass
import paramiko
from botocore.exceptions import ClientError
from botocore.exceptions import ProfileNotFound
from data_model import setup_database_engine, init_database, check_database_health

aws_profile_name = 'parlai_mturk'
region_name = 'us-east-1'
user_name = getpass.getuser()

api_gateway_name = 'ParlaiRelayServer_' + user_name
endpoint_api_name_html = 'html'  # For GET-ing HTML
endpoint_api_name_json = 'json'  # For GET-ing and POST-ing JSON

rds_db_instance_identifier = 'parlai-mturk-db-' + user_name
rds_db_name = 'parlai_mturk_db_' + user_name
rds_username = 'parlai_user'
rds_password = 'parlai_user_password'
rds_security_group_name = 'parlai-mturk-db-security-group'
rds_security_group_description = 'Security group for ParlAI MTurk DB'
rds_db_instance_class = 'db.t2.medium'

parent_dir = os.path.dirname(os.path.abspath(__file__))
generic_files_to_copy = [
    os.path.join(parent_dir, 'hit_config.json'),
    os.path.join(parent_dir, 'data_model.py'),
    os.path.join(parent_dir, 'html', 'core.html'), 
    os.path.join(parent_dir, 'html', 'cover_page.html'), 
    os.path.join(parent_dir, 'html', 'mturk_index.html')
]
ec2_server_zip_file_name = 'ec2_server.zip'

mturk_hit_frame_height = 650

def setup_aws_credentials():
    try:
        session = boto3.Session(profile_name=aws_profile_name)
    except ProfileNotFound as e:
        print('''AWS credentials not found. Please create an IAM user with programmatic access and AdministratorAccess policy at https://console.aws.amazon.com/iam/ (On the "Set permissions" page, choose "Attach existing policies directly" and then select "AdministratorAccess" policy). \nAfter creating the IAM user, please enter the user's Access Key ID and Secret Access Key below:''')
        aws_access_key_id = input('Access Key ID: ')
        aws_secret_access_key = input('Secret Access Key: ')
        if not os.path.exists(os.path.expanduser('~/.aws/')):
            os.makedirs(os.path.expanduser('~/.aws/'))
        aws_credentials_file_path = '~/.aws/credentials'
        aws_credentials_file_string = None
        if os.path.exists(os.path.expanduser(aws_credentials_file_path)):
            with open(os.path.expanduser(aws_credentials_file_path), 'r') as aws_credentials_file:
                aws_credentials_file_string = aws_credentials_file.read()
        with open(os.path.expanduser(aws_credentials_file_path), 'a+') as aws_credentials_file:
            if aws_credentials_file_string:
                if aws_credentials_file_string.endswith("\n\n"):
                    pass
                elif aws_credentials_file_string.endswith("\n"):
                    aws_credentials_file.write("\n")
                else:
                    aws_credentials_file.write("\n\n")
            aws_credentials_file.write("["+aws_profile_name+"]\n")
            aws_credentials_file.write("aws_access_key_id="+aws_access_key_id+"\n")
            aws_credentials_file.write("aws_secret_access_key="+aws_secret_access_key+"\n")
        print("AWS credentials successfully saved in "+aws_credentials_file_path+" file.\n")
    os.environ["AWS_PROFILE"] = aws_profile_name

def get_ec2_details():
    hostname = input('EC2 server hostname: ')
    key_path = input('Path to .pem key: ')
    return hostname, key_path

def setup_rds():
    # Set up security group rules first
    ec2 = boto3.client('ec2', region_name=region_name)

    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')
    security_group_id = None

    try:
        response = ec2.create_security_group(GroupName=rds_security_group_name,
                                             Description=rds_security_group_description,
                                             VpcId=vpc_id)
        security_group_id = response['GroupId']
        print('RDS: Security group created.')

        data = ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                 'IpProtocol': 'tcp',
                 'FromPort': 5432,
                 'ToPort': 5432,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                 'Ipv6Ranges': [{'CidrIpv6': '::/0'}]
                },
            ])
        print('RDS: Security group ingress IP permissions set.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            print('RDS: Security group already exists.')
            response = ec2.describe_security_groups(GroupNames=[rds_security_group_name])
            security_group_id = response['SecurityGroups'][0]['GroupId']

    rds_instance_is_ready = False
    while not rds_instance_is_ready:
        rds = boto3.client('rds', region_name=region_name)
        try:
            rds.create_db_instance(DBInstanceIdentifier=rds_db_instance_identifier,
                                   AllocatedStorage=20,
                                   DBName=rds_db_name,
                                   Engine='postgres',
                                   # General purpose SSD
                                   StorageType='gp2',
                                   StorageEncrypted=False,
                                   AutoMinorVersionUpgrade=True,
                                   MultiAZ=False,
                                   MasterUsername=rds_username,
                                   MasterUserPassword=rds_password,
                                   VpcSecurityGroupIds=[security_group_id],
                                   DBInstanceClass=rds_db_instance_class,
                                   Tags=[{'Key': 'Name', 'Value': rds_db_instance_identifier}])
            print('RDS: Starting RDS instance...')
        except ClientError as e:
            if e.response['Error']['Code'] == 'DBInstanceAlreadyExists':
                print('RDS: DB instance already exists.')
            else:
                raise

        response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
        db_instances = response['DBInstances']
        db_instance = db_instances[0]

        if db_instance['DBInstanceClass'] != rds_db_instance_class: # If instance class doesn't match
            print('RDS: Instance class does not match.')
            remove_rds_database()
            rds_instance_is_ready = False
            continue

        status = db_instance['DBInstanceStatus']

        if status == 'deleting':
            print("RDS: Waiting for previous delete operation to complete. This might take a couple minutes...")
            try:
                while status == 'deleting':
                    time.sleep(5)
                    response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
                    db_instances = response['DBInstances']
                    db_instance = db_instances[0]
                    status = db_instance['DBInstanceStatus']
            except ClientError as e:
                rds_instance_is_ready = False
                continue

        if status == 'creating':
            print("RDS: Waiting for newly created database to be available. This might take a couple minutes...")
            while status == 'creating':
                time.sleep(5)
                response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
                db_instances = response['DBInstances']
                db_instance = db_instances[0]
                status = db_instance['DBInstanceStatus']

        endpoint = db_instance['Endpoint']
        host = endpoint['Address']

        setup_database_engine(host, rds_db_name, rds_username, rds_password)
        database_health_status = check_database_health()
        if database_health_status in ['missing_table', 'healthy']:
            print("Remote database health status: "+database_health_status)
            init_database()
        elif database_health_status in ['inconsistent_schema', 'unknown_error']:
            print("Remote database error: "+database_health_status+". Removing RDS database...")
            remove_rds_database()
            rds_instance_is_ready = False
            continue

        print('RDS: DB instance ready.')
        rds_instance_is_ready = True

    return host

def remove_rds_database():
    # Remove RDS database
    rds = boto3.client('rds', region_name=region_name)
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
        db_instances = response['DBInstances']
        db_instance = db_instances[0]
        status = db_instance['DBInstanceStatus']

        if status == 'deleting':
            print("RDS: Waiting for previous delete operation to complete. This might take a couple minutes...")
        else:
            response = rds.delete_db_instance(
                DBInstanceIdentifier=rds_db_instance_identifier,
                SkipFinalSnapshot=True,
            )
            response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
            db_instances = response['DBInstances']
            db_instance = db_instances[0]
            status = db_instance['DBInstanceStatus']

            if status == 'deleting':
                print("RDS: Deleting database. This might take a couple minutes...")

        try:
            while status == 'deleting':
                time.sleep(5)
                response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
                db_instances = response['DBInstances']
                db_instance = db_instances[0]
                status = db_instance['DBInstanceStatus']
        except ClientError as e:
            print("RDS: Database deleted.")

    except ClientError as e:
        print("RDS: Database doesn't exist.")


def create_hit_config(task_description, num_hits, num_assignments, is_sandbox):
    mturk_submit_url = 'https://workersandbox.mturk.com/mturk/externalSubmit'
    if not is_sandbox:
        mturk_submit_url = 'https://www.mturk.com/mturk/externalSubmit'
    hit_config = {
        'task_description': task_description, 
        'num_hits': num_hits, 
        'num_assignments': num_assignments, 
        'is_sandbox': is_sandbox,
        'mturk_submit_url': mturk_submit_url,
    }
    hit_config_file_path = os.path.join(parent_dir, 'hit_config.json')
    if os.path.exists(hit_config_file_path):
        os.remove(hit_config_file_path)
    with open(hit_config_file_path, 'w') as hit_config_file:
        hit_config_file.write(json.dumps(hit_config))

def setup_ec2_server_api(rds_host, task_files_to_copy, ec2_host_name, key_path, should_clean_up_after_upload=True):
    # Dynamically generate handler.py file, and then create zip file
    print("Preparing AWS EC2 instance")

    # Copying files
    config_string = "frame_height = " + str(mturk_hit_frame_height) + "\n" + \
        "rds_host = \'" + rds_host + "\'\n" + \
        "rds_db_name = \'" + rds_db_name + "\'\n" + \
        "rds_username = \'" + rds_username + "\'\n" + \
        "rds_password = \'" + rds_password + "\'\n"
    with open(os.path.join(parent_dir, 'rds_vals.txt'), 'w') as config_file:
        config_file.write(config_string)

    mykey = paramiko.RSAKey.from_private_key_file(key_path)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ec2_host_name, username='ubuntu', pkey = mykey)
    
    sftp = ssh.open_sftp()
    files_to_copy = task_files_to_copy + generic_files_to_copy
    files_to_copy = files_to_copy + ['rds_vals.txt']
    dest_path = '/var/www/demoapp/'
    print ("Moving Files to Sever")
    for file_src in files_to_copy:
        if 'html' in file_src:
            filename = file_src.split('/')[-1]
            file_dest = dest_path + 'html/' + filename
        else:
            file_dest = dest_path + (file_src.split('/')[-1])
        print(sftp.put(file_src, file_dest))
            
    ssh.exec_command('cd /var/www/demoapp/')
    ssh.exec_command('pwd')
    ssh.exec_command('. venv/bin/activate')
    ssh.exec_command('sudo systemctl restart uwsgi')
    sftp.close()
    ssh.close()
    
    # Clean up if needed
    if should_clean_up_after_upload:
        os.remove(os.path.join(parent_dir, 'rds_vals.txt'))
        os.remove(os.path.join(parent_dir, 'hit_config.json'))

    html_api_endpoint_url = 'https://' + ec2_host_name + '/html'
    json_api_endpoint_url = 'https://' + ec2_host_name + '/json'

    return html_api_endpoint_url, json_api_endpoint_url

def calculate_mturk_cost(payment_opt):
    """MTurk Pricing: https://requester.mturk.com/pricing
    20% fee on the reward and bonus amount (if any) you pay Workers.
    HITs with 10 or more assignments will be charged an additional 20% fee on the reward you pay Workers.

    Example payment_opt format for paying reward:
    {
        'type': 'reward',
        'num_hits': 1,
        'num_assignments': 1,
        'reward': 0.05  # in dollars
    }

    Example payment_opt format for paying bonus:
    {
        'type': 'bonus',
        'amount': 1000  # in dollars
    }
    """
    total_cost = 0
    if payment_opt['type'] == 'reward':
        total_cost = payment_opt['num_hits'] * payment_opt['num_assignments'] * payment_opt['reward'] * 1.2
        if payment_opt['num_assignments'] >= 10:
            total_cost = total_cost * 1.2
    elif payment_opt['type'] == 'bonus':
        total_cost = payment_opt['amount'] * 1.2
    return total_cost

def check_mturk_balance(balance_needed, is_sandbox):
    client = boto3.client(
        service_name = 'mturk',
        region_name = 'us-east-1',
        endpoint_url = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    )

    # Region is always us-east-1
    if not is_sandbox:
        client = boto3.client(service_name = 'mturk', region_name='us-east-1')

    # Test that you can connect to the API by checking your account balance
    # In Sandbox this always returns $10,000
    try:
        user_balance = float(client.get_account_balance()['AvailableBalance'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'RequestError':
            print('ERROR: To use the MTurk API, you will need an Amazon Web Services (AWS) Account. Your AWS account must be linked to your Amazon Mechanical Turk Account. Visit https://requestersandbox.mturk.com/developer to get started. (Note: if you have recently linked your account, please wait for a couple minutes before trying again.)\n')
            quit()
        else:
            raise

    balance_needed = balance_needed * 1.2 # AWS charges 20% fee for both reward and bonus payment

    if user_balance < balance_needed:
        print("You might not have enough money in your MTurk account. Please go to https://requester.mturk.com/account and increase your balance to at least " + balance_needed +", and then try again.")
        return False
    else:
        return True

def get_mturk_client(is_sandbox):
    client = boto3.client(
        service_name = 'mturk',
        region_name = 'us-east-1',
        endpoint_url = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    )
    # Region is always us-east-1
    if not is_sandbox:
        client = boto3.client(service_name = 'mturk', region_name='us-east-1')
    return client

def create_hit_type(hit_title, hit_description, hit_keywords, hit_reward, assignment_duration_in_seconds, is_sandbox):
    client = boto3.client(
        service_name = 'mturk',
        region_name = 'us-east-1',
        endpoint_url = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    )

    # Region is always us-east-1
    if not is_sandbox:
        client = boto3.client(service_name = 'mturk', region_name='us-east-1')

    # Create a qualification with Locale In('US', 'CA') requirement attached
    localRequirements = [{
        'QualificationTypeId': '00000000000000000071',
        'Comparator': 'In',
        'LocaleValues': [
            {'Country': 'US'},
            {'Country': 'CA'},
            {'Country': 'GB'},
            {'Country': 'AU'},
            {'Country': 'NZ'}
        ],
        'RequiredToPreview': True
    }]

    # Create the HIT type
    response = client.create_hit_type(
        AutoApprovalDelayInSeconds=4*7*24*3600, # auto-approve after 4 weeks
        AssignmentDurationInSeconds=assignment_duration_in_seconds,
        Reward=str(hit_reward),
        Title=hit_title,
        Keywords=hit_keywords,
        Description=hit_description,
        QualificationRequirements=localRequirements
    )
    hit_type_id = response['HITTypeId']
    return hit_type_id

def create_hit_with_hit_type(page_url, hit_type_id, num_assignments, is_sandbox):
    page_url = page_url.replace('&', '&amp;')

    question_data_struture = '''<ExternalQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd">
      <ExternalURL>'''+page_url+'''</ExternalURL>
      <FrameHeight>'''+str(mturk_hit_frame_height)+'''</FrameHeight>
    </ExternalQuestion>
    '''

    client = boto3.client(
        service_name = 'mturk',
        region_name = 'us-east-1',
        endpoint_url = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    )

    # Region is always us-east-1
    if not is_sandbox:
        client = boto3.client(service_name = 'mturk', region_name='us-east-1')

    # Create the HIT
    response = client.create_hit_with_hit_type(
        HITTypeId=hit_type_id,
        MaxAssignments=num_assignments,
        LifetimeInSeconds=31536000,
        Question=question_data_struture,
        # AssignmentReviewPolicy={
        #     'PolicyName': 'string',
        #     'Parameters': [
        #         {
        #             'Key': 'string',
        #             'Values': [
        #                 'string',
        #             ],
        #             'MapEntries': [
        #                 {
        #                     'Key': 'string',
        #                     'Values': [
        #                         'string',
        #                     ]
        #                 },
        #             ]
        #         },
        #     ]
        # },
        # HITReviewPolicy={
        #     'PolicyName': 'string',
        #     'Parameters': [
        #         {
        #             'Key': 'string',
        #             'Values': [
        #                 'string',
        #             ],
        #             'MapEntries': [
        #                 {
        #                     'Key': 'string',
        #                     'Values': [
        #                         'string',
        #                     ]
        #                 },
        #             ]
        #         },
        #     ]
        # },
    )

    # The response included several fields that will be helpful later
    hit_type_id = response['HIT']['HITTypeId']
    hit_id = response['HIT']['HITId']
    hit_link = "https://workersandbox.mturk.com/mturk/preview?groupId=" + hit_type_id
    if not is_sandbox:
        hit_link = "https://www.mturk.com/mturk/preview?groupId=" + hit_type_id
    return hit_link

def setup_aws(task_files_to_copy):
    rds_host = setup_rds()
    ec2_host_name, key_path = get_ec2_details()
    html_api_endpoint_url, json_api_endpoint_url = setup_ec2_server_api(rds_host, task_files_to_copy, ec2_host_name, key_path)

    return html_api_endpoint_url, json_api_endpoint_url

def clean_aws():
    # Remove RDS database
    try:
        rds = boto3.client('rds', region_name=region_name)
        response = rds.delete_db_instance(
            DBInstanceIdentifier=rds_db_instance_identifier,
            SkipFinalSnapshot=True,
        )
        response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
        db_instances = response['DBInstances']
        db_instance = db_instances[0]
        status = db_instance['DBInstanceStatus']

        if status == 'deleting':
            print("RDS: Deleting database. This might take a couple minutes...")

        try:
            while status == 'deleting':
                time.sleep(5)
                response = rds.describe_db_instances(DBInstanceIdentifier=rds_db_instance_identifier)
                db_instances = response['DBInstances']
                db_instance = db_instances[0]
                status = db_instance['DBInstanceStatus']
        except ClientError as e:
            print("RDS: Database deleted.")

    except ClientError as e:
        print("RDS: Database doesn't exist.")

    # Remove RDS security group
    try:
        ec2 = boto3.client('ec2', region_name=region_name)

        response = ec2.describe_security_groups(GroupNames=[rds_security_group_name])
        security_group_id = response['SecurityGroups'][0]['GroupId']

        response = ec2.delete_security_group(
            DryRun=False,
            GroupName=rds_security_group_name,
            GroupId=security_group_id
        )
        print("RDS: Security group removed.")
    except ClientError as e:
        print("RDS: Security group doesn't exist.")

    # Remove IAM role
    try:
        iam_client = boto3.client('iam')

        try:
            response = iam_client.detach_role_policy(
                RoleName=iam_role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonRDSFullAccess'
            )
        except ClientError as e:
            pass

        try:
            response = iam_client.detach_role_policy(
                RoleName=iam_role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonMechanicalTurkFullAccess'
            )
        except ClientError as e:
            pass

        response = iam_client.delete_role(
            RoleName=iam_role_name
        )
        time.sleep(10)
        print("IAM: Role removed.")
    except ClientError as e:
        print("IAM: Role doesn't exist.")

if __name__ == "__main__":
    if sys.argv[1] == 'clean':
        setup_aws_credentials()
        clean_aws()
    elif sys.argv[1] == 'remove_rds':
        setup_aws_credentials()
        remove_rds_database()
    elif sys.argv[1] == 'ev':
        get_ec2_details()
