import argparse
import boto3
import logging
import sys
import time


# dev daily working ec2 instances ids
EC2_INSTANCE_LIBRARY_IDS = []
EC2_INSTANCE_SERVICE_IDS = []
EC2_INSTANCE_API_IDS = []


def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    parser = argparse.ArgumentParser(description='Trun on/off aws service instances.')
    parser.add_argument('-p', '--profile', dest='profile', type=str, required=True)
    parser.add_argument('-t', '--type', dest='type', type=str, required=True)
    parser.add_argument('-s', '--swtich', dest='swtich', type=str, required=True)
    return parser


def get_session(profile):
    return boto3.session.Session(profile_name=profile)


def get_aws_client(session, type):
    return session.client(type)


def desc_instance(client):
    status = client.describe_instances()
    logging.info('status: %s', status)


def switch_ec2_status(client, action):
    if action == 'on':
        logging.info('trun on ec2 library instances: %s', EC2_INSTANCE_LIBRARY_IDS)
        client.start_instances(InstanceIds=EC2_INSTANCE_LIBRARY_IDS)
        time.sleep(300)

        logging.info('trun on ec2 service instances: %s', EC2_INSTANCE_SERVICE_IDS)
        client.start_instances(InstanceIds=EC2_INSTANCE_SERVICE_IDS)
        time.sleep(300)
        
        logging.info('trun on ec2 api instances: %s', EC2_INSTANCE_API_IDS)
        client.start_instances(InstanceIds=EC2_INSTANCE_API_IDS)
        time.sleep(300)

    if action == 'off':
        logging.info('trun off ec2 api instances: %s', EC2_INSTANCE_API_IDS)
        client.stop_instances(InstanceIds=EC2_INSTANCE_API_IDS)
        time.sleep(300)

        logging.info('trun off ec2 service instances: %s', EC2_INSTANCE_SERVICE_IDS)
        client.stop_instances(InstanceIds=EC2_INSTANCE_SERVICE_IDS)
        time.sleep(300)

        logging.info('trun off ec2 library instances: %s', EC2_INSTANCE_LIBRARY_IDS)
        client.stop_instances(InstanceIds=EC2_INSTANCE_LIBRARY_IDS)
        time.sleep(300)


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.info('process start!!!!')

    parser = make_parser()
    arguments = parser.parse_args()
    logging.info('arguments: %s' % arguments)

    try:
        session = get_session(arguments.profile)
        client = get_aws_client(session, arguments.type)

        if 'ec2' == arguments.type:
            switch_ec2_status(client, arguments.swtich)
    finally:
        logging.info('process done!!!!')


if __name__ == "__main__":
    main()
