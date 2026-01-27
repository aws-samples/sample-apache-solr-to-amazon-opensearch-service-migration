import os
import boto3
import cfnresponse
import json
import urllib3

http = urllib3.PoolManager()


def get_auth(region):
    secrets_manager = boto3.client('secretsmanager', region_name=region)
    secret_name = os.environ['OS_SECRET_NAME']
    secret = secrets_manager.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(secret['SecretString'])
    auth = (secret_dict['username'], secret_dict['password'])
    return auth


def map_role(domain_endpoint, role_name, iam_arns, region, action='PUT'):
    """
    Maps an IAM role to an OpenSearch role
    """
    path = f'/_plugins/_security/api/rolesmapping/{role_name}'
    url = f'https://{domain_endpoint}{path}'

    auth = get_auth(region)

    headers = {'Content-Type': 'application/json'}
    headers.update(urllib3.make_headers(basic_auth=auth[0] + ':' + auth[1]))

    response = http.request('GET', url=url, headers=headers)
    existing_backend_roles = json.loads((response.data.decode('utf-8')))[role_name]['backend_roles']

    if action == 'PUT':
        for arn in iam_arns.split(","):
            if arn not in existing_backend_roles:
                existing_backend_roles.append(arn)

        payload = [{
            "op": "add", "path": "/backend_roles", "value": existing_backend_roles
        }]
        response = http.request('PATCH', url, body=json.dumps(payload), headers=headers)
    elif action == 'DELETE':
        for arn in iam_arns.split(","):
            if arn in existing_backend_roles:
                existing_backend_roles.remove(arn)
        payload = [{
            "op": "replace", "path": "/backend_roles", "value": existing_backend_roles
        }]
        response = http.request('PATCH', url, body=json.dumps(payload), headers=headers)

    return response.status in [200, 201, 404]


def handler(event, context):
    """
    Lambda function handler for the custom resource
    """
    try:
        print(f"Received event: {json.dumps(event)}")

        # Get properties from the event
        props = event['ResourceProperties']
        domain_endpoint = props['DomainEndpoint']
        role_name = props['RoleName']
        iam_arns = props['IamRoleArns']
        region = props['Region']

        response_data = {}

        if event['RequestType'] in ['Create', 'Update']:
            success = map_role(
                domain_endpoint,
                role_name,
                iam_arns,
                region,
                'PUT'
            )

            if success:
                response_data['Message'] = f'Successfully mapped role {role_name} to {iam_arns}'
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
            else:
                response_data['Message'] = f'Failed to map role {role_name} to {iam_arns}'
                cfnresponse.send(event, context, cfnresponse.FAILED, response_data)

        elif event['RequestType'] == 'Delete':
            success = map_role(
                domain_endpoint,
                role_name,
                iam_arns,
                region,
                'DELETE'
            )

            if success:
                response_data['Message'] = f'Successfully deleted role mapping for {role_name}'
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
            else:
                response_data['Message'] = f'Failed to delete role mapping for {role_name}'
                cfnresponse.send(event, context, cfnresponse.FAILED, response_data)

    except Exception as e:
        print(f"Error: {str(e)}")
        response_data = {'Error': str(e)}
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)