import yaml
import os
import requests
import sqlite3
from google.cloud import resourcemanager_v3
from dotenv import load_dotenv


# Create class so that YAML dumper doesn't create references <eyeroll>
class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


# Load configuration
load_dotenv(override=True)
api_key = os.environ["ELASTIC_API_KEY"]
endpoint = os.environ["KIBANA_ENDPOINT"]

# Set global header for Elastic API requests
headers = {"kbn-xsrf": "true",
           "Content-Type": "application/json",
           "Authorization": f"ApiKey {api_key}"}

# Create a global session object for requests to use HTTP keep-alive
s = requests.Session()

# Create a global connection to SQLlite3
connection = sqlite3.connect(database='policy.db', isolation_level=None)
cursor = connection.cursor()


# Create a local SQLLite3 db to store the revision of the master policy.  During each run we will compare
# the revision that is in the master policy to the one stored in the DB. If newer it updates the integrations
# for each GCP project with the one defined in the master policy
def is_master_policy_updated(current_revision):
    cursor.execute('CREATE TABLE IF NOT EXISTS policy(revision INTEGER)')
    rows = cursor.execute('SELECT revision FROM policy').fetchall()
    if not rows:
        cursor.execute('INSERT INTO policy VALUES(:revision)', (current_revision,))
        return False
    elif rows[0][0] == current_revision:
        return False
    else:
        return True


# Retrieve an active list of GCP projects from the configured organization
def get_active_gcp_projects(quota_project_id: str):
    resource_manager_client = resourcemanager_v3.ProjectsClient(
        client_options={
            "quota_project_id": quota_project_id
        })
    projects = resource_manager_client.search_projects(query='lifecycleState: ACTIVE')
    return projects


# Get list of Elastic Agents by a query string
def get_fleet_agents_by_query(query: str):
    url = f"{endpoint}/api/fleet/agents"
    r = s.get(url=url, headers=headers, params={"kuery": query})
    if r.status_code == 200:
        return r.json()


# Get Elastic Agent Policy
def get_agent_policy(policy_id: str):
    url = f"{endpoint}/api/fleet/agent_policies/{policy_id}"
    r = s.get(url=url, headers=headers)
    if r.status_code == 200:
        return r.json()


# Get Agent Policy by query
def get_policy_by_query(query: dict):
    url = f"{endpoint}/api/fleet/agent_policies"
    r = s.get(url=url, headers=headers, params=query)
    if r.status_code == 200:
        return r.json()


# Removes keys that are not needed when creating or updated integration policy
def prep_integration_policy(integration_policy: dict):
    # define the keys to remove
    keys = ["id", "version", "revision", "created_at", "created_by", "updated_at", "updated_by"]
    # Remove keys from the master integration policy
    for key in keys:
        integration_policy.pop(key, None)
    return integration_policy


# Creates an integration policy for a given GCP project and agent policy.
def create_integration_policy(gcp_project_id: str, agent_policy_id: str, integration_policy: dict):
    integration_policy['policy_id'] = agent_policy_id
    integration_policy['name'] = gcp_project_id
    integration_policy['vars']['project_id']['value'] = gcp_project_id
    integration_policy = prep_integration_policy(integration_policy)
    url = f'{endpoint}/api/fleet/package_policies'
    r = s.post(url=url, headers=headers, json=integration_policy)
    check_http_status_code(r.status_code, 'Create integration policy')


# This function deploys an integration to an agent_policy.  The integration definition comes from
# the integration deployed to the master agent policy defined in the .env file.
# There should be ONLY ONE master integration defined in the master agent policy
def deploy_integration(agent_policy_id: str, gcp_project_id: str):
    # Grab the master agent policy
    master_agent_policy = get_policy_by_query(query={"kuery": f'name:"{os.environ["MASTER_AGENT_POLICY_NAME"]}"',
                                                     "full": "true"})
    # Should be only 1 GCP integration deployed
    master_integration_policy = master_agent_policy['items'][0]['package_policies'][0]
    create_integration_policy(gcp_project_id=gcp_project_id,
                              agent_policy_id=agent_policy_id,
                              integration_policy=master_integration_policy)


def delete_integration_policy(package_policy_id: str):
    url = f"{endpoint}/api/fleet/package_policies/{package_policy_id}"
    r = s.delete(url=url, headers=headers)
    check_http_status_code(r.status_code, 'Delete integration policy')


def update_integration_policy(gcp_project_id: str, agent_policy_id: str, package_policy_id: str):
    # Grab the master agent policy
    master_agent_policy = get_policy_by_query(query={"kuery": f'name:"{os.environ["MASTER_AGENT_POLICY_NAME"]}"',
                                                     "full": "true"})
    # Should be only 1 GCP integration deployed
    master_integration_policy = master_agent_policy['items'][0]['package_policies'][0]
    master_integration_policy['policy_id'] = agent_policy_id
    master_integration_policy['name'] = gcp_project_id
    master_integration_policy['vars']['project_id']['value'] = gcp_project_id
    integration_policy = prep_integration_policy(integration_policy=master_integration_policy)
    url = f"{endpoint}/api/fleet/package_policies/{package_policy_id}"
    r = s.put(url=url, headers=headers, json=integration_policy)
    check_http_status_code(r.status_code, 'Updated integration policy')


def check_http_status_code(status_code: int, message: str):
    if status_code == 200:
        print(f'{message} succeeded\n')
    else:
        print(f'{message} failed\n')


def get_master_policy_revision():
    # Grab the master agent policy
    master_agent_policy = get_policy_by_query(query={"kuery": f'name:"{os.environ["MASTER_AGENT_POLICY_NAME"]}"',
                                                     "full": "true"})
    # grab the revision
    return master_agent_policy["items"][0]['revision']


# Returns a list of items that are in the primary list, but not the secondary list
def get_list_diffs(primary_list: list, secondary_list: list):
    return [x for x in primary_list if x not in secondary_list]


def is_key_in_list_dicts(target_list: list, key: str):
    return next((d for i, d in enumerate(target_list) if key in d), None)


def is_kv_in_list_dicts(target_list: list, key: str, value: str):
    return next((d for i, d in enumerate(target_list) if d[key] == value), None)


def main():
    # Grab all the GCP projects which are active.  Make call API call under the
    # provided projects quota
    projects = get_active_gcp_projects(quota_project_id=os.environ["GCP_QUOTA_PROJECT"])
    # Build a list of active GCP project IDs
    active_gcp_projects = []
    for project in projects:
        active_gcp_projects.append(project.project_id)

    # dict to build the policy hierarchy including what agents it is deployed to and what
    # integrations it contains.
    policy_hierarchy = {'agent_policy': {'name': '', 'revision': '', 'agents': [], 'integrations': []}}

    # Get a list of agents that are listening to GCP telemetry
    agents = get_fleet_agents_by_query(query=f'tags:"{os.environ["GCP_AGENT_TAG"]}"')

    # Add agent info to policy hierarchy so the end user knows where the policy is deployed
    for agent in agents['list']:
        policy_hierarchy['agent_policy']['agents'].append({'hostname': agent['local_metadata']['host']['hostname'],
                                                           'version': agent['agent']['version']})
    # Grab the first agent and get the policy
    agent = agents['list'][0]
    agent_policy = agent['policy_id']
    policy = get_agent_policy(policy_id=agent_policy)

    # Start to build the hierarchy of integrations for the agent policy, so they can be displayed/logged
    policy_hierarchy['agent_policy']['revision'] = policy['item']['revision']
    policy_hierarchy['agent_policy']['name'] = policy['item']['name']

    agent_gcp_projects = {}
    # Loop through the policy and get all the integrations (inputs)
    for pp in policy['item']['package_policies']:
        # Check to see if this integration is a GCP integration
        if 'gcp' == pp['package']['name']:
            integration_name = pp['name']
            gcp_project_name = pp['vars']['project_id']['value']
            package_policy_id = pp['id']
            for inp in pp['inputs']:
                if inp['enabled']:
                    # I have never seen more than 1 stream for GCP, it doesn't appear to be settable in the UI
                    stream = inp['streams'][0]
                    stream_datatype = stream['data_stream']['type']
                    stream_dataset = stream['data_stream']['dataset']
                    # Check to see if the GCP project is already listed in the policy_hierarchy, if so return it
                    # if not return None
                    integration = is_kv_in_list_dicts(target_list=policy_hierarchy['agent_policy']['integrations'],
                                                      key='name',
                                                      value=integration_name)
                    if integration is not None:  # GCP project not found in policy hierarchy
                        datatype = is_key_in_list_dicts(target_list=integration['datatype'], key=stream_datatype)
                        if datatype is None:
                            integration['datatype'].append({stream_datatype: {'dataset': [stream_dataset]}})
                        else:
                            datatype[stream_datatype]['dataset'].append(stream_dataset)
                    else:
                        integration = {'name': integration_name,
                                       'id': package_policy_id,
                                       'gcp_project': gcp_project_name,
                                       'datatype': [{stream_datatype: {'dataset': [stream_dataset]}}]}
                        policy_hierarchy['agent_policy']['integrations'].append(integration)

                    # Add package_policy_id to dict in case we need to delete the integration later
                    if gcp_project_name not in agent_gcp_projects.keys():
                        agent_gcp_projects[gcp_project_name] = package_policy_id
    print(f'Agent GCP Projects: {agent_gcp_projects}')

    print(f"Found the following agent policy:\n"
          f"{yaml.dump(policy_hierarchy, allow_unicode=True, default_flow_style=False, sort_keys=False, Dumper=NoAliasDumper)}\n")

    # Find GCP projects that don't have an Elastic Integration. Used * to convert to a list
    new_gcp_projects = get_list_diffs(primary_list=active_gcp_projects,
                                      secondary_list=[*agent_gcp_projects])
    print(f"New GCP Projects found that need integrations added to the agent policy: {new_gcp_projects}\n")
    # Loop through *new* GCP projects and for each one deploy an integration to the
    # agent policy listening to GCP telemetry
    for project in new_gcp_projects:
        print(f"Creating integrations for GCP Project: {project}\n")
        deploy_integration(agent_policy_id=agent_policy, gcp_project_id=project)

    # Find projects configured in fleet that point to GCP projects that don't exist anymore
    deleted_gcp_projects = get_list_diffs(primary_list=[*agent_gcp_projects],
                                          secondary_list=active_gcp_projects)
    print(f'Old GCP Projects found that need integrations deleted in the agent policy: {deleted_gcp_projects}\n')
    # Loop through *deleted* GCP projects and for each one delete the integration on each
    # agent listening to GCP telemetry
    for project in deleted_gcp_projects:
        print(f'Deleting all integrations for GCP Project: {project}\n')
        delete_integration_policy(package_policy_id=agent_gcp_projects[project])

    # If the master integration has been modified we need to update the existing
    master_policy_revision = get_master_policy_revision()
    master_agent_policy_updated = is_master_policy_updated(master_policy_revision)
    if master_agent_policy_updated:
        # Find remaining integrations because we have to update them with a new master integration version.
        remaining_gcp_projects = get_list_diffs(primary_list=[*agent_gcp_projects],
                                                secondary_list=deleted_gcp_projects)
        print(f'Master integration policy updated, syncing the following GCP projects:\n{remaining_gcp_projects}\n')
        for project in remaining_gcp_projects:
            update_integration_policy(gcp_project_id=project,
                                      agent_policy_id=agent_policy,
                                      package_policy_id=agent_gcp_projects[project])

        cursor.execute('UPDATE policy SET revision = ?', (master_policy_revision,))
        cursor.close()
        connection.close()
    else:
        print('Master integration policy was not changed')


if __name__ == "__main__":
    main()
