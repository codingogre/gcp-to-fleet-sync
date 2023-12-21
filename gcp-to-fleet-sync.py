from google.cloud import resourcemanager_v3
from dotenv import load_dotenv
import os
import requests

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


# Get detailed Elastic Agent Policy
def get_full_agent_policy(policy_id: str):
    url = f"{endpoint}/api/fleet/agent_policies/{policy_id}/full"
    r = s.get(url=url, headers=headers)
    if r.status_code == 200:
        return r.json()


# Get Agent Policy by query
def get_policy_by_query(query: dict):
    url = f"{endpoint}/api/fleet/agent_policies"
    r = s.get(url=url, headers=headers, params=query)
    if r.status_code == 200:
        return r.json()


def create_integration_policy(gcp_project_id: str, agent_policy_id: str, integration_policy: dict):
    integration_policy["policy_id"] = agent_policy_id
    integration_policy["name"] = gcp_project_id
    integration_policy["vars"]["project_id"]["value"] = gcp_project_id
    # define the keys to remove
    keys = ["id", "version", "revision", "created_at", "created_by", "updated_at", "updated_by"]
    for key in keys:
        integration_policy.pop(key, None)
    url = f"{endpoint}/api/fleet/package_policies"
    r = s.post(url=url, headers=headers, json=integration_policy)
    return r.status_code

# TODO Multi agent same agent policy
# TODO Sync changes over time to master integration

def delete_integration_policy(package_policy_id: str):
    url = f"{endpoint}/api/fleet/package_policies/{package_policy_id}"
    r = s.delete(url=url, headers=headers)
    return r.status_code


# This function deploys an integration to an agent_policy.  The integration definition comes from
# the integration deployed to the master agent policy defined in the .env file.
# There should be ONLY ONE master integration defined in the master agent policy
def deploy_integration(agent_policy_id: str, gcp_project_id: str):
    # Grab the master agent policy
    master_agent_policy = get_policy_by_query(query={"kuery": f'name:"{os.environ["MASTER_AGENT_POLICY_NAME"]}"',
                                                     "full": "true"})
    master_integration_policy = master_agent_policy["items"][0]["package_policies"][0]
    status_code = create_integration_policy(gcp_project_id=gcp_project_id,
                                            agent_policy_id=agent_policy_id,
                                            integration_policy=master_integration_policy)
    if status_code == 200:
        print("Integration Created\n")
    else:
        print("Deploy Integration Failed\n")


# Returns a list of items that are in the primary list, but not the secondary list
def get_list_diffs(primary_list: list, secondary_list: list):
    return [x for x in primary_list if x not in secondary_list]


def main():
    # Grab all the GCP projects which are active.  Make call API call under the
    # provided projects quota
    projects = get_active_gcp_projects(quota_project_id=os.environ["GCP_QUOTA_PROJECT"])
    active_gcp_projects = []
    for project in projects:
        active_gcp_projects.append(project.project_id)
    print(f"GCP Cloud configured project IDs: {active_gcp_projects}\n")

    # Get a list of agents that are listening to GCP telemetry
    agents = get_fleet_agents_by_query(query=f'tags:"{os.environ["GCP_AGENT_TAG"]}"')

    # Loop through agents and get the agent policy for each agent
    agent_gcp_projects = []
    gcp_project_to_package_policy_map = {}
    for agent in agents['list']:
        policy = get_full_agent_policy(policy_id=agent['policy_id'])
        # Loop through the policy and get all the integrations (inputs)
        for gcp_input in policy['item']['inputs']:
            # Loop through all the data streams to grab GCP project_ids
            for stream in gcp_input['streams']:
                if 'project_id' in stream:
                    agent_gcp_projects.append(stream['project_id'])
                print(f"Found the following integration:\n"
                      f"  Name: {gcp_input['name']}\n"
                      f"  Package Policy ID: {gcp_input['package_policy_id']}\n"
                      f"  GCP Project: {stream['project_id']}\n"
                      f"  Datatype: {stream['data_stream']['type']}\n"
                      f"  Dataset: {stream['data_stream']['dataset']}\n")
                # Create populate gcp_project -> package_policy_map so we can delete integration later if needed
                if stream['project_id'] in gcp_project_to_package_policy_map:
                    gcp_project_to_package_policy_map[stream['project_id']].append(gcp_input['package_policy_id'])
                else:
                    gcp_project_to_package_policy_map[stream['project_id']] = [gcp_input['package_policy_id']]

    # Create a list of unique GCP project_ids
    agent_gcp_projects = list(dict.fromkeys(agent_gcp_projects))
    print(f"Agent configured project IDs: {agent_gcp_projects}\n")

    # Find GCP projects that don't have an Elastic Integration
    new_gcp_projects = get_list_diffs(primary_list=active_gcp_projects,
                                      secondary_list=agent_gcp_projects)
    print(f"New GCP Projects found that need integrations: {new_gcp_projects}\n")

    # Loop through *new* GCP projects and for each one deploy an integration to each
    # agent listening to GCP telemetry
    for project in new_gcp_projects:
        print(f"Creating Metrics & Logs Integration for GCP Project: {project}\n")
        for agent in agents['list']:
            deploy_integration(agent_policy_id=agent["policy_id"], gcp_project_id=project)

    # Find projects configured in fleet that point to GCP projects that don't exist anymore
    deleted_gcp_projects = get_list_diffs(primary_list=agent_gcp_projects,
                                          secondary_list=active_gcp_projects)
    print(f"Integrations found for GCP projects that have been deleted: {deleted_gcp_projects}\n")
    # Loop through *deleted* GCP projects and for each one delete the integration on each
    # agent listening to GCP telemetry
    for project in deleted_gcp_projects:
        print(f"Deleting Metrics & Logs Integration for GCP Project: {project}\n")
        if project in gcp_project_to_package_policy_map.keys():
            for package_policy_id in gcp_project_to_package_policy_map[project]:
                delete_integration_policy(package_policy_id=package_policy_id)


if __name__ == "__main__":
    main()
