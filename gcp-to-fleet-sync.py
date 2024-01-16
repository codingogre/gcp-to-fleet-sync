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


# Creates an integration policy for a given GCP project and agent policy.
def create_integration_policy(gcp_project_id: str, agent_policy_id: str, integration_policy: dict):
    integration_policy["policy_id"] = agent_policy_id
    integration_policy["name"] = gcp_project_id
    integration_policy["vars"]["project_id"]["value"] = gcp_project_id
    # define the keys to remove
    keys = ["id", "version", "revision", "created_at", "created_by", "updated_at", "updated_by"]
    # Remove keys from the master integration policy
    for key in keys:
        integration_policy.pop(key, None)
    url = f"{endpoint}/api/fleet/package_policies"
    r = s.post(url=url, headers=headers, json=integration_policy)
    return r.status_code

# TODO Multi agent same agent policy, same integration
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


def gcp_sync_output_builder(agent_id="", agent_version="", agent_policy_id="", input_name="", package_policy_id="",
                            gcp_project="", dataset="", datatype=""):
    return


def main():
    # Grab all the GCP projects which are active.  Make call API call under the
    # provided projects quota
    projects = get_active_gcp_projects(quota_project_id=os.environ["GCP_QUOTA_PROJECT"])
    active_gcp_projects = []
    for project in projects:
        active_gcp_projects.append(project.project_id)
    print(f"\nGCP Cloud configured project IDs: {active_gcp_projects}\n")

    # Get a list of agents that are listening to GCP telemetry
    agents = get_fleet_agents_by_query(query=f'tags:"{os.environ["GCP_AGENT_TAG"]}"')

    # Loop through agents and get the agent policy for each agent
    agent_gcp_projects = {}
    policy = {}
    sync_output = {}
    for agent in agents['list']:
        policy = get_full_agent_policy(policy_id=agent['policy_id'])
        print(f"Found the following agent:\n"
              f"  Agent ID: {agent['agent']['id']}\n"
              f"  Agent Version: {agent['agent']['version']}\n"
              f"  Agent Policy ID: {agent['policy_id']}")
        # Loop through the policy and get all the integrations (inputs)
        for inpt in policy['item']['inputs']:
            # Check to see if this integration is a GCP integration
            if 'gcp' == inpt["meta"]["package"]["name"]:
                print(f"      Integration Name: {inpt['name']}\n"
                      f"      Integration Policy ID: {inpt['package_policy_id']}")
                # Loop through all the data streams to grab GCP project_ids
                for stream in inpt['streams']:
                    print(f"          GCP Project: {stream['project_id']}\n"
                          f"              Dataset: {stream['data_stream']['dataset']}\n"
                          f"              Datatype: {stream['data_stream']['type']}\n")
                    # Add package_policy_id to dict in case we need to delete the integration later
                    if stream['project_id'] not in agent_gcp_projects.keys():
                        agent_gcp_projects[stream['project_id']] = inpt['package_policy_id']

    print(f"Agent configured project IDs: {[*agent_gcp_projects]}\n")

    # Find GCP projects that don't have an Elastic Integration. Used * to convert to a list
    new_gcp_projects = get_list_diffs(primary_list=active_gcp_projects,
                                      secondary_list=[*agent_gcp_projects])
    print(f"New GCP Projects found that need integrations: {new_gcp_projects}\n")

    # Loop through *new* GCP projects and for each one deploy an integration to each
    # agent policy listening to GCP telemetry
    for project in new_gcp_projects:
        print(f"Creating Metrics & Logs Integration for GCP Project: {project}\n")
        deploy_integration(agent_policy_id=policy["item"]["id"], gcp_project_id=project)

    # Find projects configured in fleet that point to GCP projects that don't exist anymore
    deleted_gcp_projects = get_list_diffs(primary_list=[*agent_gcp_projects],
                                          secondary_list=active_gcp_projects)
    print(f"Integrations found for the following GCP projects that no longer exist: {deleted_gcp_projects}\n")
    # Loop through *deleted* GCP projects and for each one delete the integration on each
    # agent listening to GCP telemetry
    for project in deleted_gcp_projects:
        print(f"Deleting Metrics & Logs Integration for GCP Project: {project}\n")
        delete_integration_policy(package_policy_id=agent_gcp_projects[project])


if __name__ == "__main__":
    main()
