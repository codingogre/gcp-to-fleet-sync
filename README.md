Creates and synchronizes Google Cloud Projects to Elastic Fleet Integration Policies.

Features:

* If GCP projects are deleted the integration policies related to that project are deleted as well
* If GCP projects are added integration policies are automatically created for the new project
* A master integration policy is defined in Kibana and used as the template for integrations
* If there are changes to the master integration policy those changes are propagated to all the other integrations
* The ability to skip GCP project IDs to allow for custom integration configurations

Here is the design diagram to help one understand the program and how it works:

![GCP to Fleet Sync Design](https://github.com/codingogre/gcp-to-fleet-sync/assets/2017420/8123148a-59bb-4ab2-b27c-74f49d9a0bb0)
