Creates and synchronizes Google Cloud Projects to Elastic Fleet Integration Policies.

Features:

* If GCP projects are deleted the integration policies related to that project are deleted as well
* If GCP projects are added integration polcies are automatically created for the new project
* A master integration policy is defined in Kibana and used as the template for integrations

Here is the a design diagram to help one understand the program and how it works:

![GCP to Fleet Sync Design non transparent](https://github.com/codingogre/gcp-to-fleet-sync/assets/2017420/733c0b8b-54fd-43e0-9883-c1747feee995)
