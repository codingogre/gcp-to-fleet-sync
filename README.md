## Creates and synchronizes Google Cloud Projects to Elastic Fleet Integration Policies.

Features:

* If GCP projects are deleted the integration policies related to that project are deleted as well
* If GCP projects are added integration polcies are automatically created for the new project
* A master integration policy is defined in Kibana and used as the template for integrations
