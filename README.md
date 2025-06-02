# Prerequisites

## 1. Prepare google

If something is missing, please add

1. An existing role with the following permissions
```
    iam.roles.create
    iam.serviceAccounts.create
    iam.serviceAccounts.get
    iam.serviceAccounts.setIamPolicy
    resourcemanager.projects.getIamPolicy
    resourcemanager.projects.setIamPolicy
```

2. An existing iam that has the role from the step one assigned
3. A service account for that iam

## 2. Deploy

It's possible to run `make deploy` and the operator will get deployed by the kustomizations that can be found in the `config` folder

But there is a couple of steps that need to be done manually

1. Add a required annotation to the k8s SA

```
iam.gke.io/gcp-service-account: $EMAIL_OF_THE_SA_FROM_STEP_1_3
```

2. Execute gcloud command to let the pod use workload identity
```
gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL_STEP_1_3 --role roles/iam.workloadIdentityUser --member "serviceAccount:$GCP_PROJECT_ID.svc.id.goog[$OPERATOR_NS/$OPERATOR_SA_NAME]" --project $GCP_PROJECT_ID
```

## 3. Build

One can use docker/podman/nerdctl/buildah
```
podman build -t ghcr.io/onpier-oss/gcp-kafka-auth-operator .
podman push ghcr.io/onpier-oss/gcp-kafka-auth-operator
```

## 4. Test

Create a test namespace and deploy manifests from `example/test-auth/manifest/example.yaml`
After a couple of minutes, the pod must print all the topics of the kafka instance



## 4. Test

### Running Tests

1. Ensure you are authenticated with your GCP project and connected to google vpn.

2. Run the tests:
   ```bash
   make test-e2e
   ```


# What needs to be implemented

1. First of all, the user_controller must be refactored, it must consist of smaller functions at least. I would build the interface for the gcp objects so we can mock and test
2. Deletion of users is not supported, but it's a must have, I would also add `deletionProtection`
3. I'm using a bunch of google libs atm, maybe we can use a smaller amount of them, because there are two repos with gcp sdk
4. All the hardcoded values from the user_controller must be dynamic and the configuration shouldn't happen on the controller level, but in the main.go
5. Roles that are used for the kafka access must not be hardcoded, I would  use args to set something like
```sh
--read-only-role "managedkafka.viewer" --read-write-role "managedkafka.admin"
```
6. Conditions must be added to kafka role bindings, so one user can access only certain topics
7. I would also a topic controller, so we can manage topics with the operator and assigng user to them.
8. User status should be set after the reconciliation, so we can avoid calling the google API too often
---

# Boilerplate from the bootstrap
# gcp-kafka-auth-operator
// TODO(user): Add simple overview of use/purpose

## Description
// TODO(user): An in-depth paragraph about your project and overview of use

## Getting Started

### Prerequisites
- go version v1.22.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/gcp-kafka-auth-operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/gcp-kafka-auth-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following are the steps to build the installer and distribute this project to users.

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/gcp-kafka-auth-operator:tag
```

NOTE: The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without
its dependencies.

2. Using the installer

Users can just run kubectl apply -f <URL for YAML BUNDLE> to install the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/gcp-kafka-auth-operator/<tag or branch>/dist/install.yaml
```

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

