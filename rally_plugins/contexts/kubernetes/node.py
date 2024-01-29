# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from rally.task import context

from rally_plugins.contexts.kubernetes import context as common_context


@context.configure("nodes", order=1001, platform="kubernetes")
class NodeContext(common_context.BaseKubernetesContext):
    """Context for fetching nodes."""

    CONFIG_SCHEMA = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "labels": {
                "type": "object"
            }
        }
    }

    def setup(self):
        if self.context['scenario_name'] == \
            'Kubernetes.create_check_and_delete_pod_with_node_port_service':
            self.populate_node_port_details()

    def populate_node_port_details(self):
        node_list = self.client.list_node().items
        self.context['node_names'] = [
            node.metadata.name for node in node_list
        ]

        # Node Port URL
        kube_proxy_url = self.context["env"]["platforms"]["kubernetes"]["server"]

        if kube_proxy_url[-1] == '/':
            kube_proxy_url = kube_proxy_url[:-1]

        self.context['get_node_port_url'] = lambda node_name, svc_port: \
            f'{kube_proxy_url}/api/v1/nodes/{node_name}:{svc_port}/proxy/'

    def cleanup(self):
        pass