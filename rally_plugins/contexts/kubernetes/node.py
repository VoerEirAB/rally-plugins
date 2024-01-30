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
        """Method to create/verify context topology and populate details."""
        self.context.update({
            'node_names': self.client.list_filtered_nodes(
                node_labels=self.config.get('labels')
            )
        })

    def cleanup(self):
        """Method to clean up resource created for context."""
        pass
