# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import random
import requests

from rally.common import cfg
from rally.common import utils as commonutils
from rally import exceptions
from rally.task import atomic
from rally.task import scenario

from rally_plugins.scenarios.kubernetes import common as common_scenario

CONF = cfg.CONF


@scenario.configure(
    "Kubernetes.create_check_and_delete_pod_with_cluster_ip_service",
    platform="kubernetes"
)
class PodWithClusterIPSvc(common_scenario.BaseKubernetesScenario):

    def run(self, image, port, protocol, command=None, status_wait=True):
        """Create pod and clusterIP svc, check with curl job, delete then.

        :param image: pod's image
        :param port: pod's container port and svc port integer
        :param protocol: pod's container port and svc port protocol
        :param command: pod's array of strings representing command
        :param status_wait: wait for pod status if True
        """
        namespace = self.choose_namespace()
        labels = {"app": self.generate_random_name()}

        name = self.client.create_pod(
            image,
            namespace=namespace,
            command=command,
            port=port,
            protocol=protocol,
            labels=labels,
            status_wait=status_wait
        )

        self.client.create_service(
            name,
            namespace=namespace,
            port=port,
            protocol=protocol,
            type="ClusterIP",
            labels=labels
        )

        commonutils.interruptable_sleep(CONF.kubernetes.start_prepoll_delay)

        endpoints = self.client.get_endpoints(name, namespace=namespace)
        ips = []
        for subset in endpoints.subsets:
            addrs = [addr.ip for addr in subset.addresses]
            ports = [p.port for p in subset.ports]
            ips.extend(["%s:%s" % (a, p) for a in addrs for p in ports])

        command = ["curl"]
        command.extend(ips)
        self.client.create_job(
            name,
            namespace=namespace,
            image="appropriate/curl",
            command=command,
            status_wait=True
        )

        self.client.delete_job(
            name,
            namespace=namespace,
            status_wait=status_wait
        )
        self.client.delete_service(name, namespace=namespace)
        self.client.delete_pod(
            name,
            namespace=namespace,
            status_wait=status_wait
        )


@scenario.configure(
    "Kubernetes.create_check_and_delete_pod_with_cluster_ip_service_"
    "custom_endpoints",
    platform="kubernetes"
)
class PodWithClusterIPSvcWithEndpoints(common_scenario.BaseKubernetesScenario):

    def run(self, image, port, protocol, command=None, status_wait=True):
        """Create pod and clusterIP svc with custom endpoints.

        Create pod and clusterIP svc with custom endpoints, check it with curl
        job and delete them all then.

        :param image: pod's image
        :param port: pod's container port and svc port integer
        :param protocol: pod's container port and svc port protocol
        :param command: pod's array of strings representing command
        :param status_wait: wait for pod status if True
        """
        namespace = self.choose_namespace()

        name = self.client.create_pod(
            image,
            namespace=namespace,
            command=command,
            port=port,
            protocol=protocol,
            status_wait=status_wait
        )

        self.client.create_service(
            name,
            namespace=namespace,
            port=port,
            protocol=protocol,
            type="ClusterIP"
        )

        ip = self.client.get_pod(name, namespace=namespace).status.pod_ip

        self.client.create_endpoints(
            name,
            namespace=namespace,
            ip=ip,
            port=port
        )

        command = ["curl", "%s:%s" % (ip, port)]
        self.client.create_job(
            name,
            namespace=namespace,
            image="appropriate/curl",
            command=command,
            status_wait=True
        )

        self.client.delete_job(
            name,
            namespace=namespace,
            status_wait=status_wait
        )
        self.client.delete_endpoints(name, namespace=namespace)
        self.client.delete_service(name, namespace=namespace)
        self.client.delete_pod(
            name,
            namespace=namespace,
            status_wait=status_wait
        )


@scenario.configure(
    "Kubernetes.create_check_and_delete_pod_with_node_port_service",
    platform="kubernetes"
)
class PodWithNodePortAndCheckService(common_scenario.BaseKubernetesScenario):

    def run(self, image, port, protocol, request_timeout=None,
            command=None, status_wait=True):
        """Create pod and nodePort svc, request pod by port and delete then.

        :param image: pod's image
        :param port: pod's container port and svc port integer
        :param protocol: pod's container port and svc port protocol
        :param request_timeout: GET request timeout for check nodePort svc IP
        :param command: pod's array of strings representing command
        :param status_wait: wait for pod status if True
        """
        namespace = self.choose_namespace()
        labels = {"app": self.generate_random_name()}

        name = self.client.create_pod(
            image,
            namespace=namespace,
            command=command,
            port=port,
            protocol=protocol,
            labels=labels,
            status_wait=status_wait
        )

        self.client.create_service(
            name,
            namespace=namespace,
            port=port,
            protocol=protocol,
            type="NodePort",
            labels=labels
        )
        svc = self.client.get_service(name, namespace=namespace)

        svc_port = svc.spec.ports[0].node_port
        node_name = random.choice(self.context['node_names'])

        node_port_url = os.path.join(
            self.client._spec["server"], # kube proxy url
            f'api/v1/nodes/{node_name}:{svc_port}/proxy/'
        )

        sleep_time = CONF.kubernetes.status_poll_interval
        retries_total = CONF.kubernetes.status_total_retries

        with atomic.ActionTimer(self, "kubernetes.request_node_port_service"):

            i = 0

            try:
                while i < retries_total:
                    client_config = self.client.config
                    try:
                        kwargs= {
                            "headers": client_config.headers,
                            "cert": client_config.client_cert_files,
                            "verify": client_config.ssl_ca_cert or client_config.verify_ssl,
                            "timeout": request_timeout or None,
                        }
                        response = requests.get(node_port_url, **kwargs)
                        response.raise_for_status()
                    except (requests.RequestException) as ex:
                        if i < retries_total:
                            i += 1
                            commonutils.interruptable_sleep(sleep_time)
                        if i == retries_total:
                            raise exceptions.RallyException(
                                message="Unable to get response "
                                        "from %(url)s: %(ex)s" % {
                                            "url": node_port_url,
                                            "ex": str(ex)
                                        })
                    else:
                        break
            finally:
                self.client.delete_service(name, namespace=namespace)
                self.client.delete_pod(
                    name,
                    namespace=namespace,
                    status_wait=status_wait
                )

@scenario.configure(
    "Kubernetes.create_and_delete_pod_with_node_port_service",
    platform="kubernetes"
)
class PodWithNodePortService(common_scenario.BaseKubernetesScenario):

    def run(self, image, port, protocol, command=None, status_wait=True):
        """Create pod and nodePort svc, request pod by port and delete then.

        :param image: pod's image
        :param port: pod's container port and svc port integer
        :param protocol: pod's container port and svc port protocol
        :param command: pod's array of strings representing command
        :param status_wait: wait for pod status if True
        """
        namespace = self.choose_namespace()
        labels = {"app": self.generate_random_name()}

        name = self.client.create_pod(
            image,
            namespace=namespace,
            command=command,
            port=port,
            protocol=protocol,
            labels=labels,
            status_wait=status_wait
        )

        self.client.create_service(
            name,
            namespace=namespace,
            port=port,
            protocol=protocol,
            type="NodePort",
            labels=labels
        )

        svc = self.client.get_service(name, namespace=namespace)

        if not hasattr(svc.spec.ports[0], 'node_port'):
            raise exceptions.RallyException(
                message="Unable to get nodePort from service"
            )

        self.client.delete_service(name, namespace=namespace)
        self.client.delete_pod(
            name,
            namespace=namespace,
            status_wait=status_wait
        )
