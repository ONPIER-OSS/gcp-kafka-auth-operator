/*
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
*/

package e2e

import (
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ONPIER-playground/gcp-kafka-auth-operator/test/utils"
)


// testNameSpace where the example custom resource is deployed
const testNameSpace = "test-kafka-user"

var _ = Describe("Manager", Ordered, func() {

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	BeforeAll(func() {})
	AfterAll(func() {
		By("uninstalling test-topic-access pod and removing kafka user custom resource")
		cmd := exec.Command("kubectl", "delete", "-f", "example/test-topic-view/manifest/", "-n", testNameSpace)
		_, _ = utils.Run(cmd)

		By("removing test namespace")
		cmd = exec.Command("kubectl", "delete", "ns", testNameSpace)
		_, _ = utils.Run(cmd)
	})
	AfterEach(func() {})
	Context("When a kafkaUser CR is created.", func() {
		It("should grant access to the specified kafka topics.", func() {
			By("creating test namespace")
			cmd := exec.Command("kubectl", "create", "ns", testNameSpace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

			By("creating a kafkaUser and k8s service account.")
			createUser := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "example/test-topic-view/manifest/kafkaUser.yaml", "-n", testNameSpace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(createUser, 1*time.Minute).Should(Succeed())

			// wait for google to assign the permissions before creating the pod
			time.Sleep(2 * time.Minute)

			By("creating the consumer pod.")
			createPod := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "example/test-topic-view/manifest/pod.yaml", "-n", testNameSpace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(createPod, 1*time.Minute).Should(Succeed())


			By("verifying access to kafka topics.")
			verifyKafkaTopicAccess := func(g Gomega) {
				expectedMessage := "Consumed message"
				cmd := exec.Command("kubectl", "logs", "-l", "app=kafka-access-test", "--namespace", testNameSpace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring(expectedMessage), "Kafka user access verify failed!")
			}
			Eventually(verifyKafkaTopicAccess, 5*time.Minute).Should(Succeed())
		})
	})
})
