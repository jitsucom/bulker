package app

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"net/http"
	"regexp"
	"sync"
)

const kubeConfig = "apiVersion: v1\nclusters:\n- cluster:\n    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1ERXpNVEV3TXpnMU4xb1hEVE16TURFeU9ERXdNemcxTjFvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTUdtCnJQOHVCMzluQjNzb2Z2bldMMWJLcHNnOTJSSmpKc2h2Z1F3WndSNjFyQlhIOTd0WG5YR2doeTllQjFiS0NkTS8KV2h2ZEF0L0drTmhIT3p2VlpHTDFXdUZwZ0pocnhYSHpHRjF4OEVzK2ZMc1FFMHR4UDdROGNXRXFJbnZ4cTVlWgpBNCtCQUt6b3NYL0JoVmUrRndyMHdRYXQzZzBqQXgzNWpkQWdSMTd2bzNQTDBsdU56THRnYldReFVpYlltZklwCmp3L3F5UmxXN1BqU1ZDYllhQUo3ZEJ1QWE5Ui9NQzl6SGd4aDZqWXRrRVppdmFxYWlVNDZqaGsyZHRRN2lSL1cKMi9oajJjMHlrYlR3TTgyRGVabHhqNkdCUlRGVk9WK29tQmk1aW1XbUJWZFV6WFAzZ0sySjd3VEp5VUVpN2dCUQpla0tHTXhBMFhYdmVxL3dlTnlVQ0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZDMEtJMHhIam1MTUhiWXFSTFVDeC81aGluS3RNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBRk1PNzdrRUJoUSsyVndPWFJtKwpJbTg4L1JZaTRySUZyQ2R6aXMwdlo3WXJxTFFMMHBvd0dJdU1ncHcyOEI0bUxBc2ZCTWdPb3JTQldsT0FENitwCjhOc0xBRVZ4MGR0bTgwTm5kQVd6TkFPdnQ4TlpOcnlmcVZ1MThoVENCWGJKUmhHZy9yV1hBUWlpL2lWR1JEOGcKTEo4c2tzVmtwa09MeVpuVHBlcjhNY3o0NnIyL1FUc2pHN3VMbTBVWE5HQkd0VFJuQmhsTWkxQllOZEdPRDNHUAplVndqQUEwMzQ5T2VxMzY2cGx0eW85V0t1SDBDZjBoZzMzMXJsdW1kMVI5bFN5R1NaUFBPNmUvaWhsSFZjVFFGCm5PK1JHK1I0NUt1ZmJuRWhxL21WTkpiNHBJYTVGR1RNcDlPUmU1Nzg1UlpqZis3cnF5d1ZFbDZHZVdIQ000ZUoKbWpJPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==\n    server: https://AE5AF71C497388DA3560E1F336147D05.gr7.us-east-1.eks.amazonaws.com\n  name: arn:aws:eks:us-east-1:907458119157:cluster/Bulker\n- cluster:\n    certificate-authority: /Users/ildarnurislamov/.minikube/ca.crt\n    extensions:\n    - extension:\n        last-update: Wed, 29 Mar 2023 11:42:34 +04\n        provider: minikube.sigs.k8s.io\n        version: v1.29.0\n      name: cluster_info\n    server: https://127.0.0.1:49251\n  name: minikube\ncontexts:\n- context:\n    cluster: arn:aws:eks:us-east-1:907458119157:cluster/Bulker\n    namespace: bulker\n    user: arn:aws:eks:us-east-1:907458119157:cluster/Bulker\n  name: arn:aws:eks:us-east-1:907458119157:cluster/Bulker\n- context:\n    cluster: minikube\n    extensions:\n    - extension:\n        last-update: Wed, 29 Mar 2023 11:42:34 +04\n        provider: minikube.sigs.k8s.io\n        version: v1.29.0\n      name: context_info\n    namespace: default\n    user: minikube\n  name: minikube\ncurrent-context: minikube\nkind: Config\npreferences: {}\nusers:\n- name: arn:aws:eks:us-east-1:907458119157:cluster/Bulker\n  user:\n    exec:\n      apiVersion: client.authentication.k8s.io/v1beta1\n      args:\n      - --region\n      - us-east-1\n      - eks\n      - get-token\n      - --cluster-name\n      - Bulker\n      command: aws\n      env: null\n      interactiveMode: IfAvailable\n      provideClusterInfo: false\n- name: minikube\n  user:\n    client-certificate: /Users/ildarnurislamov/.minikube/profiles/minikube/client.crt\n    client-key: /Users/ildarnurislamov/.minikube/profiles/minikube/client.key\n"

// regex non alphanumeric characters
var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9-]`)

type JobRunner struct {
	objects.ServiceBase
	config    *AppConfig
	clientset *kubernetes.Clientset
}

func NewJobRunner(appContext *AppContext) (*JobRunner, error) {
	base := objects.NewServiceBase("job-runner")
	clientconfig, err := clientcmd.NewClientConfigFromBytes([]byte(appContext.config.KubernetesClientConfig))
	if err != nil {
		return nil, fmt.Errorf("error parsing kubernetes client config: %v", err)
	}
	cc, err := clientconfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating kubernetes client config: %v", err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(cc)
	if err != nil {
		return nil, fmt.Errorf("error creating kubernetes clientset: %v", err)
	}
	return &JobRunner{ServiceBase: base, config: appContext.config, clientset: clientset}, nil
}

func (j *JobRunner) SpecHandler(c *gin.Context) {
	image := c.Query("image")
	version := c.Query("version")

	pod := createPod(image, version, "spec", nil)
	pod, err := j.clientset.CoreV1().Pods("default").Create(c, pod, metav1.CreateOptions{})
	if err != nil {
		j.Errorf(err.Error())
		return
	}
	fmt.Println("Pod created")
	w, err := j.clientset.CoreV1().Pods("default").Watch(c, metav1.ListOptions{FieldSelector: "metadata.name=" + pod.Name})
	if err != nil {
		j.Errorf(err.Error())
		return
	}
	defer w.Stop()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Watching pod")
		for {
			select {
			case event := <-w.ResultChan():
				status := event.Object.(*v1.Pod).Status
				j.Infof("Pod %s Status %s : %v", pod.Name, event.Type, status.Phase)
				if status.Phase == v1.PodSucceeded || status.Phase == v1.PodFailed {
					err := j.clientset.CoreV1().Pods("default").Delete(c, pod.Name, metav1.DeleteOptions{})
					if err != nil {
						j.Infof("Error deleting pod: %v", err)
					}
					return
				}
			}
		}
	}()
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func createPod(image, version, purpose string, envMap map[string]string) *v1.Pod {
	podName := nonAlphaNum.ReplaceAllLiteralString(image, "-") + "." + version
	var command string
	switch purpose {
	case "read":
		command = "read --config /config/config.json --catalog /config/catalog.json --catalog /config/state.json"
	case "spec":
		command = "spec"
	}
	sideCarEnv := map[string]string{
		"STDOUT_PIPE_FILE":    "/pipes/stdout",
		"STDERR_PIPE_FILE":    "/pipes/stderr",
		"TASKS_CONNECTION_ID": "tasks",
		"STATE_CONNECTION_ID": "tasks_state",
	}
	utils.MapPutAll(sideCarEnv, envMap)
	sideCarEnvVar := make([]v1.EnvVar, 0, len(sideCarEnv))
	for k, v := range sideCarEnv {
		sideCarEnvVar = append(sideCarEnvVar, v1.EnvVar{Name: k, Value: v})
	}
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			Labels: map[string]string{
				"purpose": "bulker-source-" + purpose,
			},
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{Name: "source",
					Image:   fmt.Sprintf("%s:%s", image, version),
					Command: []string{"sh", "-c", fmt.Sprintf("eval \"$AIRBYTE_ENTRYPOINT %s\" 2> /pipes/stderr > /pipes/stdout", command)},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "pipes",
							MountPath: "/pipes",
						},
					},
				},
				{
					Name:  "sidecar",
					Image: "docker.io/jitsucom/sidecar:0.0.11",
					Env:   sideCarEnvVar,
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "pipes",
							MountPath: "/pipes",
						},
					},
				},
			},
			InitContainers: []v1.Container{
				{
					Name:    "init",
					Image:   "alpine",
					Command: []string{"sh", "-c", "mkfifo /pipes/stdout; mkfifo /pipes/stderr"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "pipes",
							MountPath: "/pipes",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "pipes",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}
	return pod
}

func (j *JobRunner) Close() error {
	return nil
}
