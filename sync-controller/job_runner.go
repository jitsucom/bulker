package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/mitchellh/mapstructure"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"math"
	"regexp"
	"strings"
	"time"
)

const (
	k8sLabelPrefix       = "jitsu.com/"
	k8sCreatorLabel      = k8sLabelPrefix + "creator"
	k8sCreatorLabelValue = "bulker-sync-controller"
)

// regex non alphanumeric characters
var labelUnsupportedChars = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9-]`)

type JobRunner struct {
	appbase.Service
	config       *Config
	namespace    string
	clientset    *kubernetes.Clientset
	closeCh      chan struct{}
	taskStatusCh chan *TaskStatus
}

func NewJobRunner(appContext *Context) (*JobRunner, error) {
	base := appbase.NewServiceBase("job-runner")
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
	j := &JobRunner{Service: base, config: appContext.config, clientset: clientset, namespace: appContext.config.KubernetesNamespace,
		closeCh:      make(chan struct{}),
		taskStatusCh: make(chan *TaskStatus, 100)}
	go j.watchPodStatuses()
	return j, nil
}

func (j *JobRunner) watchPodStatuses() {
	for {
		//recover from panic
		defer func() {
			if r := recover(); r != nil {
				j.Errorf("watchPodStatuses Recovered from panic: %v", r)
			}
		}()
		ticker := utils.NewTicker(time.Second*time.Duration(j.config.ContainerStatusCheckSeconds), time.Second*time.Duration(j.config.ContainerStatusCheckSeconds))
		select {
		case <-j.closeCh:
			return
		case <-ticker.C:
			list, err := j.clientset.CoreV1().Pods(j.namespace).List(context.Background(), metav1.ListOptions{LabelSelector: k8sCreatorLabel + "=" + k8sCreatorLabelValue})
			if err != nil {
				j.Errorf("failed to list pods: %v", err.Error())
				return
			}
			for _, pod := range list.Items {
				labels := make(map[string]string, len(pod.Labels))
				for k, v := range pod.Labels {
					if strings.HasPrefix(k, k8sLabelPrefix) {
						labels[strings.TrimPrefix(k, k8sLabelPrefix)] = v
					}
				}
				taskStatus := TaskStatus{}
				_ = mapstructure.Decode(labels, &taskStatus)
				taskStatus.PodName = pod.Name
				status := pod.Status
				bytes, _ := json.Marshal(status)
				j.Infof("Pod %s Status %s:\n%s", pod.Name, status.Phase, string(bytes))
				switch status.Phase {
				case v1.PodSucceeded:
					taskStatus.Status = StatusSuccess
					j.Infof("Pod %s succeeded. Cleaning up.", pod.Name)
					j.cleanupPod(pod.Name)
				case v1.PodFailed:
					taskStatus.Status = StatusFailed
					taskStatus.Description = accumulatePodStatus(status)
					j.Infof("Pod %s failed. Cleaning up.", pod.Name)
					j.cleanupPod(pod.Name)
				case v1.PodRunning:
					taskStatus.Status = StatusRunning
					j.Debugf("Pod %s is running", pod.Name)
				case v1.PodPending:
					if time.Now().Sub(status.StartTime.Time) > time.Second*time.Duration(j.config.ContainerInitTimeoutSeconds) {
						taskStatus.Status = StatusInitTimeout
						taskStatus.Description = accumulatePodStatus(status)
						j.Errorf("Pod %s is pending for more than %d seconds. Deleting", pod.Name, j.config.ContainerInitTimeoutSeconds)
						j.cleanupPod(pod.Name)
					} else {
						taskStatus.Status = StatusPending
						taskStatus.Description = accumulatePodStatus(status)
						j.Debugf("Pod %s is pending", pod.Name)
					}
				default:
					taskStatus.Status = StatusUnknown
					taskStatus.Description = accumulatePodStatus(status)
					j.SystemErrorf("Pod %s is in unknown state %s", pod.Name, status.Phase)
				}
				j.taskStatusCh <- &taskStatus
			}
		}
	}

}

func (j *JobRunner) cleanupPod(name string) {
	gracePeriodSeconds := int64(math.Max(1.0, float64(j.config.ContainerStatusCheckSeconds)*0.8))
	_ = j.clientset.CoreV1().Pods(j.namespace).Delete(context.Background(), name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds})
	_ = j.clientset.CoreV1().ConfigMaps(j.namespace).Delete(context.Background(), name+"-config", metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds})
}

func accumulatePodStatus(status v1.PodStatus) string {
	stb := strings.Builder{}
	//gather status from all containers
	c := make([]v1.ContainerStatus, 0, len(status.ContainerStatuses)+len(status.InitContainerStatuses))
	c = append(c, status.InitContainerStatuses...)
	c = append(c, status.ContainerStatuses...)
	for _, s := range c {
		state := s.State
		if state.Terminated != nil {
			stb.WriteString(fmt.Sprintf("[%s] exit code %d message: %s. %s\n", s.Name, state.Terminated.ExitCode, state.Terminated.Reason, state.Terminated.Message))
		} else if state.Waiting != nil {
			stb.WriteString(fmt.Sprintf("[%s] waiting: %s. %s\n", s.Name, state.Waiting.Reason, state.Waiting.Message))
		} else if state.Running != nil {
			stb.WriteString(fmt.Sprintf("[%s] running\n", s.Name))
		}
	}
	return stb.String()
}

func (j *JobRunner) CreatePod(taskDescriptor TaskDescriptor, configuration *TaskConfiguration) TaskStatus {
	taskStatus := TaskStatus{TaskDescriptor: taskDescriptor}
	podName := nonAlphaNum.ReplaceAllLiteralString(taskDescriptor.Package, "-") + "." + taskDescriptor.PackageVersion + "-" + uuid.NewLettersNumbers()
	if !configuration.IsEmpty() {
		configMap := j.createConfigMap(podName, taskDescriptor, configuration)
		_, err := j.clientset.CoreV1().ConfigMaps(j.namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
		if err != nil {
			taskStatus.Status = StatusCreateFailed
			taskStatus.Description = err.Error()
			j.taskStatusCh <- &taskStatus
			return taskStatus
		}
	}
	pod := j.createPod(podName, taskDescriptor, configuration)
	pod, err := j.clientset.CoreV1().Pods(j.namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		taskStatus.Status = StatusCreateFailed
		taskStatus.Description = err.Error()
	} else {
		taskStatus.Status = StatusCreated
		taskStatus.PodName = pod.Name
	}
	j.taskStatusCh <- &taskStatus
	return taskStatus
}

func (j *JobRunner) createConfigMap(podName string, task TaskDescriptor, configuration *TaskConfiguration) *v1.ConfigMap {
	configMapName := podName + "-config"
	if configuration.IsEmpty() {
		return nil
	}
	cm := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind: "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Labels:    task.ExtractLabels(),
			Namespace: j.namespace,
		},
		Data: configuration.ToMap(),
	}
	return cm
}

func (j *JobRunner) createPod(podName string, task TaskDescriptor, configuration *TaskConfiguration) *v1.Pod {
	var command string
	switch task.TaskType {
	case "check":
		command = "check --config /config/config.json"
	case "discover":
		command = "discover --config /config/config.json"
	case "read":
		command = "read --config /config/config.json --catalog /config/catalog.json --state /config/state.json"
	case "spec":
		command = "spec"
	}
	sideCarEnv := map[string]string{
		"STDOUT_PIPE_FILE":    "/pipes/stdout",
		"STDERR_PIPE_FILE":    "/pipes/stderr",
		"BULKER_URL":          j.config.BulkerURL,
		"BULKER_AUTH_TOKEN":   j.config.BulkerAuthToken,
		"TASKS_CONNECTION_ID": j.config.BulkerTaskConnectionId,
		"STATE_CONNECTION_ID": "tasks_state",
		"PACKAGE":             task.Package,
		"PACKAGE_VERSION":     task.PackageVersion,
	}
	if task.SourceID != "" {
		sideCarEnv["SOURCE_ID"] = task.SourceID
	}
	if task.TaskID != "" {
		sideCarEnv["TASK_ID"] = task.TaskID
	}
	if task.DestinationId != "" {
		sideCarEnv["CONNECTION_ID"] = task.DestinationId
	}
	//utils.MapPutAll(sideCarEnv, envMap)
	sideCarEnvVar := make([]v1.EnvVar, 0, len(sideCarEnv))
	for k, v := range sideCarEnv {
		sideCarEnvVar = append(sideCarEnvVar, v1.EnvVar{Name: k, Value: v})
	}
	volumes := []v1.Volume{
		{
			Name: "pipes",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
	}
	sourceVolumeMounts := []v1.VolumeMount{
		{
			Name:      "pipes",
			MountPath: "/pipes",
		},
	}
	if !configuration.IsEmpty() {
		items := []v1.KeyToPath{}
		for k := range configuration.ToMap() {
			items = append(items, v1.KeyToPath{
				Key:  k,
				Path: k + ".json",
			})
		}
		cmVolume := v1.Volume{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: podName + "-config",
					},
					Items: items,
				},
			},
		}
		volumes = append(volumes, cmVolume)
		sourceVolumeMounts = append(sourceVolumeMounts, v1.VolumeMount{
			Name:      "config",
			MountPath: "/config",
		})
	}
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Labels:    task.ExtractLabels(),
			Namespace: j.namespace,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{Name: "source",
					Image:        fmt.Sprintf("%s:%s", task.Package, task.PackageVersion),
					Command:      []string{"sh", "-c", fmt.Sprintf("eval \"$AIRBYTE_ENTRYPOINT %s\" 2> /pipes/stderr > /pipes/stdout", command)},
					VolumeMounts: sourceVolumeMounts,
				},
				{
					Name:            "sidecar",
					ImagePullPolicy: v1.PullIfNotPresent,
					Image:           j.config.SidecarImage,
					Env:             sideCarEnvVar,
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
			Volumes: volumes,
		},
	}
	return pod
}

func (j *JobRunner) Close() error {
	select {
	case <-j.closeCh:
		return nil
	default:
		close(j.closeCh)
	}
	return nil
}

// Pod status watcher code
//
//w, err := j.clientset.CoreV1().Pods(j.namespace).Watch(c, metav1.ListOptions{FieldSelector: "metadata.name=" + pod.Name})
//if err != nil {
//j.Errorf(err.Error())
//return
//}
//defer w.Stop()
//wg := sync.WaitGroup{}
//wg.Add(1)
//go func() {
//	defer wg.Done()
//	fmt.Println("Watching pod")
//	for {
//		select {
//		case event := <-w.ResultChan():
//			status := event.Object.(*v1.Pod).Status
//			p, _ := j.clientset.CoreV1().Pods(j.namespace).Get(c, pod.Name, metav1.GetOptions{})
//			status = p.Status
//			bytes, _ := json.Marshal(status)
//			j.Infof("Pod %s Status %s : %v\n%s", pod.Name, event.Type, status.Phase, string(bytes))
//
//			if status.Phase == v1.PodSucceeded || status.Phase == v1.PodFailed {
//				err := j.clientset.CoreV1().Pods(j.namespace).Delete(c, pod.Name, metav1.DeleteOptions{})
//				if err != nil {
//					j.Infof("Error deleting pod: %v", err)
//				}
//				return
//			}
//		}
//	}
//}()
//wg.Wait()
