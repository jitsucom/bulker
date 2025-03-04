package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/mitchellh/mapstructure"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/ptr"
	"math"
	"regexp"
	"strconv"
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

var cgroupCPUUsage = regexp.MustCompile(`(?m:^usage_usec (\d+)$)`)
var cgroupMemUsage = regexp.MustCompile(`(?m:^(\d+)$)`)

type JobRunner struct {
	appbase.Service
	config        *Config
	namespace     string
	clientConfig  *rest.Config
	clientset     *kubernetes.Clientset
	closeCh       chan struct{}
	taskStatusCh  chan *TaskStatus
	runningPods   map[string]time.Time
	cleanedUpPods types.Set[string]
}

func NewJobRunner(appContext *Context) (*JobRunner, error) {
	base := appbase.NewServiceBase("job-runner")
	clientset, clientConfig, err := GetK8SClientSet(appContext)
	if err != nil {
		return nil, err
	}
	j := &JobRunner{Service: base, config: appContext.config, clientset: clientset, clientConfig: clientConfig, namespace: appContext.config.KubernetesNamespace,
		closeCh:       make(chan struct{}),
		taskStatusCh:  make(chan *TaskStatus, 100),
		runningPods:   map[string]time.Time{},
		cleanedUpPods: types.NewSet[string](),
	}
	safego.RunWithRestart(j.watchPodStatuses)
	return j, nil
}

func (j *JobRunner) watchPodStatuses() {
	ticker := utils.NewTicker(time.Second*time.Duration(j.config.ContainerStatusCheckSeconds), time.Second*time.Duration(j.config.ContainerStatusCheckSeconds))
	defer ticker.Stop()
	for {
		select {
		case <-j.closeCh:
			return
		case <-ticker.C:
			list, err := j.clientset.CoreV1().Pods(j.namespace).List(context.Background(), metav1.ListOptions{LabelSelector: k8sCreatorLabel + "=" + k8sCreatorLabelValue})
			if err != nil {
				j.Errorf("failed to list pods: %v", err.Error())
				continue
			}
			activePods := types.NewSet[string]()
			for _, pod := range list.Items {
				activePods.Put(pod.Name)
				if j.cleanedUpPods.Contains(pod.Name) {
					continue
				}
				taskStatus := TaskStatus{}
				_ = mapstructure.Decode(pod.Annotations, &taskStatus)
				taskStatus.PodName = pod.Name
				status := pod.Status
				bytes, _ := json.Marshal(status)
				j.Debugf("Pod %s Status %s:\n%s", pod.Name, status.Phase, string(bytes))
				switch status.Phase {
				case v1.PodSucceeded:
					taskStatus.Status = StatusSuccess
					j.Infof("Pod %s succeeded. Cleaning up.", pod.Name)
					j.cleanupPod(pod.Name)
				case v1.PodFailed:
					taskStatus.Status = StatusFailed
					errors, _ := j.accumulateErrorLogs(pod.Name, taskStatus.TaskType, status)
					if len(strings.TrimSpace(errors)) == 0 {
						errors = accumulatePodStatus(status)
					}
					taskStatus.Error = errors
					j.Infof("Pod %s failed. Cleaning up.", pod.Name)
					j.cleanupPod(pod.Name)
				case v1.PodRunning:
					errors, sourceFailed := j.accumulateErrorLogs(pod.Name, taskStatus.TaskType, status)
					if len(strings.TrimSpace(errors)) == 0 && sourceFailed {
						errors = accumulatePodStatus(status)
					}
					if len(errors) > 0 || sourceFailed {
						taskStatus.Status = StatusFailed
						taskStatus.Error = errors
						j.Infof("Pod %s is running but had errors. Cleaning up.", pod.Name)
						j.cleanupPod(pod.Name)
					} else {
						if timeMark, ok := j.runningPods[pod.Name]; !ok || time.Now().Sub(timeMark) >= time.Minute {
							if time.Now().Sub(taskStatus.StartedAtTime()) > time.Hour*time.Duration(j.config.TaskTimeoutHours) {
								taskStatus.Status = StatusTimeExceeded
								taskStatus.Error = fmt.Sprintf("Task timeout: The task has been running for more than %d hours. Consider splitting the selected streams into multiple Sync entities.", j.config.TaskTimeoutHours)
								j.Errorf("Pod %s is running for more than %d hours. Deleting", pod.Name, j.config.TaskTimeoutHours)
								j.cleanupPod(pod.Name)
							} else {
								taskStatus.Status = StatusRunning
								metrics := j.getPodResUsage(pod.Name, "source")
								if len(metrics) > 0 {
									taskStatus.Metrics = metrics
								}
								j.Infof("Pod %s is running", pod.Name)
								j.runningPods[pod.Name] = time.Now()
							}
						} else {
							//report running status only once per minute
							continue
						}

					}
				case v1.PodPending:
					if time.Now().Sub(taskStatus.StartedAtTime()) > time.Second*time.Duration(j.config.ContainerInitTimeoutSeconds) {
						taskStatus.Status = StatusInitTimeout
						taskStatus.Error = accumulatePodStatus(status)
						j.Errorf("Pod %s is pending for more than %d seconds. Deleting", pod.Name, j.config.ContainerInitTimeoutSeconds)
						j.cleanupPod(pod.Name)
					} else {
						taskStatus.Status = StatusPending
						taskStatus.Error = accumulatePodStatus(status)
						j.Debugf("Pod %s is pending", pod.Name)
						continue
					}
				default:
					taskStatus.Status = StatusUnknown
					taskStatus.Error = accumulatePodStatus(status)
					j.SystemErrorf("Pod %s is in unknown state %s", pod.Name, status.Phase)
				}
				j.sendStatus(&taskStatus)
			}
			//clean up pods that are not active anymore
			for podName := range j.runningPods {
				if !activePods.Contains(podName) {
					delete(j.runningPods, podName)
				}
			}
			for podName := range j.cleanedUpPods {
				if !activePods.Contains(podName) {
					j.cleanedUpPods.Remove(podName)
				}
			}
		}
	}

}

func (j *JobRunner) sendStatus(taskStatus *TaskStatus) {
	select {
	case j.taskStatusCh <- taskStatus:
	default:
		j.SystemErrorf("taskStatusCh is full. Dropping task status: %+v", *taskStatus)
	}
}

func (j *JobRunner) cleanupPod(name string) {
	j.cleanedUpPods.Put(name)
	gracePeriodSeconds := int64(j.config.ContainerGraceShutdownSeconds + 5)
	_ = j.clientset.CoreV1().Pods(j.namespace).Delete(context.Background(), name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds})
	_ = j.clientset.CoreV1().Secrets(j.namespace).Delete(context.Background(), name+"-config", metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds})
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

func (j *JobRunner) accumulateErrorLogs(podName string, taskType string, status v1.PodStatus) (logs string, sourceFailed bool) {
	stb := strings.Builder{}
	//gather status from all containers
	c := make([]v1.ContainerStatus, 0, len(status.ContainerStatuses)+len(status.InitContainerStatuses))
	c = append(c, status.InitContainerStatuses...)
	c = append(c, status.ContainerStatuses...)
	for _, s := range c {
		state := s.State
		if state.Terminated != nil && state.Terminated.ExitCode != 0 {
			if s.Name == "source" {
				if taskType == "read" {
					// if read command fails for source container we expect that the sidecar will
					// handle all error status reporting because some streams could be already synced
					//so we don't mark source as failed here
					continue
				}
			}
			sourceFailed = true
			logs := j.getPodLogs(podName, "source", true, 50)
			if len(logs) > 0 {
				stb.WriteString(logs)
				stb.WriteRune('\n')
			}
		}
	}
	// all source logs get directed to pipe and translated to the sidecar
	// so if 'source' container fails we need to look for errors in the sidecar
	if stb.Len() == 0 && sourceFailed {
		logs := j.getPodLogs(podName, "sidecar", true, 50)
		if len(logs) > 0 {
			stb.WriteString(logs)
			stb.WriteRune('\n')
		} else {
			// if we couldn't find lines with errors in the source logs - get last 5 lines
			logs = j.getPodLogs(podName, "source", false, 5)
			if len(logs) > 0 {
				stb.WriteString(logs)
				stb.WriteRune('\n')
			}
		}
	}
	return stb.String(), sourceFailed
}

func (j *JobRunner) getPodLogs(podName, container string, onlyErrors bool, tailLines int64) string {
	req := j.clientset.CoreV1().Pods(j.namespace).GetLogs(podName, &v1.PodLogOptions{Container: container, TailLines: &tailLines})
	podLogs, err := req.Stream(context.Background())
	if err != nil {
		return fmt.Sprintf("ERR_FAILED_TO_READ_POD_LOGS:%s", err.Error())
	}
	defer podLogs.Close()
	buf := strings.Builder{}
	scanner := bufio.NewScanner(podLogs)
	scanner.Buffer(make([]byte, 1024*10), 1024*1024*10)
	errFound := false
	for scanner.Scan() {
		t := scanner.Text()
		if onlyErrors {
			tL := strings.ToLower(t)
			if !errFound && (strings.Contains(tL, "error") || strings.Contains(tL, "panic") || strings.Contains(tL, "errstd") || strings.Contains(tL, "fatal")) {
				errFound = true
			}
			//log everything after error was found
			if errFound {
				buf.WriteString(fmt.Sprintf("%s\n", t))
			}
		} else {
			buf.WriteString(fmt.Sprintf("%s\n", t))
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Sprintf("ERR_FAILED_TO_READ_POD_LOGS:%s", err.Error())
	}
	if buf.Len() > 0 {
		return fmt.Sprintf("[%s]: %s", container, buf.String())
	} else {
		return ""
	}
}

func (j *JobRunner) getPodResUsage(podName string, container string) (metrics map[string]any) {
	startedAt := time.Now()
	var err error
	defer func() {
		if err != nil {
			j.Errorf("Pod %s resource usage: %+v ms: %v error: %v", podName, metrics, time.Now().Sub(startedAt), err)
		} else {
			j.Infof("Pod %s resource usage: %+v ms: %v", podName, metrics, time.Now().Sub(startedAt))
		}
	}()
	cmd := []string{
		"sh",
		"-c",
		"cat /sys/fs/cgroup/cpu.stat && cat /sys/fs/cgroup/memory.current && cat /sys/fs/cgroup/memory.peak",
	}
	req := j.clientset.CoreV1().RESTClient().Post().Resource("pods").Name(podName).
		Namespace(j.namespace).SubResource("exec")
	option := &v1.PodExecOptions{
		Container: container,
		Command:   cmd,
		Stdout:    true,
		Stderr:    true,
	}
	req.VersionedParams(
		option,
		scheme.ParameterCodec,
	)
	exec, err := remotecommand.NewSPDYExecutor(j.clientConfig, "POST", req.URL())
	if err != nil {
		return nil
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return nil
	}
	metrics = map[string]any{}
	stdoutStr := stdout.String()
	cpuUsageMatches := cgroupCPUUsage.FindStringSubmatch(stdoutStr)
	if len(cpuUsageMatches) == 2 {
		c, _ := strconv.Atoi(cpuUsageMatches[1])
		if c > 0 {
			metrics["cpu_usage"] = float64(c) / 1000000
		}
	}
	memUsage := 0
	memUsageAllMatches := cgroupMemUsage.FindAllStringSubmatch(stdoutStr, -1)
	for _, memUsageMatches := range memUsageAllMatches {
		if len(memUsageMatches) == 2 {
			m, _ := strconv.Atoi(memUsageMatches[1])
			memUsage = utils.MaxInt(memUsage, m)
		}
	}
	if memUsage > 0 {
		metrics["mem_usage"] = memUsage
	}
	if stderr.Len() > 0 {
		err = fmt.Errorf(stderr.String())
	}

	return metrics
}

//func (j *JobRunner) CreateCronJob(taskDescriptor TaskDescriptor, configuration *TaskConfiguration) TaskStatus {
//	taskStatus := TaskStatus{TaskDescriptor: taskDescriptor}
//	jobId := "sync-" + strings.ToLower(taskStatus.SyncID)
//	if !configuration.IsEmpty() {
//		secret := j.createSecret(jobId, taskDescriptor, configuration)
//		_, err := j.clientset.CoreV1().Secrets(j.namespace).Create(context.Background(), secret, metav1.CreateOptions{})
//		if err != nil {
//			taskStatus.Status = StatusCreateFailed
//			taskStatus.Description = err.Error()
//			j.sendStatus(&taskStatus)
//			return taskStatus
//		}
//	}
//	cronJob := j.createCronJob(jobId, taskDescriptor, configuration)
//	cronJob, err := j.clientset.BatchV1().CronJobs(j.namespace).Create(context.Background(), cronJob, metav1.CreateOptions{})
//	if err != nil {
//		taskStatus.Status = StatusCreateFailed
//		taskStatus.Description = err.Error()
//	} else {
//		taskStatus.Status = StatusCreated
//		taskStatus.Description = "Starting sync job..."
//		taskStatus.PodName = cronJob.Name
//	}
//	j.sendStatus(&taskStatus)
//	return taskStatus
//}

func PodName(syncId, taskId, pkg string) string {
	taskId = utils.NvlString(taskId, uuid.NewLettersNumbers())
	podId := utils.JoinNonEmptyStrings(".", syncId, taskId)
	return strings.ToLower(nonAlphaNum.ReplaceAllLiteralString(pkg, "-") + "-" + podId)
}

func (j *JobRunner) CreatePod(taskDescriptor TaskDescriptor, configuration *TaskConfiguration) TaskStatus {
	taskStatus := TaskStatus{TaskDescriptor: taskDescriptor}
	podName := taskDescriptor.PodName()
	if !configuration.IsEmpty() {
		secret := j.createSecret(podName, taskDescriptor, configuration)
		_, err := j.clientset.CoreV1().Secrets(j.namespace).Create(context.Background(), secret, metav1.CreateOptions{})
		if err != nil {
			taskStatus.Status = StatusCreateFailed
			taskStatus.Error = err.Error()
			j.sendStatus(&taskStatus)
			return taskStatus
		}
	}
	pod := j.createPod(podName, taskDescriptor, configuration)
	pod, err := j.clientset.CoreV1().Pods(j.namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		taskStatus.Status = StatusCreateFailed
		taskStatus.Error = err.Error()
	} else {
		taskStatus.Status = StatusCreated
		taskStatus.PodName = pod.Name
	}
	j.sendStatus(&taskStatus)
	return taskStatus
}

func (j *JobRunner) createSecret(podName string, task TaskDescriptor, configuration *TaskConfiguration) *v1.Secret {
	trueVar := true
	configMapName := podName + "-config"
	if configuration.IsEmpty() {
		return nil
	}
	cm := &v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind: "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Labels:    map[string]string{k8sCreatorLabel: k8sCreatorLabelValue},
			Namespace: j.namespace,
		},
		Immutable: &trueVar,
		Data:      configuration.ToMap(),
	}
	return cm
}

//
//func (j *JobRunner) createCronJob(jobId string, task TaskDescriptor, configuration *TaskConfiguration) *v1batch.CronJob {
//	var command string
//	switch task.TaskType {
//	case "check":
//		command = "check --config /config/config.json"
//	case "discover":
//		command = "discover --config /config/config.json"
//	case "read":
//		command = "read --config /config/config.json --catalog /config/catalog.json --state /config/state.json"
//	case "spec":
//		command = "spec"
//	}
//	databaseURL := utils.NvlString(j.config.SidecarDatabaseURL, j.config.DatabaseURL)
//	sideCarEnv := map[string]string{
//		"STDOUT_PIPE_FILE":  "/pipes/stdout",
//		"STDERR_PIPE_FILE":  "/pipes/stderr",
//		"PACKAGE":           task.Package,
//		"PACKAGE_VERSION":   task.PackageVersion,
//		"COMMAND":           task.TaskType,
//		"NAMESPACE":         task.Namespace,
//		"TO_SAME_CASE":      task.ToSameCase,
//		"ADD_META":          task.AddMeta,
//		"TABLE_NAME_PREFIX": task.TableNamePrefix,
//		"FULL_SYNC":         task.FullSync,
//		"DATABASE_URL":      databaseURL,
//		"LOG_LEVEL":         j.config.LogLevel,
//		"DB_LOG_LEVEL":      j.config.DBLogLevel,
//		"STARTED_BY":        task.StartedBy,
//		"STARTED_AT":        task.StartedAt,
//	}
//	if task.SyncID != "" {
//		sideCarEnv["SYNC_ID"] = task.SyncID
//	}
//	if task.TaskID != "" {
//		sideCarEnv["TASK_ID"] = task.TaskID
//	}
//	if task.StorageKey != "" {
//		sideCarEnv["STORAGE_KEY"] = task.StorageKey
//	}
//	//utils.MapPutAll(sideCarEnv, envMap)
//	sideCarEnvVar := make([]v1.EnvVar, 0, len(sideCarEnv))
//	for k, v := range sideCarEnv {
//		sideCarEnvVar = append(sideCarEnvVar, v1.EnvVar{Name: k, Value: v})
//	}
//	volumes := []v1.Volume{
//		{
//			Name: "pipes",
//			VolumeSource: v1.VolumeSource{
//				EmptyDir: &v1.EmptyDirVolumeSource{},
//			},
//		},
//	}
//	volumeMounts := []v1.VolumeMount{
//		{
//			Name:      "pipes",
//			MountPath: "/pipes",
//		},
//	}
//	var nodeSelector map[string]string
//	if j.config.KubernetesNodeSelector != "" {
//		nodeSelector = map[string]string{}
//		err := hjson.Unmarshal([]byte(j.config.KubernetesNodeSelector), &nodeSelector)
//		if err != nil {
//			j.Errorf("failed to parse node selector from string: %s\nIngoring it. Error: %v", j.config.KubernetesNodeSelector, err)
//		}
//	}
//	initCommand := []string{"sh", "-c", "mkfifo /pipes/stdout; mkfifo /pipes/stderr"}
//	if !configuration.IsEmpty() {
//		initCommand = []string{"sh", "-c", "mkfifo /pipes/stdout; mkfifo /pipes/stderr; cp /configmap/* /config/; gunzip /config/*.gz"}
//		items := []v1.KeyToPath{}
//		for _, k := range configuration.Keys() {
//			items = append(items, v1.KeyToPath{
//				Key:  k,
//				Path: k + ".json.gz",
//			})
//		}
//		volumes = append(volumes, v1.Volume{
//			Name: "configmap",
//			VolumeSource: v1.VolumeSource{
//				Secret: &v1.SecretVolumeSource{
//					SecretName: jobId + "-config",
//					Items:      items,
//				},
//			},
//		})
//		volumes = append(volumes, v1.Volume{
//			Name: "config",
//			VolumeSource: v1.VolumeSource{
//				EmptyDir: &v1.EmptyDirVolumeSource{},
//			},
//		})
//		volumeMounts = append(volumeMounts, v1.VolumeMount{
//			Name:      "configmap",
//			MountPath: "/configmap",
//		})
//		volumeMounts = append(volumeMounts, v1.VolumeMount{
//			Name:      "config",
//			MountPath: "/config",
//		})
//	}
//	//truevar := true
//	tz := "Etc/UTC"
//	cronJob := &v1batch.CronJob{
//		TypeMeta: metav1.TypeMeta{
//			Kind: "CronJob",
//		},
//		ObjectMeta: metav1.ObjectMeta{
//			Name:        jobId,
//			Labels:      map[string]string{k8sCreatorLabel: k8sCreatorLabelValue},
//			Annotations: task.ExtractAnnotations(),
//			Namespace:   j.namespace,
//		},
//		Spec: v1batch.CronJobSpec{
//			Schedule: "*/2 * * * *",
//			TimeZone: &tz,
//			//Suspend:           &truevar,
//			ConcurrencyPolicy: v1batch.ForbidConcurrent,
//			JobTemplate: v1batch.JobTemplateSpec{
//				Spec: v1batch.JobSpec{
//					Template: v1.PodTemplateSpec{
//						ObjectMeta: metav1.ObjectMeta{
//							Name:        jobId,
//							Labels:      map[string]string{k8sCreatorLabel: k8sCreatorLabelValue},
//							Annotations: task.ExtractAnnotations(),
//							Namespace:   j.namespace,
//						},
//						Spec: v1.PodSpec{
//							RestartPolicy:                 v1.RestartPolicyNever,
//							NodeSelector:                  nodeSelector,
//							TerminationGracePeriodSeconds: ptr.To(int64(j.config.ContainerGraceShutdownSeconds)),
//							Containers: []v1.Container{
//								{Name: "source",
//									Image:   fmt.Sprintf("%s:%s", task.Package, task.PackageVersion),
//									Command: []string{"sh", "-c", fmt.Sprintf("eval \"$AIRBYTE_ENTRYPOINT %s\" 2> /pipes/stderr > /pipes/stdout", command)},
//									Env: []v1.EnvVar{{Name: "USE_STREAM_CAPABLE_STATE", Value: "true"},
//										{Name: "AUTO_DETECT_SCHEMA", Value: "true"}},
//									VolumeMounts: volumeMounts,
//									Resources: v1.ResourceRequirements{
//										Limits: v1.ResourceList{
//											v1.ResourceCPU: *resource.NewMilliQuantity(int64(2000), resource.DecimalSI),
//											// 2Gi
//											v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 31)), resource.BinarySI),
//										},
//										Requests: v1.ResourceList{
//											v1.ResourceCPU: *resource.NewMilliQuantity(int64(125), resource.DecimalSI),
//											// 256Mi
//											v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 29)), resource.BinarySI),
//										},
//									},
//								},
//								{
//									Name:            "sidecar",
//									ImagePullPolicy: v1.PullAlways,
//									Image:           j.config.SidecarImage,
//									Env:             sideCarEnvVar,
//									VolumeMounts:    volumeMounts,
//									Resources: v1.ResourceRequirements{
//										Limits: v1.ResourceList{
//											v1.ResourceCPU: *resource.NewMilliQuantity(int64(1000), resource.DecimalSI),
//											// 512Mi
//											v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 29)), resource.BinarySI),
//										},
//										Requests: v1.ResourceList{
//											v1.ResourceCPU: *resource.NewMilliQuantity(int64(0), resource.DecimalSI),
//											// 256
//											v1.ResourceMemory: *resource.NewQuantity(int64(0), resource.BinarySI),
//										},
//									},
//								},
//							},
//							InitContainers: []v1.Container{
//								{
//									Name:         "init",
//									Image:        "alpine",
//									Command:      initCommand,
//									VolumeMounts: volumeMounts,
//								},
//							},
//							Volumes: volumes,
//						},
//					},
//				},
//			},
//		},
//	}
//	return cronJob
//}

func (j *JobRunner) TerminatePod(podName string) {
	_ = j.clientset.CoreV1().Pods(j.namespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	_ = j.clientset.CoreV1().Secrets(j.namespace).Delete(context.Background(), podName+"-config", metav1.DeleteOptions{})
	_ = j.clientset.CoreV1().ConfigMaps(j.namespace).Delete(context.Background(), podName+"-config", metav1.DeleteOptions{})
}

func (j *JobRunner) createPod(podName string, task TaskDescriptor, configuration *TaskConfiguration) *v1.Pod {
	var command string
	debug := utils.Ternary(task.Debug == "true", "--debug ", "")
	switch task.TaskType {
	case "check":
		command = "check --config /config/config.json"
	case "discover":
		command = "discover --config /config/config.json"
	case "read":
		command = "read " + debug + "--config /config/config.json --catalog /config/catalog.json --state /config/state.json"
	case "spec":
		command = "spec"
	}
	databaseURL := utils.NvlString(j.config.SidecarDatabaseURL, j.config.DatabaseURL)
	sideCarEnv := map[string]string{
		"STDOUT_PIPE_FILE":   "/pipes/stdout",
		"STDERR_PIPE_FILE":   "/pipes/stderr",
		"PACKAGE":            task.Package,
		"PACKAGE_VERSION":    task.PackageVersion,
		"COMMAND":            task.TaskType,
		"NAMESPACE":          task.Namespace,
		"TO_SAME_CASE":       task.ToSameCase,
		"ADD_META":           task.AddMeta,
		"TABLE_NAME_PREFIX":  task.TableNamePrefix,
		"FULL_SYNC":          task.FullSync,
		"DATABASE_URL":       databaseURL,
		"LOG_LEVEL":          j.config.LogLevel,
		"DB_LOG_LEVEL":       j.config.DBLogLevel,
		"STARTED_BY":         task.StartedBy,
		"STARTED_AT":         task.StartedAt,
		"TASK_TIMEOUT_HOURS": strconv.Itoa(j.config.TaskTimeoutHours),
	}
	if task.SyncID != "" {
		sideCarEnv["SYNC_ID"] = task.SyncID
	}
	if task.TaskID != "" {
		sideCarEnv["TASK_ID"] = task.TaskID
	}
	if task.StorageKey != "" {
		sideCarEnv["STORAGE_KEY"] = task.StorageKey
	}
	if j.config.ClickhouseHost != "" {
		sideCarEnv["CLICKHOUSE_HOST"] = j.config.ClickhouseHost
		sideCarEnv["CLICKHOUSE_DATABASE"] = j.config.ClickhouseDatabase
		sideCarEnv["CLICKHOUSE_USERNAME"] = j.config.ClickhouseUsername
		sideCarEnv["CLICKHOUSE_PASSWORD"] = j.config.ClickhousePassword
		sideCarEnv["CLICKHOUSE_SSL"] = fmt.Sprintf("%t", j.config.ClickhouseSSL)
	}
	if len(configuration.FunctionsEnv) > 0 {
		b, _ := json.Marshal(configuration.FunctionsEnv)
		sideCarEnv["FUNCTIONS_ENV"] = string(b)
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
	volumeMounts := []v1.VolumeMount{
		{
			Name:      "pipes",
			MountPath: "/pipes",
		},
	}
	var nodeSelector map[string]string
	if j.config.KubernetesNodeSelector != "" {
		nodeSelector = map[string]string{}
		err := hjson.Unmarshal([]byte(j.config.KubernetesNodeSelector), &nodeSelector)
		if err != nil {
			j.Errorf("failed to parse node selector from string: %s\nIngoring it. Error: %v", j.config.KubernetesNodeSelector, err)
		}
	}
	initCommand := []string{"sh", "-c", "mkfifo /pipes/stdout; mkfifo /pipes/stderr; chmod 777 /pipes/*; echo \"OK\""}
	if !configuration.IsEmpty() {
		initCommand = []string{"sh", "-c", "mkfifo /pipes/stdout; mkfifo /pipes/stderr; chmod 777 /pipes/*; cp /configmap/* /config/; gunzip /config/*.gz; chmod 777 /config/*; echo \"OK\""}
		items := []v1.KeyToPath{}
		for _, k := range configuration.Keys() {
			items = append(items, v1.KeyToPath{
				Key:  k,
				Path: k + ".json.gz",
			})
		}
		volumes = append(volumes, v1.Volume{
			Name: "configmap",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: podName + "-config",
					Items:      items,
				},
			},
		})
		volumes = append(volumes, v1.Volume{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      "configmap",
			MountPath: "/configmap",
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      "config",
			MountPath: "/config",
		})
	}
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Labels:      map[string]string{k8sCreatorLabel: k8sCreatorLabelValue},
			Annotations: task.ExtractAnnotations(),
			Namespace:   j.namespace,
		},
		Spec: v1.PodSpec{
			RestartPolicy:                 v1.RestartPolicyNever,
			NodeSelector:                  nodeSelector,
			TerminationGracePeriodSeconds: ptr.To(int64(j.config.ContainerGraceShutdownSeconds)),
			//SecurityContext:               &v1.PodSecurityContext{FSGroup: ptr.To(int64(65534))},
			Containers: []v1.Container{
				{Name: "source",
					Image:   fmt.Sprintf("%s:%s", task.Package, task.PackageVersion),
					Command: []string{"sh", "-c", fmt.Sprintf("eval \"$AIRBYTE_ENTRYPOINT %s\" 2> /pipes/stderr > /pipes/stdout", command)},
					Env: []v1.EnvVar{{Name: "USE_STREAM_CAPABLE_STATE", Value: "true"},
						{Name: "AUTO_DETECT_SCHEMA", Value: "true"},
						{Name: "JAVA_OPTS", Value: "-Xmx7000m"}},

					VolumeMounts: volumeMounts,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: *resource.NewMilliQuantity(int64(500), resource.DecimalSI),
							// 8Gi
							v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 33)), resource.BinarySI),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU: *resource.NewMilliQuantity(int64(100), resource.DecimalSI),
							// 256Mi
							v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 28)), resource.BinarySI),
						},
					},
				},
				{
					Name:            "sidecar",
					ImagePullPolicy: v1.PullAlways,
					Image:           j.config.SidecarImage,
					Env:             sideCarEnvVar,
					VolumeMounts:    volumeMounts,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: *resource.NewMilliQuantity(int64(500), resource.DecimalSI),
							// 4Gi
							v1.ResourceMemory: *resource.NewQuantity(int64(math.Pow(2, 32)), resource.BinarySI),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU: *resource.NewMilliQuantity(int64(0), resource.DecimalSI),
							// 256
							v1.ResourceMemory: *resource.NewQuantity(int64(0), resource.BinarySI),
						},
					},
				},
			},
			InitContainers: []v1.Container{
				{
					Name:         "init",
					Image:        "alpine",
					Command:      initCommand,
					VolumeMounts: volumeMounts,
				},
			},
			Volumes: volumes,
		},
	}
	return pod
}

func (j *JobRunner) TaskStatusChannel() <-chan *TaskStatus {
	return j.taskStatusCh
}

func (j *JobRunner) Close() {
	select {
	case <-j.closeCh:
	default:
		close(j.closeCh)
	}
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
