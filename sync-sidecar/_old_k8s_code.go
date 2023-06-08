package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	fmt.Println("STARTED")

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	catalogJson := "{\n    \"streams\": [\n        {\n            \"sync_mode\": \"full_refresh\",\n            \"destination_sync_mode\": \"overwrite\",\n            \"cursor_field\": [\n                \"created_at\"\n            ],\n            \"stream\": {\n                \"name\": \"commits\",\n                \"json_schema\": {\n                    \"properties\": {\n                        \"author\": {\n                            \"type\": [\n                                \"null\",\n                                \"object\"\n                            ],\n                            \"properties\": {\n                                \"avatar_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"events_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"followers_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"following_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"gists_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"gravatar_id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"html_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"integer\"\n                                    ]\n                                },\n                                \"login\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"node_id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"organizations_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"received_events_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"repos_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"site_admin\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"boolean\"\n                                    ]\n                                },\n                                \"starred_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"subscriptions_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"type\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                }\n                            }\n                        },\n                        \"comments_url\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ]\n                        },\n                        \"commit\": {\n                            \"type\": [\n                                \"null\",\n                                \"object\"\n                            ],\n                            \"properties\": {\n                                \"author\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"object\"\n                                    ],\n                                    \"properties\": {\n                                        \"date\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ],\n                                            \"format\": \"date-time\"\n                                        },\n                                        \"email\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"name\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        }\n                                    }\n                                },\n                                \"comment_count\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"integer\"\n                                    ]\n                                },\n                                \"committer\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"object\"\n                                    ],\n                                    \"properties\": {\n                                        \"date\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ],\n                                            \"format\": \"date-time\"\n                                        },\n                                        \"email\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"name\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        }\n                                    }\n                                },\n                                \"message\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"tree\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"object\"\n                                    ],\n                                    \"properties\": {\n                                        \"sha\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"url\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        }\n                                    }\n                                },\n                                \"url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"verification\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"object\"\n                                    ],\n                                    \"properties\": {\n                                        \"payload\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"reason\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"signature\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"string\"\n                                            ]\n                                        },\n                                        \"verified\": {\n                                            \"type\": [\n                                                \"null\",\n                                                \"boolean\"\n                                            ]\n                                        }\n                                    }\n                                }\n                            }\n                        },\n                        \"committer\": {\n                            \"type\": [\n                                \"null\",\n                                \"object\"\n                            ],\n                            \"properties\": {\n                                \"avatar_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"events_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"followers_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"following_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"gists_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"gravatar_id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"html_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"integer\"\n                                    ]\n                                },\n                                \"login\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"node_id\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"organizations_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"received_events_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"repos_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"site_admin\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"boolean\"\n                                    ]\n                                },\n                                \"starred_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"subscriptions_url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"type\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                },\n                                \"url\": {\n                                    \"type\": [\n                                        \"null\",\n                                        \"string\"\n                                    ]\n                                }\n                            }\n                        },\n                        \"created_at\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ],\n                            \"format\": \"date-time\"\n                        },\n                        \"html_url\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ]\n                        },\n                        \"node_id\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ]\n                        },\n                        \"parents\": {\n                            \"type\": [\n                                \"null\",\n                                \"array\"\n                            ]\n                        },\n                        \"repository\": {\n                            \"type\": [\n                                \"string\"\n                            ]\n                        },\n                        \"sha\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ]\n                        },\n                        \"url\": {\n                            \"type\": [\n                                \"null\",\n                                \"string\"\n                            ]\n                        }\n                    }\n                },\n                \"supported_sync_modes\": [\n                    \"full_refresh\",\n                    \"incremental\"\n                ],\n                \"source_defined_primary_key\": [\n                    [\n                        \"sha\"\n                    ]\n                ],\n                \"source_defined_cursor\": true,\n                \"default_cursor_field\": [\n                    \"created_at\"\n                ]\n            }\n        }\n    ]\n}"
	configJson := "{\"branch\":\"\",\"client_id\":\"ccf6ad9bdb05b38441a3\",\"client_secret\":\"d34b01ac1d1cc52ce6b9552ea9a20f4b5fe80113\",\"credentials\":{\"access_token\":\"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\",\"option_title\":\"OAuth Credentials\"},\"page_size_for_large_streams\":10,\"repository\":\"jitsucom/jitsu\",\"start_date\":\"2022-04-30T20:00:00Z\"}"
	configMap := v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind: "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test2",
		},
		Data: map[string]string{
			"config":  configJson,
			"catalog": catalogJson,
		},
	}

	_, err = clientset.CoreV1().ConfigMaps("default").Create(context.Background(), &configMap, metav1.CreateOptions{})
	if err != nil {

		fmt.Println(err)
	}

	podName := "github-" + string(uuid.NewUUID())
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			Labels: map[string]string{
				"app": "test2",
			},
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{Name: "test3",
					Image: "airbyte/source-github:0.4.3",
					Args:  []string{"read", "--config", "/config/config.json", "--catalog", "/config/catalog.json"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "config",
							MountPath: "/config",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "config",
					VolumeSource: v1.VolumeSource{
						ConfigMap: &v1.ConfigMapVolumeSource{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "test2",
							},
							Items: []v1.KeyToPath{
								{
									Key:  "config",
									Path: "config.json",
								},
								{
									Key:  "catalog",
									Path: "catalog.json",
								},
							},
						},
					},
				},
			},
		},
	}
	pod, err = clientset.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("Pod created")
	w, err := clientset.CoreV1().Pods("default").Watch(context.Background(), metav1.ListOptions{LabelSelector: "app=test2"})
	if err != nil {
		fmt.Println(err.Error())
	}
	defer w.Stop()
	go func() {
		fmt.Println("Watching pod")
		select {
		case event := <-w.ResultChan():
			fmt.Printf("%s : %v", event.Type, event.Object)
		}
	}()
	var logStream io.ReadCloser
	for {
		logStream, err = clientset.CoreV1().Pods("default").GetLogs(pod.Name, &v1.PodLogOptions{Follow: true}).Stream(context.Background())
		//Stream(context.Background())
		if err != nil {
			fmt.Println("logStream query error: " + err.Error())
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	defer logStream.Close()
	reader := bufio.NewScanner(logStream)
	var line string
	for reader.Scan() {
		line = reader.Text()
		fmt.Printf("Pod: %v line: %v\n", podName, line)
	}

	fmt.Println("FINISHED")
}
