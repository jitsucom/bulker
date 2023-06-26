package main

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func GetK8SClientSet(config string) (*kubernetes.Clientset, error) {
	if config == "" || config == "local" {
		cc, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error getting in cluster config: %v", err)
		}
		// creates the clientset
		clientset, err := kubernetes.NewForConfig(cc)
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes clientset: %v", err)
		}
		return clientset, nil
	} else {
		clientconfig, err := clientcmd.NewClientConfigFromBytes([]byte(config))
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
		return clientset, nil
	}
}
