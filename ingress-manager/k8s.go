package main

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"strings"
)

func GetK8SClientSet(c *Config) (*kubernetes.Clientset, error) {
	config := c.KubernetesClientConfig
	if config == "" || config == "local" {
		// creates the in-cluster config
		cc, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error getting in cluster config: %v", err)
		}
		clientset, err := kubernetes.NewForConfig(cc)
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes clientset: %v", err)
		}
		return clientset, nil
	} else if strings.ContainsRune(config, '\n') {
		// suppose yaml file
		clientconfig, err := clientcmd.NewClientConfigFromBytes([]byte(config))
		if err != nil {
			return nil, fmt.Errorf("error parsing kubernetes client config: %v", err)
		}
		rawConfig, _ := clientconfig.RawConfig()
		clientconfig = clientcmd.NewNonInteractiveClientConfig(rawConfig,
			utils.NvlString(c.KubernetesContext, rawConfig.CurrentContext),
			&clientcmd.ConfigOverrides{},
			&clientcmd.ClientConfigLoadingRules{})
		cc, err := clientconfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes client config: %v", err)
		}
		clientset, err := kubernetes.NewForConfig(cc)
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes clientset: %v", err)
		}
		return clientset, nil
	} else {
		// suppose kubeconfig file path
		clientconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: config},
			&clientcmd.ConfigOverrides{
				CurrentContext: c.KubernetesContext,
			})
		cc, err := clientconfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes client config: %v", err)
		}
		clientset, err := kubernetes.NewForConfig(cc)
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes clientset: %v", err)
		}
		return clientset, nil
	}
}
