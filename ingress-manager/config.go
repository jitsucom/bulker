package main

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/spf13/viper"
	"os"
)

type Config struct {
	appbase.Config `mapstructure:",squash"`

	// KubernetesNamespace namespace of bulker app. Default: `default`
	KubernetesNamespace    string `mapstructure:"KUBERNETES_NAMESPACE" default:"default"`
	KubernetesClientConfig string `mapstructure:"KUBERNETES_CLIENT_CONFIG" default:"local"`
	KubernetesContext      string `mapstructure:"KUBERNETES_CONTEXT"`
	// nodeSelector for sync pods in json format, e.g: {"disktype": "ssd"}
	KubernetesNodeSelector string `mapstructure:"KUBERNETES_NODE_SELECTOR"`

	// InitialSetup if true, ingress-manager will create ingress on start
	InitialSetup     bool `mapstructure:"INITIAL_SETUP" default:"false"`
	MigrateFromCaddy bool `mapstructure:"MIGRATE_FROM_CADDY" default:"false"`

	// IngressName name of ingress to create or manage
	IngressName string `mapstructure:"INGRESS_NAME" default:"ingest-custom-domain"`
	// CertificateIssuerName name of certificate issuer entity to use for ingress
	CertificateIssuerName string `mapstructure:"CERT_ISSUER_NAME" default:"cert-issuer"`
	// StaticIPName name of static ip to use for ingress
	StaticIPName    string `mapstructure:"STATIC_IP_NAME" default:""`
	StaticIPAddress string `mapstructure:"STATIC_IP_ADDRESS" default:""`

	BackendServiceName string `mapstructure:"BACKEND_SERVICE_NAME" default:"ingest-service"`
	BackendServicePort int    `mapstructure:"BACKEND_SERVICE_PORT" default:"8080"`
}

func init() {
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3051"))
}

func (c *Config) PostInit(settings *appbase.AppSettings) error {
	if c.KubernetesClientConfig == "" {
		return fmt.Errorf("%sKUBERNETES_CLIENT_CONFIG is required", settings.EnvPrefixWithUnderscore())
	}
	return c.Config.PostInit(settings)
}
