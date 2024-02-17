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

	// InitialSetup if true, ingress-manager will create ingress on start
	InitialSetup     bool `mapstructure:"INITIAL_SETUP" default:"false"`
	MigrateFromCaddy bool `mapstructure:"MIGRATE_FROM_CADDY" default:"false"`

	JitsuCnames        string `mapstructure:"JITSU_CNAMES" default:"cname.jitsu.com,cname2.jitsu.com"`
	CertificateMapName string `mapstructure:"CERTIFICATE_MAP_NAME" default:"custom-domains"`
	GoogleCloudProject string `mapstructure:"GOOGLE_CLOUD_PROJECT"`
	// AddGoogleCerts if true, for each CertificateMapEntry with letsencrypt cert ingress-manager will add google certs
	AddGoogleCerts bool `mapstructure:"ADD_GOOGLE_CERTS" default:"false"`
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
