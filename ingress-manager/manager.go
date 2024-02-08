package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	cmv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"sync"

	v1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"net"
	"regexp"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
)

const (
	k8sLabelPrefix       = "jitsu.com/"
	k8sCreatorLabel      = k8sLabelPrefix + "creator"
	k8sCreatorLabelValue = "bulker-ingress-manager"
)

type Manager struct {
	sync.Mutex
	appbase.Service
	clientset *kubernetes.Clientset
	config    *Config
	ingress   *netv1.Ingress
}

func NewManager(appContext *Context) *Manager {
	base := appbase.NewServiceBase("ingress-manager")

	m := &Manager{Service: base, clientset: appContext.clientset, config: appContext.config}
	err := m.Init()
	if err != nil {
		panic(err)
	}
	if appContext.config.MigrateFromCaddy {
		_ = m.MigrateCaddyCerts()
	}
	return m
}

func (m *Manager) Init() error {
	ingress, err := m.clientset.NetworkingV1().Ingresses(m.config.KubernetesNamespace).Get(context.Background(), m.config.IngressName, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return m.NewError("error getting ingress: %v", err)
	}
	if ingress == nil || errors.IsNotFound(err) {
		if !m.config.InitialSetup {
			return m.NewError("ingress not found. And initial setup is disabled")
		}
		// create ingress
		ingress = &netv1.Ingress{
			TypeMeta: metav1.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:   m.config.IngressName,
				Labels: map[string]string{k8sCreatorLabel: k8sCreatorLabelValue},
				Annotations: map[string]string{
					"kubernetes.io/ingress.class":                 "gce",
					"cert-manager.io/cluster-issuer":              m.config.CertificateIssuerName,
					"cert-manager.io/private-key-rotation-policy": "Always",
					"acme.cert-manager.io/http01-edit-in-place":   "true",
					"kubernetes.io/ingress.allow-http":            "true",
					"kubernetes.io/ingress.global-static-ip-name": m.config.StaticIPName,
				},
				Namespace: m.config.KubernetesNamespace,
			},
			Spec: netv1.IngressSpec{
				DefaultBackend: &netv1.IngressBackend{
					Service: &netv1.IngressServiceBackend{
						Name: m.config.BackendServiceName,
						Port: netv1.ServiceBackendPort{
							Number: int32(m.config.BackendServicePort),
						},
					},
				},
				TLS: []netv1.IngressTLS{},
			},
		}
		ingress, err = m.clientset.NetworkingV1().Ingresses(m.config.KubernetesNamespace).Create(context.Background(), ingress, metav1.CreateOptions{})
		if err != nil {
			return m.NewError("error creating ingress: %v", err)
		}
		m.Infof("ingress created: %s", ingress.Name)
	}
	m.ingress = ingress
	return nil
}

func (m *Manager) MigrateCaddyCerts() error {
	secrets, err := m.clientset.CoreV1().Secrets("caddy-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		m.Errorf("error listing secrets: %v", err)
		return m.NewError("error listing secrets: %v", err)
	}
	pattern := regexp.MustCompile(`caddy\.ingress--ocsp\.(.+)-(.+)`)
	domains := utils.NewSet[string]()
	for _, secret := range secrets.Items {
		if pattern.MatchString(secret.Name) {
			domain := pattern.FindStringSubmatch(secret.Name)[1]
			if !strings.HasSuffix(domain, ".d.jitsu.com") {
				domains.Put(domain)
			}
		}
	}
	m.Infof("found caddy domains: %v", domains.ToSlice())

	for _, domain := range domains.ToSlice() {
		crt, err := m.clientset.CoreV1().Secrets("caddy-system").Get(context.Background(), fmt.Sprintf("caddy.ingress--certificates.acme-v02.api.letsencrypt.org-directory.%s.%s.crt", domain, domain), metav1.GetOptions{})
		if err != nil {
			m.Errorf("error migrating crt for domain '%s': %v", domain, err)
		}
		key, err := m.clientset.CoreV1().Secrets("caddy-system").Get(context.Background(), fmt.Sprintf("caddy.ingress--certificates.acme-v02.api.letsencrypt.org-directory.%s.%s.key", domain, domain), metav1.GetOptions{})
		if err != nil {
			m.Errorf("error migrating key for domain '%s': %v", domain, err)
		}
		crtBytes := crt.Data["value"]
		keyBytes := key.Data["value"]
		_, err = m.addSecret(domain, crtBytes, keyBytes)
		if err != nil {
			m.Errorf("error creating tls secret for '%s': %v", domain, err)
		} else {
			_, err = m.addDomainToIngress(domain)
			if err != nil {
				m.Errorf("error adding domain to ingress for '%s': %v", domain, err)
			}
		}
	}
	return nil
}

type DomainStatus string

const (
	DomainStatusError       DomainStatus = "error"
	DomainStatusCNAME       DomainStatus = "dns_error"
	DomainStatusOK          DomainStatus = "ok"
	DomainStatusIssuingCert DomainStatus = "pending_ssl"
)

func (m *Manager) AddDomain(domain string) (status DomainStatus, err error) {
	m.Infof("[%s] adding domain...", domain)
	// first check that domain leads to the static ip
	ips, err := net.LookupIP(domain)
	if err != nil {
		m.Warnf("[%s] error looking up domain: %v", domain, err)
		return DomainStatusCNAME, nil
	}
	if !(len(ips) == 1 && ips[0].String() == m.config.StaticIPAddress) {
		m.Warnf("[%s] domain does not lead to static ip: %s", domain, m.config.StaticIPAddress)
		return DomainStatusCNAME, nil
	}
	secretAlreadyExists, err := m.addSecret(domain, []byte(""), []byte(""))
	if err != nil {
		return DomainStatusError, err
	}
	domainAlreadyExists, err := m.addDomainToIngress(domain)
	if err != nil {
		return DomainStatusError, err
	}
	if !domainAlreadyExists || !secretAlreadyExists {
		// domain was just added to ingress, so it's pending
		return DomainStatusIssuingCert, nil
	}

	certStatus, err := m.checkCertificate(domain)
	switch certStatus {
	case CertificateStatusError:
		m.Errorf("[%s] check certificate error: %v", domain, err)
		return DomainStatusError, err
	case CertificateStatusPending:
		m.Infof("[%s] issuing certificate", domain)
		return DomainStatusIssuingCert, nil
	case CertificateStatusOK:
		m.Infof("[%s] certificate is OK.", domain)
		return DomainStatusOK, nil
	default:
		m.Errorf("[%s] unknown certificate status: %v", domain, certStatus)
		return DomainStatusError, fmt.Errorf("unknown certificate status: %v", certStatus)
	}
}

type CertificateStatus string

const (
	CertificateStatusError   CertificateStatus = "error"
	CertificateStatusPending CertificateStatus = "pending"
	CertificateStatusOK      CertificateStatus = "ok"
)

func (m *Manager) addSecret(domain string, crt []byte, key []byte) (alreadyExists bool, err error) {
	m.Lock()
	defer m.Unlock()
	secret, err := m.clientset.CoreV1().Secrets(m.config.KubernetesNamespace).Get(context.Background(), domain, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		m.Errorf("[%s] error getting tls secret: %v", domain, err)
		return false, fmt.Errorf("error getting tls secret: %v", err)
	}
	if secret == nil || errors.IsNotFound(err) {
		// create secret
		_, err = m.clientset.CoreV1().Secrets(m.config.KubernetesNamespace).Create(context.Background(), &v1.Secret{
			Type: v1.SecretTypeTLS,
			ObjectMeta: metav1.ObjectMeta{
				Name: domain,
			},
			Data: map[string][]byte{
				"tls.crt": crt,
				"tls.key": key,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			m.Errorf("[%s] error creating tls secret: %v", domain, err)
			return false, fmt.Errorf("error creating tls secret: %v", err)
		}
		m.Infof("[%s] tls secret created.", domain)
	} else {
		alreadyExists = true
		m.Infof("[%s] tls secret already exists.", domain)
	}
	return
}

func (m *Manager) addDomainToIngress(domain string) (alreadyExists bool, err error) {
	m.Lock()
	defer m.Unlock()
	// add domain to ingress
	ingress, err := m.clientset.NetworkingV1().Ingresses(m.config.KubernetesNamespace).Get(context.Background(), m.config.IngressName, metav1.GetOptions{})
	if err != nil || ingress == nil {
		m.Errorf("[%s] error getting ingress: %v", domain, err)
		return false, fmt.Errorf("error getting ingress: %v", err)
	}
	ingressTLS := ingress.Spec.TLS
	for _, t := range ingressTLS {
		if t.Hosts[0] == domain {
			alreadyExists = true
			m.Infof("[%s] domain is already in ingress", domain)
			break
		}
	}
	if !alreadyExists {
		var patch []map[string]any
		if len(ingressTLS) == 0 {
			// create tls array
			patch = []map[string]any{{"op": "add", "path": "/spec/tls", "value": []map[string]any{{"hosts": []string{domain}, "secretName": domain}}}}
		} else {
			// add to the end
			patch = []map[string]any{{"op": "add", "path": "/spec/tls/-", "value": map[string]any{"hosts": []string{domain}, "secretName": domain}}}
		}
		marshaled, err := json.Marshal(patch)
		if err != nil {
			m.Errorf("[%s] error marshaling patch json: %v", domain, err)
			return alreadyExists, fmt.Errorf("error marshaling patch json: %v", err)
		}
		_, err = m.clientset.NetworkingV1().Ingresses(m.config.KubernetesNamespace).Patch(context.Background(), m.config.IngressName, types.JSONPatchType, marshaled, metav1.PatchOptions{})
		if err != nil {
			m.Errorf("[%s] error patching ingress: %v", domain, err)
			return alreadyExists, fmt.Errorf("error patching ingress: %v", err)
		}
		m.Infof("[%s] added to ingress.", domain)
	}
	return
}

func (m *Manager) addCertManagerCertificate(domain string) error {
	m.Lock()
	defer m.Unlock()
	// create certificate
	cert := cmv1.Certificate{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Certificate",
			APIVersion: "cert-manager.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      domain,
			Namespace: m.config.KubernetesNamespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         utils.NvlString(m.ingress.APIVersion, "networking.k8s.io/v1"),
				BlockOwnerDeletion: utils.BoolPointer(true),
				Controller:         utils.BoolPointer(true),
				Kind:               utils.NvlString(m.ingress.Kind, "Ingress"),
				Name:               m.ingress.Name,
				UID:                m.ingress.UID,
			},
			},
		},
		Spec: cmv1.CertificateSpec{
			DNSNames: []string{domain},
			IssuerRef: cmmetav1.ObjectReference{
				Group: "cert-manager.io",
				Kind:  "ClusterIssuer",
				Name:  m.config.CertificateIssuerName,
			},
			SecretName: domain,
			PrivateKey: &cmv1.CertificatePrivateKey{
				RotationPolicy: "Always",
			},
			Usages: []cmv1.KeyUsage{"digital signature", "key encipherment"},
		},
	}
	payload, err := json.Marshal(cert)
	if err != nil {
		m.Errorf("[%s] error marshaling certificate: %v", domain, err)
		return fmt.Errorf("error marshaling certificate: %v", err)
	}
	m.Infof("[%s] creating certificate...", domain)
	// /apis/networking.k8s.io/v1/namespaces/newjitsu/ingresses/ingest-custom-domain
	res := m.clientset.RESTClient().Post().Prefix("apis", "cert-manager.io", "v1").Namespace("newjitsu").Resource("certificates").Body(payload).Do(context.Background())
	if res.Error() != nil {
		m.Errorf("[%s] error creating certificate: %v", domain, res.Error())
		return res.Error()
	} else {
		st, _ := res.Raw()
		status := 0
		res.StatusCode(&status)
		m.Infof("[%s] certificate created: %s status: %d", domain, string(st), status)
		return nil
	}
}

func (m *Manager) checkCertificate(domain string) (status CertificateStatus, err error) {
	conn, err := tls.Dial("tcp", fmt.Sprintf("%s:443", domain), nil)
	if err != nil {
		m.Warnf("[%s] tls: error dialing domain: %v", domain, err)
		return CertificateStatusPending, nil
	}

	err = conn.VerifyHostname(domain)
	if err != nil {
		m.Warnf("[%s] tls: error verifying hostname: %v", domain, err)
		return CertificateStatusPending, nil
	}
	expiry := conn.ConnectionState().PeerCertificates[0].NotAfter
	m.Infof("[%s] found certificate: issuer: %s expiry: %v", domain, conn.ConnectionState().PeerCertificates[0].Issuer, expiry.Format(time.RFC850))
	if time.Now().After(expiry) {
		return CertificateStatusError, fmt.Errorf("certificate expired: %v", expiry)
	}
	return CertificateStatusOK, nil
}
