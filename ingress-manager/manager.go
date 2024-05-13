package main

import (
	"cloud.google.com/go/certificatemanager/apiv1"
	"cloud.google.com/go/certificatemanager/apiv1/certificatemanagerpb"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net"
	"regexp"
	"strings"
	"time"
)

type Manager struct {
	sync.Mutex
	appbase.Service
	certMgr  *certificatemanager.Client
	config   *Config
	cnames   types.Set[string]
	cmParent string
}

func NewManager(appContext *Context) *Manager {
	base := appbase.NewServiceBase("ingress-manager")
	cnames := strings.Split(appContext.config.JitsuCnames, ",")
	m := &Manager{Service: base, certMgr: appContext.certMgr, config: appContext.config, cnames: types.NewSet(cnames...),
		cmParent: fmt.Sprintf("projects/%s/locations/global", appContext.config.GoogleCloudProject)}
	err := m.Init()
	if err != nil {
		panic(err)
	}
	if appContext.config.MigrateFromCaddy {
		err = m.MigrateCaddyCerts()
		if err != nil {
			panic(err)
		}
	}
	if appContext.config.AddGoogleCerts {
		err = m.AddGoogleCerts()
		if err != nil {
			panic(err)
		}
	}
	if appContext.config.CleanupCerts {
		err = m.CleanupCerts()
		if err != nil {
			panic(err)
		}
	}
	return m
}

func (m *Manager) Init() error {
	if !m.config.InitialSetup {
		return nil
	}
	cmi := m.certMgr.ListCertificateMaps(context.Background(), &certificatemanagerpb.ListCertificateMapsRequest{Parent: m.cmParent})
	cm, err := cmi.Next()
	for ; err == nil; cm, err = cmi.Next() {
		if cm.Name == fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName) {
			m.Infof("certificate map '%s' already exists", m.config.CertificateMapName)
			return nil
		}
	}
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("error listing certificate maps: %v", err)
	}
	cmo, err := m.certMgr.CreateCertificateMap(context.Background(), &certificatemanagerpb.CreateCertificateMapRequest{
		Parent:           m.cmParent,
		CertificateMapId: m.config.CertificateMapName,
		CertificateMap: &certificatemanagerpb.CertificateMap{
			Name: fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
		},
	})
	if err != nil {
		return fmt.Errorf("error creating certificate map: %v", err)
	}
	cm, err = cmo.Wait(context.Background())
	if err != nil {
		return fmt.Errorf("error creating certificate map: %v", err)
	}
	m.Infof("certificate map created: %+v", cm)
	return nil
}

func (m *Manager) MigrateCaddyCerts() error {
	clientset, err := GetK8SClientSet(m.config)
	if err != nil {
		return err
	}
	ctx := context.Background()
	cm, err := m.certMgr.GetCertificateMap(ctx, &certificatemanagerpb.GetCertificateMapRequest{Name: "projects/jitsu-cloud-infra/locations/global/certificateMaps/custom-domains"})
	if err != nil {
		m.Errorf("error getting certificate map: %v", err)
		return m.NewError("error getting certificate map: %v", err)
	}
	m.Infof("certificate map: %+v", *cm)
	//return nil
	secrets, err := clientset.CoreV1().Secrets("caddy-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		m.Errorf("error listing secrets: %v", err)
		return m.NewError("error listing secrets: %v", err)
	}
	pattern := regexp.MustCompile(`caddy\.ingress--ocsp\.(.+)-(.+)`)
	domains := types.NewSet[string]()
	for _, secret := range secrets.Items {
		if pattern.MatchString(secret.Name) {
			domain := pattern.FindStringSubmatch(secret.Name)[1]
			if !strings.HasSuffix(domain, ".d.jitsu.com") && !strings.HasSuffix(domain, ".g.jitsu.com") {
				domains.Put(domain)
			}
		}
	}
	m.Infof("found caddy domains: %v", domains.ToSlice())

	for _, domain := range domains.ToSlice() {
		if domain == "ildar3.jitsu.dev" || domain == "ildar.jitsu.dev" || domain == "ildar2.jitsu.dev" {
			continue
		}
		crt, err := clientset.CoreV1().Secrets("caddy-system").Get(context.Background(), fmt.Sprintf("caddy.ingress--certificates.acme-v02.api.letsencrypt.org-directory.%s.%s.crt", domain, domain), metav1.GetOptions{})
		if err != nil {
			m.Errorf("[%s] error migrating crt: %v", domain, err)
		}
		key, err := clientset.CoreV1().Secrets("caddy-system").Get(context.Background(), fmt.Sprintf("caddy.ingress--certificates.acme-v02.api.letsencrypt.org-directory.%s.%s.key", domain, domain), metav1.GetOptions{})
		if err != nil {
			m.Errorf("[%s] error migrating key: %v", domain, err)
		}
		crtBytes := crt.Data["value"]
		keyBytes := key.Data["value"]

		certName := fmt.Sprintf("lets-%s", name(domain))
		op, err := m.certMgr.CreateCertificate(ctx, &certificatemanagerpb.CreateCertificateRequest{
			Parent:        m.cmParent,
			CertificateId: certName,
			Certificate: &certificatemanagerpb.Certificate{
				Name: fmt.Sprintf("%s/certificates/%s", m.cmParent, certName),
				Type: &certificatemanagerpb.Certificate_SelfManaged{
					SelfManaged: &certificatemanagerpb.Certificate_SelfManagedCertificate{
						PemCertificate: string(crtBytes),
						PemPrivateKey:  string(keyBytes),
					},
				},
			},
		})
		if err != nil {
			m.Errorf("[%s] error creating certificate: %v", domain, err)
		} else {
			cert, err := op.Wait(ctx)
			if err != nil {
				m.Errorf("[%s] error creating certificate: %v", domain, err)
			} else {
				m.Infof("[%s] certificate created: %+v", domain, cert)
				op2, err := m.certMgr.CreateCertificateMapEntry(ctx, &certificatemanagerpb.CreateCertificateMapEntryRequest{
					Parent:                fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
					CertificateMapEntryId: name(domain),
					CertificateMapEntry: &certificatemanagerpb.CertificateMapEntry{
						Name: fmt.Sprintf("%s/certificateMaps/%s/certificateMapEntries/%s", m.cmParent, m.config.CertificateMapName, name(domain)),
						Match: &certificatemanagerpb.CertificateMapEntry_Hostname{
							Hostname: domain,
						},
						Certificates: []string{fmt.Sprintf("%s/certificates/%s", m.cmParent, certName)},
					},
				})
				if err != nil {
					m.Errorf("[%s] error creating certificate map entry: %v", domain, err)
				} else {
					_, err = op2.Wait(ctx)
					if err != nil {
						m.Errorf("[%s] error creating certificate map entry: %v", domain, err)
					} else {
						m.Infof("[%s] certificate map entry created for", domain)
					}
				}
			}

		}
	}
	return nil
}

// CleanupCerts deletes all certificates and certificate map entries for domains that no longer lead to a valid cname
func (m *Manager) CleanupCerts() error {
	cmi := m.certMgr.ListCertificateMapEntries(context.Background(), &certificatemanagerpb.ListCertificateMapEntriesRequest{
		Parent:   fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
		PageSize: 1000,
	})
	cm, err := cmi.Next()
	for ; err == nil; cm, err = cmi.Next() {
		domain := cm.GetHostname()
		if strings.HasSuffix(domain, ".com.br") {
			continue
		}
		ok, _ := m.checkCname(domain)
		if !ok {
			m.Infof("[%s] cleaning certificate map entry: %+v", domain, cm)
			//sleep for 5 seconds to give time for the dns to propagate
			time.Sleep(5 * time.Second)
			op, err := m.certMgr.DeleteCertificateMapEntry(context.Background(), &certificatemanagerpb.DeleteCertificateMapEntryRequest{
				Name: cm.Name,
			})
			if err != nil {
				m.Errorf("[%s] error deleting certificate map entry: %v", domain, err)
				continue
			}
			err = op.Wait(context.Background())
			if err != nil {
				m.Errorf("[%s] error deleting certificate map entry: %v", domain, err)
			} else {
				m.Infof("[%s] certificate map entry deleted", domain)
			}
			for _, certName := range cm.Certificates {
				cop, err := m.certMgr.DeleteCertificate(context.Background(), &certificatemanagerpb.DeleteCertificateRequest{Name: certName})
				if err != nil {
					m.Errorf("[%s] error deleting certificate: %v", certName, err)
				}
				if cop != nil {
					err = cop.Wait(context.Background())
					if err != nil {
						m.Errorf("[%s] error deleting certificate: %v", certName, err)
					} else {
						m.Infof("[%s] certificate deleted", certName)
					}
				}
			}
		}
	}
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("error cleaning up certs: error listing certificate map entries: %v", err)
	}
	return nil
}

// RemoveLegacy One time function to remove legacy certificates from certificate map entries
func (m *Manager) RemoveLegacy() error {
	cmi := m.certMgr.ListCertificateMapEntries(context.Background(), &certificatemanagerpb.ListCertificateMapEntriesRequest{
		Parent:   fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
		PageSize: 1000,
	})
	cm, err := cmi.Next()
	for ; err == nil; cm, err = cmi.Next() {
		domain := cm.GetHostname()
		if strings.HasSuffix(domain, ".com.br") {
			continue
		}
		//if domain != "data.investing.com" {
		//	continue
		//}
		iof := utils.ArrayIndexOf(cm.Certificates, func(s string) bool {
			return strings.Contains(s, "/locations/global/certificates/lets-")
		})
		if iof >= 0 {
			m.Infof("[%s] removing legacy from certificate map entry: %+v", domain, cm)
			toDelete := cm.Certificates[iof]
			cm.Certificates = utils.ArrayFilter(cm.Certificates, func(s string) bool {
				return !strings.Contains(s, "/locations/global/certificates/lets-")
			})
			if len(cm.Certificates) > 0 {
				op, err := m.certMgr.UpdateCertificateMapEntry(context.Background(), &certificatemanagerpb.UpdateCertificateMapEntryRequest{
					CertificateMapEntry: cm,
					UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"certificates"}},
				})
				if err != nil {
					return fmt.Errorf("[%s] error removing legacy cert %s from map entry: %v", domain, toDelete, err)
				}
				_, err = op.Wait(context.Background())
				if err != nil {
					return fmt.Errorf("[%s] error removing legacy cert %s from map entry: %v", domain, toDelete, err)
				} else {
					m.Infof("[%s] legacy cert %s removed from map entry", domain, toDelete)
				}
				//cop, err := m.certMgr.DeleteCertificate(context.Background(), &certificatemanagerpb.DeleteCertificateRequest{Name: toDelete})
				//if err != nil {
				//	m.Errorf("[%s] error deleting certificate: %v", toDelete, err)
				//}
				//if cop != nil {
				//	err = cop.Wait(context.Background())
				//	if err != nil {
				//		m.Errorf("[%s] error deleting certificate: %v", toDelete, err)
				//	} else {
				//		m.Infof("[%s] certificate deleted", toDelete)
				//	}
				//}
			}
		}
	}
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("error cleaning up certs: error listing certificate map entries: %v", err)
	}
	return nil
}

// RemoveLegacyCerts One time function to remove legacy certificates
func (m *Manager) RemoveLegacyCerts() error {
	ci := m.certMgr.ListCertificates(context.Background(), &certificatemanagerpb.ListCertificatesRequest{
		Parent:   m.cmParent,
		PageSize: 1000,
	})
	c, err := ci.Next()
	for ; err == nil; c, err = ci.Next() {
		if !strings.Contains(c.Name, "/locations/global/certificates/lets-") {
			continue
		}
		if strings.HasSuffix(c.Name, "-com-br") {
			continue
		}
		toDelete := c.Name
		m.Infof("deleting legacy certificate: %s", toDelete)
		cop, err := m.certMgr.DeleteCertificate(context.Background(), &certificatemanagerpb.DeleteCertificateRequest{Name: toDelete})
		if err != nil {
			m.Errorf("[%s] error deleting certificate: %v", toDelete, err)
		}
		if cop != nil {
			err = cop.Wait(context.Background())
			if err != nil {
				m.Errorf("[%s] error deleting certificate: %v", toDelete, err)
			} else {
				m.Infof("[%s] certificate deleted", toDelete)
			}
		}
	}
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("error cleaning up certs: error listing certificates: %v", err)
	}
	return nil
}

func (m *Manager) AddGoogleCerts() error {
	cmi := m.certMgr.ListCertificateMapEntries(context.Background(), &certificatemanagerpb.ListCertificateMapEntriesRequest{
		Parent:   fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
		PageSize: 1000,
	})
	cm, err := cmi.Next()
	for ; err == nil; cm, err = cmi.Next() {
		domain := cm.GetHostname()
		ok, _ := m.checkCname(domain)
		if ok {
			m.Infof("[%s] certificate map entry: %+v", domain, cm)
			alreadyExists, err2 := m.IssueGoogleCert(domain, cm)
			if err2 != nil {
				m.Errorf("[%s] error issuing google certificate: %v", domain, err2)
			}
			if alreadyExists {
				m.Infof("[%s] certificate already exists", domain)
			} else {
				m.Infof("[%s] certificate issued", domain)
			}
		} else {
			m.Warnf("[%s] CNAME is NOT OK", domain)
		}
	}
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("error adding google certs: error listing certificate map entries: %v", err)
	}
	return nil
}

func name(domain string) string {
	return strings.ReplaceAll(domain, ".", "-")
}

func (m *Manager) IssueGoogleCert(domain string, mapEntry *certificatemanagerpb.CertificateMapEntry) (bool, error) {
	ctx := context.Background()
	cert, _ := m.certMgr.GetCertificate(ctx, &certificatemanagerpb.GetCertificateRequest{Name: fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain))})
	if cert == nil {
		m.Infof("[%s] creating google certificate", domain)
		op, err := m.certMgr.CreateCertificate(ctx, &certificatemanagerpb.CreateCertificateRequest{
			Parent:        m.cmParent,
			CertificateId: name(domain),
			Certificate: &certificatemanagerpb.Certificate{
				Name: fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain)),
				Type: &certificatemanagerpb.Certificate_Managed{
					Managed: &certificatemanagerpb.Certificate_ManagedCertificate{
						Domains: []string{domain},
					},
				},
			},
		})
		if err != nil {
			return false, fmt.Errorf("[%s] error creating google certificate: %v", domain, err)
		}
		cert, err = op.Wait(ctx)
		if err != nil {
			return false, fmt.Errorf("[%s] error creating google certificate: %v", domain, err)
		}
	} else {
		m.Infof("[%s] certificate already exists: %s", domain, cert.Name)
	}
	if mapEntry == nil {
		mapEntry, _ = m.certMgr.GetCertificateMapEntry(ctx, &certificatemanagerpb.GetCertificateMapEntryRequest{
			Name: fmt.Sprintf("%s/certificateMaps/%s/certificateMapEntries/%s", m.cmParent, m.config.CertificateMapName, name(domain)),
		})
	}
	if mapEntry == nil {
		m.Infof("[%s] creating google certificate map entry", domain)
		op, err := m.certMgr.CreateCertificateMapEntry(ctx, &certificatemanagerpb.CreateCertificateMapEntryRequest{
			Parent:                fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
			CertificateMapEntryId: name(domain),
			CertificateMapEntry: &certificatemanagerpb.CertificateMapEntry{
				Name: fmt.Sprintf("%s/certificateMaps/%s/certificateMapEntries/%s", m.cmParent, m.config.CertificateMapName, name(domain)),
				Match: &certificatemanagerpb.CertificateMapEntry_Hostname{
					Hostname: domain,
				},
				Certificates: []string{fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain))},
			},
		})
		if err != nil {
			return false, fmt.Errorf("[%s] error creating google certificate map entry: %v", domain, err)
		}
		_, err = op.Wait(ctx)
		if err != nil {
			return false, fmt.Errorf("[%s] error creating google certificate map entry: %v", domain, err)
		}
		return false, nil
	} else {
		m.Infof("[%s] certificate map entry already exists: %+v", domain, mapEntry)
		certDomains := utils.ArrayMap(mapEntry.Certificates, func(s string) string {
			return s[strings.LastIndex(s, "/")+1:]
		})
		if !utils.ArrayContains(certDomains, name(domain)) {
			m.Infof("[%s] adding domain to map entry", domain)
			mapEntry.Certificates = append(mapEntry.Certificates, fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain)))
			op, err := m.certMgr.UpdateCertificateMapEntry(ctx, &certificatemanagerpb.UpdateCertificateMapEntryRequest{
				CertificateMapEntry: mapEntry,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"certificates"}},
			})
			if err != nil {
				return false, fmt.Errorf("[%s] error adding domain to map entry: %v", domain, err)
			}
			_, err = op.Wait(ctx)
			if err != nil {
				return false, fmt.Errorf("[%s] error adding domain to map entry: %v", domain, err)
			}
			return false, nil
		} else {
			m.Infof("[%s] domain already exists in map entry", domain)
			return true, nil
		}
	}
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
	// first check that domain leads to the cna e
	cname, _ := m.checkCname(domain)
	if !cname {
		return DomainStatusCNAME, nil
	}

	alreadyExists, err := m.IssueGoogleCert(domain, nil)
	if err != nil {
		m.Errorf("[%s] error issuing google certificate: %v", domain, err)
		return DomainStatusError, err
	}
	if !alreadyExists {
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

func (m *Manager) checkCname(domain string) (ok bool, err error) {
	cname, err := net.LookupCNAME(domain)
	if err != nil {
		m.Warnf("[%s] error looking up domain: %v", domain, err)
		return false, err
	}
	if !m.cnames.Contains(strings.TrimSuffix(cname, ".")) {
		m.Warnf("[%s] incorrect CNAME record: %s", domain, cname)
		return false, nil
	}
	return true, nil
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
