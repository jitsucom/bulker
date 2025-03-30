package main

import (
	"cloud.google.com/go/certificatemanager/apiv1"
	"cloud.google.com/go/certificatemanager/apiv1/certificatemanagerpb"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"regexp"
	"slices"

	"sync"

	"net"
	"strings"
	"time"
)

type Manager struct {
	sync.Mutex
	appbase.Service
	certMgr  *certificatemanager.Client
	config   *Config
	cnames   []string
	cmParent string
}

func NewManager(appContext *Context) *Manager {
	base := appbase.NewServiceBase("ingress-manager")
	cnames := strings.Split(appContext.config.JitsuCnames, ",")
	m := &Manager{Service: base, certMgr: appContext.certMgr, config: appContext.config, cnames: cnames,
		cmParent: fmt.Sprintf("projects/%s/locations/global", appContext.config.GoogleCloudProject)}
	err := m.Init()
	if err != nil {
		panic(err)
	}
	if appContext.config.CleanupCerts {
		err = m.CleanupCerts()
		if err != nil {
			panic(err)
		}
	}
	err = m.MigrateCaddyCert()
	if err != nil {
		panic(err)
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

func (m *Manager) MigrateCaddyCert() error {
	ctx := context.Background()
	cm, err := m.certMgr.GetCertificateMap(ctx, &certificatemanagerpb.GetCertificateMapRequest{Name: "projects/jitsu-cloud-infra/locations/global/certificateMaps/custom-domains"})
	if err != nil {
		m.Errorf("error getting certificate map: %v", err)
		return m.NewError("error getting certificate map: %v", err)
	}
	m.Infof("certificate map: %+v", *cm)
	domain := "t.calendso.com"
	crtBytes, _ := base64.StdEncoding.DecodeString("LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURmRENDQXdLZ0F3SUJBZ0lTQXh3N0tBaS9BYVJkMlMvWHVqeEVuRXprTUFvR0NDcUdTTTQ5QkFNRE1ESXgKQ3pBSkJnTlZCQVlUQWxWVE1SWXdGQVlEVlFRS0V3MU1aWFFuY3lCRmJtTnllWEIwTVFzd0NRWURWUVFERXdKRgpOakFlRncweU5UQXpNRFV5TWpNd05UZGFGdzB5TlRBMk1ETXlNak13TlRaYU1Ca3hGekFWQmdOVkJBTVREblF1ClkyRnNaVzVrYzI4dVkyOXRNRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUV0MzQ3NGVPVE1lZEwKWlRGTklxRS9NU0hHckk2OWxrL2xnMHdBYU9yVUc2RkFJdk0zWUhyanoyd0IwSkNwVWdwMDJDMUhaaHIxOGE3ZQppN3NodUdTYUM2T0NBZzh3Z2dJTE1BNEdBMVVkRHdFQi93UUVBd0lIZ0RBZEJnTlZIU1VFRmpBVUJnZ3JCZ0VGCkJRY0RBUVlJS3dZQkJRVUhBd0l3REFZRFZSMFRBUUgvQkFJd0FEQWRCZ05WSFE0RUZnUVVnaldtTTVHTGhKZTIKUHgvcnMrbnlXRDRweXhFd0h3WURWUjBqQkJnd0ZvQVVreWRHbUFPcFVXaU9tTmJFUWtqYkk3OVlsTkl3VlFZSQpLd1lCQlFVSEFRRUVTVEJITUNFR0NDc0dBUVVGQnpBQmhoVm9kSFJ3T2k4dlpUWXVieTVzWlc1amNpNXZjbWN3CklnWUlLd1lCQlFVSE1BS0dGbWgwZEhBNkx5OWxOaTVwTG14bGJtTnlMbTl5Wnk4d0dRWURWUjBSQkJJd0VJSU8KZEM1allXeGxibVJ6Ynk1amIyMHdFd1lEVlIwZ0JBd3dDakFJQmdabmdRd0JBZ0V3Z2dFREJnb3JCZ0VFQWRaNQpBZ1FDQklIMEJJSHhBTzhBZFFCT2RhTW5YSm9Rd3poYmJOVGZQMUxySGZEZ2podU5hY0N4K21TeFlwbzUzd0FBCkFaVm9wSWFpQUFBRUF3QkdNRVFDSUhGbktDVXhSR2dKQk5RbUIrNDhQaHhoMTRpV3RCNDJvNHVTS3MwR1cwZW0KQWlCZXdxQlRpZlVxWkE4dy9OU01TbkJtNnY3cURPV0pmRlA1TzM2VnBoM2Jwd0IyQUJOSzN4cTFtRUlKZUF4dgo3MHg2a2FRV3R5Tkp6bGhYYXQrdTJxZkNxK0FpQUFBQmxXaWtoM0VBQUFRREFFY3dSUUlnYnRGUTBDUUFuTmFXCnMrS0E1WStmOWRzT3VlOVpWVHJoNGROOURDYjZiMjBDSVFDMU5QV0VhOElnRC80S25nOUNrRHpHalRFTXpsOWkKOEJoNVJqTGluSkVxbkRBS0JnZ3Foa2pPUFFRREF3Tm9BREJsQWpFQW8rdjhtMCtTL2NXWFJQVzJsUUxvd1A1egpCTDFsSVh5M3QvMHQrUUZ5eW9TVm12Z25RdHdMYlpGTmgvbVZhR2hHQWpBeGVQSlZmZ0JLWEpZVlpmeTk4TEU5CkpnNVZqQWF2Z1BicWNQMzl6ckVEU00xNE0xak9kVzV5SHQ0UEZvVEVMUXM9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KCi0tLS0tQkVHSU4gQ0VSVElGSUNBVEUtLS0tLQpNSUlFVnpDQ0FqK2dBd0lCQWdJUkFMQlhQcEZ6bHlkdzI3U0h5enBGS3pnd0RRWUpLb1pJaHZjTkFRRUxCUUF3ClR6RUxNQWtHQTFVRUJoTUNWVk14S1RBbkJnTlZCQW9USUVsdWRHVnlibVYwSUZObFkzVnlhWFI1SUZKbGMyVmgKY21Ob0lFZHliM1Z3TVJVd0V3WURWUVFERXd4SlUxSkhJRkp2YjNRZ1dERXdIaGNOTWpRd016RXpNREF3TURBdwpXaGNOTWpjd016RXlNak0xT1RVNVdqQXlNUXN3Q1FZRFZRUUdFd0pWVXpFV01CUUdBMVVFQ2hNTlRHVjBKM01nClJXNWpjbmx3ZERFTE1Ba0dBMVVFQXhNQ1JUWXdkakFRQmdjcWhrak9QUUlCQmdVcmdRUUFJZ05pQUFUWjhaNUcKaC9naGNXQ29KdXVqK3JucTJoMjVFcWZVSnRsUkZMRmhmSFdXdnlJTE9SL1Z2dEVLUnFvdFBFb0poQzYrUUpWVgo2UmxBTjJaMTdUSk9kd1JKK0hCN3d4am56dmR4RVA2c2ROZ0ExTzF0SEhNV014Q2NPckxxYkdMMHZiaWpnZmd3CmdmVXdEZ1lEVlIwUEFRSC9CQVFEQWdHR01CMEdBMVVkSlFRV01CUUdDQ3NHQVFVRkJ3TUNCZ2dyQmdFRkJRY0QKQVRBU0JnTlZIUk1CQWY4RUNEQUdBUUgvQWdFQU1CMEdBMVVkRGdRV0JCU1RKMGFZQTZsUmFJNlkxc1JDU05zagp2MWlVMGpBZkJnTlZIU01FR0RBV2dCUjV0Rm5tZTdibDVBRnpnQWlJeUJwWTl1bWJiakF5QmdnckJnRUZCUWNCCkFRUW1NQ1F3SWdZSUt3WUJCUVVITUFLR0ZtaDBkSEE2THk5NE1TNXBMbXhsYm1OeUxtOXlaeTh3RXdZRFZSMGcKQkF3d0NqQUlCZ1puZ1F3QkFnRXdKd1lEVlIwZkJDQXdIakFjb0JxZ0dJWVdhSFIwY0RvdkwzZ3hMbU11YkdWdQpZM0l1YjNKbkx6QU5CZ2txaGtpRzl3MEJBUXNGQUFPQ0FnRUFmWXQ3U2lBMXNnV0dDSXB1bms0NnI0QUV4SVJjCk14a0tnVWhObHJydjFCMjFoT2FYTi81bWlFK0xPVGJyY21VL005eXZDNk1WWTczMEdORm9MOEloSjhqOHZyT0wKcE1ZMjJPUDZiYVMxazlZTXJ0RFRsd0pIb0dieTA0VGhUVWVCRGtzUzlSaXVIdmljWnFCZWRRZElGNjVwWnVocAplRGNHQmNMaVlhc1FyL0VPNWd4eHRMeVRtZ3NIU09WU0JjRk9uOWxndjdMRUNQcTlpN21mSDNtcHhnclJLU3hICnBPb1owS1hNY0IraEh1dmxrbEhudHZjSTBtTU1RMG1oWWo2cXRNRlN0a0YxUnBDRzNJUGRJd3BWQ1FxdThHVjcKczh1YmtuUnpzKzNDL0JtMTlSRk9vaVBwRGt3dnlOZnZtUTE0WGt5cXFLSzVvWjh6aEQzMmtGUlFreGE4dVpTdQpoNGFUSW1GeGtudTM5d2FCeElSWEU0akt4bEFtUWM0UWpGWm9xMUttUXFRZzBKLzFKRjhSbEZ2SmFzMVZjakx2CllsdlVCMnQ2bnBPNm9RakIzbCtQTmYwRHBRSDdpVXgzV3o1QWpRQ2k2TDI1Rmp5RTA2cTZCWi9RbG10WWRsLzgKWllhbzRTUnFQRXMvNmNBaUYrUWY1emcyVWthV3REcGhsMUxLTXVUTkxvdHZzWDk5SFA2OVYyZmFOeWVnb2RRMApMeVRBcHIvdlQwMVlQRTQ2dk5zRExnSys0Y0w2VHJ6Qy9hNFdjbUY1U1JKOTM4enJ2L2R1SkhMWFFJa3U1djArCkV3T3k1OUhkbTBQVC9Fci84NGREVjBDU2pkUi8yWHVaTTNrcHlzU0tMZ0QxY0tpREErSVJndU9EQ3hmTzljeVkKSWc0NnY5bUZtQnZ5SDA0PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==")
	keyBytes, _ := base64.StdEncoding.DecodeString("LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSUNLMXRQNHVtTzRoMW4vUzBtWFF3SWZhbXBNcG1JM2JHSDhvZ1BrOElTM0RvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFdDM0NzRlT1RNZWRMWlRGTklxRS9NU0hHckk2OWxrL2xnMHdBYU9yVUc2RkFJdk0zWUhyagp6MndCMEpDcFVncDAyQzFIWmhyMThhN2VpN3NodUdTYUN3PT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=")

	if (len(crtBytes) == 0) || (len(keyBytes) == 0) {
		return m.NewError("empty crt or key")
	}
	m.Infof("migrating certificate for domain:\n%s", string(keyBytes))

	certName := fmt.Sprintf("lets-%s-2", name(domain))
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
			op2, err := m.certMgr.UpdateCertificateMapEntry(context.Background(), &certificatemanagerpb.UpdateCertificateMapEntryRequest{
				CertificateMapEntry: &certificatemanagerpb.CertificateMapEntry{
					Name: fmt.Sprintf("%s/certificateMaps/%s/certificateMapEntries/%s", m.cmParent, m.config.CertificateMapName, name(domain)),
					Match: &certificatemanagerpb.CertificateMapEntry_Hostname{
						Hostname: domain,
					},
					Certificates: []string{fmt.Sprintf("%s/certificates/%s", m.cmParent, certName)},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"certificates"}},
			})
			//op2, err := m.certMgr.CreateCertificateMapEntry(ctx, &certificatemanagerpb.CreateCertificateMapEntryRequest{
			//	Parent:                fmt.Sprintf("%s/certificateMaps/%s", m.cmParent, m.config.CertificateMapName),
			//	CertificateMapEntryId: name(domain),
			//	CertificateMapEntry: &certificatemanagerpb.CertificateMapEntry{
			//		Name: fmt.Sprintf("%s/certificateMaps/%s/certificateMapEntries/%s", m.cmParent, m.config.CertificateMapName, name(domain)),
			//		Match: &certificatemanagerpb.CertificateMapEntry_Hostname{
			//			Hostname: domain,
			//		},
			//		Certificates: []string{fmt.Sprintf("%s/certificates/%s", m.cmParent, certName)},
			//	},
			//})
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
		ok, _ := m.checkCname(domain, m.cnames)
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
		ok, _ := m.checkCname(domain, m.cnames)
		if ok {
			m.Infof("[%s] certificate map entry: %+v", domain, cm)
			alreadyExists, err2 := m.IssueGoogleCert(domain, cm, nil)
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
	return strings.ReplaceAll(strings.ReplaceAll(domain, ".", "-"), "*", "wildcard")
}

func (m *Manager) IssueGoogleCert(domain string, mapEntry *certificatemanagerpb.CertificateMapEntry, dnsAuth *certificatemanagerpb.DnsAuthorization) (bool, error) {
	ctx := context.Background()
	cert, _ := m.certMgr.GetCertificate(ctx, &certificatemanagerpb.GetCertificateRequest{Name: fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain))})
	if cert == nil {
		managedCert := certificatemanagerpb.Certificate_ManagedCertificate{
			Domains: []string{domain},
		}
		if dnsAuth != nil {
			managedCert.DnsAuthorizations = []string{dnsAuth.Name}
		}
		m.Infof("[%s] creating google certificate", domain)
		op, err := m.certMgr.CreateCertificate(ctx, &certificatemanagerpb.CreateCertificateRequest{
			Parent:        m.cmParent,
			CertificateId: name(domain),
			Certificate: &certificatemanagerpb.Certificate{
				Name: fmt.Sprintf("%s/certificates/%s", m.cmParent, name(domain)),
				Type: &certificatemanagerpb.Certificate_Managed{
					Managed: &managedCert,
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

type ExtendedDomainStatus struct {
	Status DomainStatus  `json:"status"`
	Cnames []CnameStatus `json:"cnames"`
	Error  error         `json:"error"`
}

type CnameStatus struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Ok    bool   `json:"ok"`
}

const (
	DomainStatusError       DomainStatus = "error"
	DomainStatusCNAME       DomainStatus = "dns_error"
	DomainStatusOK          DomainStatus = "ok"
	DomainStatusIssuingCert DomainStatus = "pending_ssl"
)

func (m *Manager) AddDomain(domain string) (status DomainStatus, err error) {
	m.Infof("[%s] adding domain...", domain)
	// first check that domain leads to the cna e
	cname, _ := m.checkCname(domain, m.cnames)
	if !cname {
		return DomainStatusCNAME, nil
	}
	m.Infof("[%s] cname is OK", domain)

	alreadyExists, err := m.IssueGoogleCert(domain, nil, nil)
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

func (m *Manager) AddWildcardDomain(domain string) (status ExtendedDomainStatus) {
	m.Infof("[%s] adding wildcard domain...", domain)
	specificDomain := strings.ReplaceAll(domain, "*", "wildcard")
	// first check that domain leads to the cna e
	cname, _ := m.checkCname(specificDomain, m.cnames)
	if !cname {
		status.Status = DomainStatusCNAME
		status.Cnames = []CnameStatus{{Name: domain, Value: m.cnames[0], Ok: false}}
	} else {
		status.Cnames = []CnameStatus{{Name: domain, Value: m.cnames[0], Ok: true}}
	}
	dnsAuth, err := m.CreateDnsAuthorization(domain)
	if err != nil {
		status.Status = DomainStatusError
		status.Error = err
		return status
	}
	dnsAuthOk, _ := m.checkCname(dnsAuth.DnsResourceRecord.Name, []string{strings.TrimSuffix(dnsAuth.DnsResourceRecord.Data, ".")})
	if !dnsAuthOk {
		status.Status = DomainStatusCNAME
		status.Cnames = append(status.Cnames, CnameStatus{Name: strings.TrimSuffix(dnsAuth.DnsResourceRecord.Name, "."), Value: strings.TrimSuffix(dnsAuth.DnsResourceRecord.Data, "."), Ok: false})
		return status
	} else {
		status.Cnames = append(status.Cnames, CnameStatus{Name: strings.TrimSuffix(dnsAuth.DnsResourceRecord.Name, "."), Value: strings.TrimSuffix(dnsAuth.DnsResourceRecord.Data, "."), Ok: true})
		if !cname {
			return status
		}
	}
	m.Infof("[%s] cname is OK", domain)
	alreadyExists, err := m.IssueGoogleCert(domain, nil, dnsAuth)
	if err != nil {
		m.Errorf("[%s] error issuing google certificate: %v", domain, err)
		status.Status = DomainStatusError
		status.Error = err
		return status
	}
	if !alreadyExists {
		// domain was just added to ingress, so it's pending
		status.Status = DomainStatusIssuingCert
		return status
	}

	certStatus, err := m.checkCertificate(domain)
	switch certStatus {
	case CertificateStatusError:
		m.Errorf("[%s] check certificate error: %v", domain, err)
		status.Status = DomainStatusError
		status.Error = err
		return status
	case CertificateStatusPending:
		m.Infof("[%s] issuing certificate", domain)
		status.Status = DomainStatusIssuingCert
		return status
	case CertificateStatusOK:
		m.Infof("[%s] certificate is OK.", domain)
		status.Status = DomainStatusOK
		return status
	default:
		m.Errorf("[%s] unknown certificate status: %v", domain, certStatus)
		status.Status = DomainStatusError
		status.Error = fmt.Errorf("unknown certificate status: %v", certStatus)
		return status
	}
}

func (m *Manager) CreateDnsAuthorization(domain string) (*certificatemanagerpb.DnsAuthorization, error) {
	ctx := context.Background()
	dnsAuthDomain := strings.TrimPrefix(domain, "*.")
	dns, _ := m.certMgr.GetDnsAuthorization(ctx, &certificatemanagerpb.GetDnsAuthorizationRequest{
		Name: fmt.Sprintf("%s/dnsAuthorizations/%s", m.cmParent, name(dnsAuthDomain)),
	})
	if dns == nil {
		op, err := m.certMgr.CreateDnsAuthorization(ctx, &certificatemanagerpb.CreateDnsAuthorizationRequest{
			Parent:             m.cmParent,
			DnsAuthorizationId: name(dnsAuthDomain),
			DnsAuthorization: &certificatemanagerpb.DnsAuthorization{
				Name:   fmt.Sprintf("%s/dnsAuthorizations/%s", m.cmParent, name(dnsAuthDomain)),
				Domain: dnsAuthDomain,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("error creating dns authorization: %v", err)
		}
		dnsAuthorization, err := op.Wait(ctx)
		if err != nil {
			return nil, fmt.Errorf("error creating dns authorization: %v", err)
		}
		m.Infof("dns authorization created: %s", dnsAuthorization.DnsResourceRecord.String())
		return dnsAuthorization, nil
	} else {
		m.Infof("dns authorization already exists: %s", dns.DnsResourceRecord.String())
		return dns, nil
	}
}

type CertificateStatus string

const (
	CertificateStatusError   CertificateStatus = "error"
	CertificateStatusPending CertificateStatus = "pending"
	CertificateStatusOK      CertificateStatus = "ok"
)

func (m *Manager) checkCname(domain string, cnames []string) (ok bool, err error) {
	currentDomain := domain
	for i := 0; i < 10; i++ {
		lookupName := currentDomain
		currentDomain, err = net.LookupCNAME(lookupName)
		if err != nil {
			m.Warnf("[%s] error looking up domain: %v", domain, err)
			return false, err
		}
		currentDomain = strings.TrimSuffix(currentDomain, ".")
		if slices.Contains(cnames, currentDomain) {
			return true, nil
		} else if currentDomain == lookupName {
			// no more cnames
			break
		}
	}
	m.Warnf("[%s] incorrect CNAME record: %s", domain, currentDomain)
	return false, nil
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
