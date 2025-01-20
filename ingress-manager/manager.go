package main

import (
	"cloud.google.com/go/certificatemanager/apiv1"
	"cloud.google.com/go/certificatemanager/apiv1/certificatemanagerpb"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
