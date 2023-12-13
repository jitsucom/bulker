package main

import "strings"

type AnalyticsServerEvent map[string]any

func (e *AnalyticsServerEvent) GetS(key string) string {
	str, ok := (*e)[key].(string)
	if !ok {
		return ""
	}
	return str
}

func (e *AnalyticsServerEvent) GetPathS(path string) string {
	p := strings.Split(path, ".")
	var obj map[string]any
	obj = *e
	for i, key := range p {
		if i == len(p)-1 {
			str, ok := obj[key].(string)
			if !ok {
				return ""
			}
			return str
		}
		var ok bool
		obj, ok = obj[key].(map[string]any)
		if !ok {
			return ""
		}
	}
	return ""
}

//
//type PageReservedProps struct {
//	Path            string `json:"path,omitempty"`
//	Referrer        string `json:"referrer,omitempty"`
//	ReferringDomain string `json:"referring_domain,omitempty"`
//	Host            string `json:"host,omitempty"`
//	Search          string `json:"search,omitempty"`
//	Title           string `json:"title,omitempty"`
//	Url             string `json:"url,omitempty"`
//}
//
//type Library struct {
//	Name    string `json:"name,omitempty"`
//	Version string `json:"version,omitempty"`
//}
//
//type Traits struct {
//	CrossDomainId string `json:"crossDomainId,omitempty"`
//}
//
//type Campaign struct {
//	Name    string `json:"name,omitempty"`
//	Term    string `json:"term,omitempty"`
//	Source  string `json:"source,omitempty"`
//	Medium  string `json:"medium,omitempty"`
//	Content string `json:"content,omitempty"`
//}
//
//type Referrer struct {
//	Btid string `json:"btid,omitempty"`
//	Urid string `json:"urid,omitempty"`
//}
//
//type Amp struct {
//	Id string `json:"id,omitempty"`
//}
//
//type Ga4 struct {
//	ClientId   string         `json:"clientId,omitempty"`
//	SessionIds map[string]any `json:"sessionIds,omitempty"`
//}
//
//type ClientIds struct {
//	Ga4 Ga4    `json:"ga4,omitempty"`
//	Fbc string `json:"fbc,omitempty"`
//	Fbp string `json:"fbp,omitempty"`
//}

//type AnalyticsContext map[string]any

//type AnalyticsContext struct {
//	Ip string `json:"ip"`
//
//	Page PageReservedProps `json:"page,omitempty"`
//
//	Metrics []map[string]any `json:"metrics,omitempty"`
//
//	UserAgent string `json:"userAgent,omitempty"`
//
//	UserAgentVendor string `json:"userAgentVendor,omitempty"`
//
//	Locale string `json:"locale,omitempty"`
//
//	Library Library `json:"library,omitempty"`
//
//	Traits Traits `json:"traits,omitempty"`
//
//	Campaign Campaign `json:"campaign,omitempty"`
//
//	Referrer Referrer `json:"referrer,omitempty"`
//
//	Amp Amp `json:"amp,omitempty"`
//
//	ClientIds ClientIds `json:"clientIds,omitempty"`
//
//	UnknownField map[string]json.RawMessage `json:",inline,omitempty"`
//}

//type ServerContext struct {
//	RequestIp  string `json:"request_ip,omitempty"`
//	ReceivedAt string `json:"receivedAt,omitempty"`
//}
//
//type ProcessingContext struct {
//	Table string `json:"$table,omitempty"`
//}

//
//type AnalyticsServerEvent struct {
//	ServerContext `json:",inline,omitempty"`
//
//	ProcessingContext `json:",inline,omitempty"`
//
//	MessageId  string         `json:"messageId,omitempty"`
//	Timestamp  string         `json:"timestamp,omitempty"`
//	Type       string         `json:"type,omitempty"`
//	Category   string         `json:"category,omitempty"`
//	Name       string         `json:"name,omitempty"`
//	Properties map[string]any `json:"properties,omitempty"`
//	Traits     map[string]any `json:"traits,omitempty"`
//
//	Context AnalyticsContext `json:"context,omitempty"`
//
//	UserId      string `json:"userId,omitempty"`
//	AnonymousId string `json:"anonymousId,omitempty"`
//	GroupId     string `json:"groupId,omitempty"`
//	PreviousId  string `json:"previousId,omitempty"`
//
//	Event    string `json:"event,omitempty"`
//	WriteKey string `json:"writeKey,omitempty"`
//	SentAt   string `json:"sentAt,omitempty"`
//
//	UnknownField map[string]json.RawMessage `json:",inline,omitempty"`
//}
