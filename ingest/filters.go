package main

import (
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"strings"
)

func SatisfyFilter(filter, subject string) bool {
	return filter == "*" || strings.TrimSpace(strings.ToLower(filter)) == strings.TrimSpace(strings.ToLower(subject))
}

// SatisfyDomainFilter checks if subject satisfies filter.
// if eager=true -> *.domain.com will match domain.com
func SatisfyDomainFilter(filter, subject string, eager bool) bool {
	if filter == "*" {
		return true
	}
	if strings.HasPrefix(filter, "*.") {
		return strings.HasSuffix(subject, filter[1:]) || (eager && filter[2:] == subject)
	} else {
		return filter == subject
	}
}

func parseFilter(value any) []string {
	switch v := value.(type) {
	case string:
		return strings.Split(v, "\n")
	case []string:
		return v
	case nil:
		return []string{"*"}
	default:
		return []string{}
	}
}

func ApplyFilters(event types.Json, opts map[string]any) bool {
	eventsArray := parseFilter(opts["events"])
	hostsArray := parseFilter(opts["hosts"])

	return utils.ArrayContainsF(hostsArray, func(f string) bool {
		return SatisfyDomainFilter(f, event.GetPathS("context.page.host"), false)
	}) && (utils.ArrayContainsF(eventsArray, func(f string) bool {
		return SatisfyFilter(f, event.GetS("type"))
	}) || utils.ArrayContainsF(eventsArray, func(f string) bool {
		return SatisfyFilter(f, event.GetS("event"))
	}))
}

func ApplyAuthorizedJavaScriptDomainsFilter(domains string, origin string) bool {
	domainRules := strings.Split(domains, ",")
	return utils.ArrayContainsF(domainRules, func(rule string) bool {
		return SatisfyDomainFilter(sanitizeAuthorizedJavaScriptDomain(rule), origin, true)
	})
}

func sanitizeAuthorizedJavaScriptDomain(domain string) string {
	domain = strings.TrimSpace(domain)
	domain, trimmed := strings.CutPrefix(domain, "https://")
	if !trimmed {
		domain = strings.TrimPrefix(domain, "http://")
	}
	domain = strings.TrimSuffix(domain, "/")
	return domain
}
