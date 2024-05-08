package main

import (
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"strings"
)

func SatisfyFilter(filter, subject string) bool {
	return filter == "*" || strings.TrimSpace(strings.ToLower(filter)) == strings.TrimSpace(strings.ToLower(subject))
}

func SatisfyDomainFilter(filter, subject string) bool {
	if filter == "*" {
		return true
	}
	if strings.HasPrefix(filter, "*.") {
		return strings.HasSuffix(subject, filter[1:])
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
		return SatisfyDomainFilter(f, event.GetPathS("context.page.host"))
	}) && (utils.ArrayContainsF(eventsArray, func(f string) bool {
		return SatisfyFilter(f, event.GetS("type"))
	}) || utils.ArrayContainsF(eventsArray, func(f string) bool {
		return SatisfyFilter(f, event.GetS("event"))
	}))
}
