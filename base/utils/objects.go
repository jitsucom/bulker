package utils

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
)

// ParseObject parses struct of any type from input object that can be:
//
// map, json string or yaml string,
//
// already struct of provided type or pointer to it
func ParseObject[K any](inputObject interface{}, result *K) error {
	if result == nil {
		return fmt.Errorf("result variable must be an empty struct of desired type, got nil")
	}
	switch cfg := inputObject.(type) {
	case *K:
		*result = *cfg
	case K:
		*result = cfg
	case map[string]interface{}:
		if err := mapstructure.Decode(cfg, result); err != nil {
			return fmt.Errorf("failed to parse map as %T : %w", result, err)
		}
	case string:
		if len(cfg) == 0 {
			return fmt.Errorf("failed to parse. input string is empty")
		}
		if cfg[0] == '{' {
			if err := json.Unmarshal([]byte(cfg), result); err != nil {
				return fmt.Errorf("failed to parse json as %T : %w", result, err)
			}
		} else {
			if err := yaml.Unmarshal([]byte(cfg), result); err != nil {
				return fmt.Errorf("failed to parse yaml as %T : %w", result, err)
			}
		}
	default:
		return fmt.Errorf("can't parse object from type: %T", cfg)
	}
	return nil
}

func ExtractObject(object interface{}, path ...string) (interface{}, error) {
	mp, ok := object.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected object of type map[string]interface{} got: %T", object)
	}
	last := len(path) == 1
	val, ok := mp[path[0]]
	if !last && (!ok || val == nil) {
		return nil, fmt.Errorf("Failed to reach end of the path. Left path: %s", path)
	}
	if last {
		return val, nil
	}
	return ExtractObject(val, path[1:]...)
}

// Nvl returns first not null object or pointer from varargs
//
// return nil if all passed arguments are nil
func Nvl(args ...interface{}) interface{} {
	for _, str := range args {
		if str != nil {
			return str
		}
	}
	return nil
}

// NvlMap returns first not empty map from varargs
//
// return nil if all passed maps are empty
func NvlMap(args ...map[string]interface{}) map[string]interface{} {
	for _, str := range args {
		if len(str) > 0 {
			return str
		}
	}
	return nil
}
