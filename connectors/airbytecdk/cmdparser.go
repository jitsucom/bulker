package airbyte

import (
	"encoding/json"
	"fmt"
	"os"
)

func getSourceConfigPath() (string, error) {
	if os.Args[2] != "--config" {
		return "", fmt.Errorf("expect --config")
	}
	return os.Args[3], nil
}

func getStatePath() (string, error) {
	if len(os.Args) <= 6 {
		return "", nil
	}
	if os.Args[6] != "--state" {
		return "", fmt.Errorf("expect --state")
	}
	return os.Args[7], nil
}

func getCatalogPath() (string, error) {
	if os.Args[4] != "--catalog" {
		return "", fmt.Errorf("expect --catalog")
	}
	return os.Args[5], nil
}

// UnmarshalFromPath is used to unmarshal json files into respective struct's
// this is most commonly used to unmarshal your State between runs and also unmarshal SourceConfig's
//
// Example usage
//
//	 type CustomState struct {
//		 Timestamp int    `json:"timestamp"`
//		 Foobar    string `json:"foobar"`
//	 }
//
//	 func (s *CustomSource) Read(stPath string, ...) error {
//		 var cs CustomState
//		 err = airbyte.UnmarshalFromPath(stPath, &cs)
//		 if err != nil {
//			 // handle error
//		 }
//	 	 // cs is populated
//	  }
func UnmarshalFromPath(path string, v interface{}) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}
