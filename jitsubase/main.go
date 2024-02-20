package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/jitsucom/bulker/jitsubase/appbase"
)

func main() {
	fmt.Println("Enter token secret:")
	var tokenSecret string
	_, err := fmt.Scanln(&tokenSecret)
	if err != nil {
		panic(err)
	}
	fmt.Println("Enter auth token:")
	var authToken string
	_, err = fmt.Scanln(&authToken)
	if err != nil {
		panic(err)
	}
	salt := uuid.NewString()

	hashedToken := appbase.HashTokenHex(authToken, salt, tokenSecret)
	fmt.Printf("Hashed token: %s.%s", salt, hashedToken)
}
