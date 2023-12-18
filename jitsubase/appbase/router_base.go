package appbase

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"net/http"
	"strings"
)

type Router struct {
	Service
	engine       *gin.Engine
	authTokens   []string
	tokenSecrets []string
	noAuthPaths  utils.Set[string]
}

func NewRouterBase(authTokens, tokenSecrets, noAuthPaths []string) *Router {
	base := NewServiceBase("router")
	if len(authTokens) == 1 && authTokens[0] == "" {
		authTokens = nil
		base.Warnf("⚠️ No auth tokens provided. All requests will be allowed")
	}

	router := &Router{
		Service:      base,
		authTokens:   authTokens,
		tokenSecrets: tokenSecrets,
		noAuthPaths:  utils.NewSet(noAuthPaths...),
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(router.authMiddleware)
	router.engine = engine
	return router
}

// Engine returns gin router
func (r *Router) Engine() *gin.Engine {
	return r.engine
}

func (r *Router) authMiddleware(c *gin.Context) {
	if len(r.authTokens) == 0 {
		return
	}
	if r.noAuthPaths.Contains(c.FullPath()) {
		//no auth for this path
		return
	}
	authorizationHeader := c.GetHeader("Authorization")
	token := strings.TrimPrefix(authorizationHeader, "Bearer ")
	if token == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header with Bearer token is required"})
		return
	}
	for _, authToken := range r.authTokens {
		if !strings.Contains(authToken, ".") {
			if token == authToken {
				//logging.Debugf("Token %s is valid", token)
				return
			}
		} else {
			hashedToken := strings.Split(authToken, ".")
			salt := hashedToken[0]
			hash := hashedToken[1]
			for _, secret := range r.tokenSecrets {
				//a := HashToken(token, salt, secret)
				//logging.Debugf("Hashed token: %s. Hash: %s ", a, hash)
				if HashToken(token, salt, secret) == hash {
					//logging.Debugf("Token %s is valid", token)
					return
				}
			}
		}
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + token})
	return
}

func (r *Router) ResponseError(c *gin.Context, code int, errorType string, maskError bool, err error, logPrefix string, sendResponse bool) *RouterError {
	routerError := RouterError{ErrorType: errorType}
	if err != nil {
		if maskError {
			errorID := uuid.NewLettersNumbers()
			err = fmt.Errorf("error# %s: %s: %v", errorID, errorType, err)
			routerError.PublicError = fmt.Errorf("error# %s: %s", errorID, errorType)
		} else {
			err = fmt.Errorf("%s: %v", errorType, err)
			routerError.PublicError = err
		}
	} else {
		err = fmt.Errorf(errorType)
		routerError.PublicError = err
	}
	routerError.Error = err
	logFormat := utils.JoinNonEmptyStrings(" ", logPrefix, "%v")
	r.Errorf(logFormat, err)
	if sendResponse {
		c.JSON(code, gin.H{"error": routerError.PublicError.Error()})
	}
	return &routerError
}

func (r *Router) ShouldCompress(req *http.Request) bool {
	if !strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") ||
		strings.Contains(req.Header.Get("Connection"), "Upgrade") ||
		strings.Contains(req.Header.Get("Accept"), "text/event-stream") {
		return false
	}

	return true
}

func HashToken(token string, salt string, secret string) string {
	//logging.Infof("Hashing token: %s. Salt: %s. Secret: %s", token, salt, secret)
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	return base64.RawStdEncoding.EncodeToString(hash.Sum(nil))
}

type RouterError struct {
	Error       error
	PublicError error
	ErrorType   string
}
