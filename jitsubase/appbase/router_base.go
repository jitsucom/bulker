package appbase

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"net/http"
	"regexp"
	"strings"
)

const ContextLoggerName = "contextLogger"
const ContextDomain = "contextDomain"
const ContextMessageId = "contextMessageId"

var EmptyGif = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0x21,
	0xF9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3B,
}

var IsHexRegex = regexp.MustCompile(`^[a-fA-F0-9]+$`)

type Router struct {
	Service
	engine        *gin.Engine
	authTokens    []string
	rawAuthTokens []string
	tokenSecrets  []string
	noAuthPaths   types.Set[string]
}

func NewRouterBase(config Config, noAuthPaths []string) *Router {
	authTokens := strings.Split(config.AuthTokens, ",")
	rawAuthTokens := strings.Split(config.RawAuthTokens, ",")
	tokenSecrets := strings.Split(config.TokenSecrets, ",")
	base := NewServiceBase("router")
	if config.AuthTokens == "" {
		authTokens = nil
	}
	if config.RawAuthTokens == "" {
		rawAuthTokens = nil
	}
	if authTokens == nil && rawAuthTokens == nil {
		base.Warnf("⚠️ No auth tokens provided. All requests will be allowed")
	}
	for _, authToken := range authTokens {
		if !strings.Contains(authToken, ".") {
			base.Fatalf("Invalid auth token: %s Should be in format ${salt}.${hash} or %sRAW_AUTH_TOKENS config variable must be used instead", authToken, config.AppSetting.EnvPrefixWithUnderscore())
		}
	}

	router := &Router{
		Service:       base,
		authTokens:    authTokens,
		rawAuthTokens: rawAuthTokens,
		tokenSecrets:  tokenSecrets,
		noAuthPaths:   types.NewSet(noAuthPaths...),
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(router.authMiddleware)
	engine.RemoveExtraSlash = true
	router.engine = engine
	return router
}

// Engine returns gin router
func (r *Router) Engine() *gin.Engine {
	return r.engine
}

func (r *Router) authMiddleware(c *gin.Context) {
	if len(r.authTokens) == 0 && len(r.rawAuthTokens) == 0 {
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
		hashedToken := strings.Split(authToken, ".")
		salt := hashedToken[0]
		hash := hashedToken[1]
		hex := IsHexRegex.MatchString(hash)
		for _, secret := range r.tokenSecrets {
			if hex {
				if HashTokenHex(token, salt, utils.Nvl(secret, DefaultSeed)) == hash {
					return
				}
			} else if HashTokenBase64(token, salt, secret) == hash {
				//logging.Debugf("Token %s is valid", token)
				return
			}
		}
	}
	for _, rawAuthToken := range r.rawAuthTokens {
		if token == rawAuthToken {
			return
		}
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + token})
	return
}

func (r *Router) ResponseError(c *gin.Context, code int, errorType string, maskError bool, err error, sendResponse bool) *RouterError {
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
	builder := strings.Builder{}
	loggerName, ok := c.Get(ContextLoggerName)
	if ok {
		builder.WriteString(fmt.Sprintf("[%s]", loggerName))
	}
	messageId, ok := c.Get(ContextMessageId)
	if ok {
		builder.WriteString(fmt.Sprintf("[messageId: %s]", messageId))
	}
	domain, ok := c.Get(ContextDomain)
	if ok {
		builder.WriteString(fmt.Sprintf("[domain: %s]", domain))
	}
	logFormat := utils.JoinNonEmptyStrings(" ", builder.String(), "%v")
	r.Errorf(logFormat, err)
	if sendResponse {
		if c.FullPath() == "/api/px/:tp" {
			c.Header("X-Jitsu-Error", routerError.PublicError.Error())
			c.Data(http.StatusOK, "image/gif", EmptyGif)
		} else {
			c.JSON(code, gin.H{"error": routerError.PublicError.Error()})
		}
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

// Deprecated: Use HashTokenHex. Not sure how we started to use base64 encoding for hash.
// But for compatibility with previous release we must keep it for a while
func HashTokenBase64(token string, salt string, secret string) string {
	//logging.Infof("Hashing token: %s. Salt: %s. Secret: %s", token, salt, secret)
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	return base64.RawStdEncoding.EncodeToString(hash.Sum(nil))
}

func HashTokenHex(token string, salt string, secret string) string {
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	res := hash.Sum(nil)
	return fmt.Sprintf("%x", res)
}

type RouterError struct {
	Error       error
	PublicError error
	ErrorType   string
}
