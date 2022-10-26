package app

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/base/logging"
	"io"
	"net/http"
	"strings"
)

const workspaceId = "ildar"

type Router struct {
	engine       *gin.Engine
	authTokens   []string
	tokenSecrets []string
}

func NewRouter(config *AppConfig, repository *Repository, topicManager *TopicManager, producer *Producer) *Router {
	authTokens := strings.Split(config.AuthTokens, ",")
	if len(authTokens) == 1 && authTokens[0] == "" {
		authTokens = nil
		logging.Warn("⚠️ No auth tokens provided. All requests will be allowed")
	}
	tokenSecrets := strings.Split(config.TokenSecrets, ",")

	router := &Router{authTokens: authTokens, tokenSecrets: tokenSecrets}
	engine := gin.New()
	engine.Use(router.AuthMiddleware)
	engine.POST("/load/:destinationId", func(c *gin.Context) {
		destinationId := c.Param("destinationId")
		tableName := c.Query("tableName")
		destinationId = fmt.Sprintf("%s_%s", workspaceId, destinationId)

		destination := repository.GetDestination(destinationId)
		if destination == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "destination not found"})
			return
		}
		topicId := destination.TopicId(tableName)
		topics := topicManager.GetTopics(destinationId)
		if !topics.Contains(topicId) {
			logging.Infof("Topic %s not found for destination %s. Creating one", topicId, destinationId)
			err := topicManager.CreateTopic(destination, tableName)
			if err != nil {
				err = fmt.Errorf("couldn't create topic: %s : %w", topicId, err)
				logging.Error(err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Printf("error reading HTTP body: %v\n", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("error reading HTTP body: %w", err).Error()})
			return
		}
		err = producer.ProduceAsync(topicId, body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("couldn't produce message for kafka topic: %s : %w", topicId, err).Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "ok"})
	})
	router.engine = engine
	return router
}

// GetEngine returns gin router
func (r *Router) GetEngine() *gin.Engine {
	return r.engine
}

func (r *Router) AuthMiddleware(c *gin.Context) {
	if len(r.authTokens) == 0 {
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
				//a := hashToken(token, salt, secret)
				//logging.Debugf("Hashed token: %s. Hash: %s ", a, hash)
				if hashToken(token, salt, secret) == hash {
					//logging.Debugf("Token %s is valid", token)
					return
				}
			}
		}
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + token})
	return
}

func hashToken(token string, salt string, secret string) string {
	//logging.Infof("Hashing token: %s. Salt: %s. Secret: %s", token, salt, secret)
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	return base64.RawStdEncoding.EncodeToString(hash.Sum(nil))
}
