package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	conf "github.com/altxtech/webhook-connector/src/configurations"
	"github.com/altxtech/webhook-connector/src/database"
	"github.com/altxtech/webhook-connector/src/model"
	"github.com/altxtech/webhook-connector/src/sink"
	"github.com/altxtech/webhook-connector/src/utils"
)

// Initialization
func initDB() (database.Database){
	db, err := database.NewFirestoreDatabase(context.Background(), os.Getenv("DATABASE_ID"))
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	return db
}
var db database.Database = initDB()


// API Interface
type ConfigOperationRequest struct {
	// Request for creating new configurations and updating new ones
	Name string `json:"name"`
	UseKey bool `json:"use_key"`
	Sink struct  {
		Type string `json:"type"`
		Config map[string]interface{}
	} `json:"sink"`
}

type ConfigCreationRestul struct {
	// Request for creating new configurations and updating new ones
	ID string `json:"id"`
	Name string `json:"name"`
	UseKey bool `json:"use_key"`
	Key string `json:"key"`
	Sink struct  {
		Type string `json:"type"`
		Config map[string]interface{}
	} `json:"sink"`
}

type APIErrorResponse struct {
	Error string `json:"error"`
}

func NewAPIErrorResponse(error string) APIErrorResponse {
	return APIErrorResponse{Error: error}
}

func helloWorld(c *gin.Context) {
	c.String(http.StatusOK, "Hello webhook connector!")
}

// Handlers
// Configurations
func CreateConfig(c *gin.Context) {

	// Read and validate request
	var request ConfigOperationRequest
	err := c.ShouldBindJSON(&request)
	if err != nil {
		message := fmt.Sprintf("Invalid configuration object: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// Create the config object 
	newConfig, err := ConfigFromRequest(request)
	if err != nil {
		message := fmt.Sprintf("Error creating configuration object: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}
	
	// Insert into database
	idConfig, err := db.InsertConfig(newConfig)
	if err != nil {
		message := fmt.Sprintf("Failed to insert configuration into database: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	result := &ConfigCreationRestul{
		ID: idConfig.ID,
		Name: idConfig.Name,
		UseKey: idConfig.UseKey,
		Sink: struct{Type string "json:\"type\""; Config map[string]interface{}}(idConfig.Sink),
	}

	// Create a webhook key
	if request.UseKey {
		key, err := GenerateRandomString(24)
		if err != nil {
			message := fmt.Sprintf("Failed to generate webhook key: %v", err)
			response := NewAPIErrorResponse(message)
			c.IndentedJSON(http.StatusInternalServerError, response)
			return
		}
		hash := CreateKeyHashPair(idConfig.ID, key)
		idConfig.SetKeyHash(hash)
		_, err = db.UpdateConfig(idConfig)
		if err != nil {
			message := fmt.Sprintf("Failed to generate webhook key: %v", err)
			response := NewAPIErrorResponse(message)
			c.IndentedJSON(http.StatusInternalServerError, response)
			return
		}
		result.Key = key
	}


	c.IndentedJSON(http.StatusOK, result)
	return
}

func ConfigFromRequest(request ConfigOperationRequest) (conf.Configuration, error) {

	var newConfig conf.Configuration

	// Create Sink config
	sinkConf, err := conf.NewSink(request.Sink.Type, request.Sink.Config)
	if err != nil {
		return newConfig, fmt.Errorf("Failed to process sink configuration: %v", err)
	}
	newConfig = conf.NewConfiguration(request.Name, sinkConf, request.UseKey)

	return newConfig, nil
}

// List configs
func ListConfigs(c *gin.Context) {
	configs, err := db.ListConfigs()
	if err != nil {
		message := fmt.Sprintf("Failed to retrieve configurations: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusInternalServerError, response)
		return
	}

	c.IndentedJSON(http.StatusOK, configs)
}


// GenerateRandomString generates a random string of the specified length.
func GenerateRandomString(length int) (string, error) {
    // Determine the number of bytes needed to represent the random string
    numBytes := length / 2
    
    // Create a byte slice to store the random bytes
    randomBytes := make([]byte, numBytes)
    
    // Read random bytes from the crypto/rand package
    _, err := rand.Read(randomBytes)
    if err != nil {
        return "", err
    }
    
    // Convert the random bytes to a hexadecimal string
    randomString := hex.EncodeToString(randomBytes)
    
    // Truncate the string to the desired length
    if len(randomString) > length {
        randomString = randomString[:length]
    }
    
    return randomString, nil
}


// CreateKeyHashPair generates a salted hash for a given key.
func CreateKeyHashPair(salt string, key string) (string) {
    // Concatenate the salt and key
    data := []byte(salt + key)
    
    // Generate the hash
    hash := sha256.Sum256(data)
    
    // Convert the hash to a hexadecimal string
    hashString := hex.EncodeToString(hash[:])
    
    return hashString
}

// CheckKey verifies if the provided key matches the given hash and salt.
func CheckKey(salt string, key string, keyHash string) bool {
    // Recalculate the hash using the provided key and salt
    newHash := CreateKeyHashPair(salt, key)
    
    // Compare the recalculated hash with the provided hash
    return newHash == keyHash
}


func GetConfig(c *gin.Context) {
	id := c.Param("id")
	config, err := db.GetConfigByID(id)
	if err != nil {
		response := NewAPIErrorResponse(fmt.Sprintf("Configuration with id %s not found.", id))
		c.IndentedJSON(http.StatusNotFound, response)
		return
	}

	c.IndentedJSON(http.StatusOK, config)
	return
}

func UpdateConfig(c *gin.Context) {
	var request ConfigOperationRequest
	err := c.ShouldBindJSON(&request)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, NewAPIErrorResponse("Invalid request body."))
		return
	}

	// Retrieve the existing configuration
	id := c.Param("id")
	oldConfig, err := db.GetConfigByID(id)
	if err != nil {
		message := fmt.Sprintf("Error retrieving existing configuration: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}
	
	// Create configuration object
	updatedConfig, err := ConfigFromRequest(request)
	if err != nil {
		message := fmt.Sprintf("Error creating configuration object: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// Set inherited fields from existing config
	updatedConfig.SetID(id)
	updatedConfig.CreatedAt = oldConfig.CreatedAt

	result, err := db.UpdateConfig(updatedConfig)
	if err != nil {
		message := fmt.Sprintf("Error Updating configurations: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// If there is an active sink for this configuration, end it
	_, err = sm.terminateSinkIfExists(id)
	if err != nil {
		message := fmt.Sprintf("Error deleting existing sink for configuration: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	c.IndentedJSON(http.StatusOK, result)
	return
}

func DeleteConfig(c *gin.Context){
	id := c.Param("id")
	deletedConfig, err := db.DeleteConfig(id)
	if err != nil {
		message := fmt.Sprintf("Failed to delete config with id %s: %v", id, err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusNotFound, response)
		return
	}

	c.IndentedJSON(http.StatusOK, deletedConfig)
	return
}


// Ingesting webhooks
func IngestWebhook(c *gin.Context){

	event := model.WebhookEvent{
		Metadata: &model.Metadata{
			ReceivedAt: timestamppb.Now(),
			LoadedAt: timestamppb.Now(), // TODO: Fix. Should be as close as possible to the instante the event is loaded into the sink
		},
	}

	// Validate id exists
	config, err := db.GetConfigByID(c.Param("id"))
	if err != nil {
		message := fmt.Sprintf("Config with id %s not found.", c.Param("id"))
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusNotFound, response)
		return
	}
	event.Metadata.SourceId = config.ID
	event.Metadata.SourceName = config.Name

	// Check authorization
	if config.UseKey {
		// Extract the key
		keyParts := strings.Split(c.Request.Header.Get("Authorization"), " ")
		key := keyParts[len(keyParts) - 1]

		if !CheckKey(config.ID, key, config.KeyHash) {
			response := NewAPIErrorResponse("Unauthorized. Invalid webhook key.")
			c.IndentedJSON(http.StatusUnauthorized, response)
			return
		}
	}
	
	// Read body data
	data, err := io.ReadAll(c.Request.Body) 
	if err != nil {
		message := fmt.Sprintf("Error reading reponse body: %v", err)
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// Check if data is valid json
	if !utils.IsValidJSON(data){
		response := NewAPIErrorResponse("Request body is not valid JSON")
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// Set event data
	event.Event = string(data)

	// Get sink for configuration
	thisSink, err := sm.getSink(&config)	
	if err != nil {
		response := NewAPIErrorResponse(fmt.Sprintf("Failed to get sink for config %s: %v", config.ID, err))
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	// Write rows
	/*
		TODO: We're writing one row at a time.
		We COULD write to a buffer and have the output to the sink be done in batches.
		There are pros and cons of doing it like this. Consider.
	*/
	err = thisSink.WriteRows([]protoreflect.ProtoMessage{&event})
	if err != nil {
		response := NewAPIErrorResponse(fmt.Sprintf("Failed to write rows to sink: %v", err))
		c.IndentedJSON(http.StatusBadRequest, response)
		return
	}

	c.String(http.StatusOK, "Received")
	return
}






// Type to manage sinks
type SinkManager map[string]sink.Sink
func NewSinkManager() SinkManager {
	return SinkManager{}
}
func (sm SinkManager) getSink(config *conf.Configuration) (sink.Sink, error){
	// If the sink for this configuration exists, return it.
	// If not, build it

	var result sink.Sink
	result, ok := sm[config.ID]
	if ok {
		return result, nil
	}

	// Create the appriate sink based on sink
	result, err := sink.NewSink(config.Sink)
	if err != nil {
		return result, fmt.Errorf("Failed to create new sink: %v", err)
	}

	// Register new sink
	sm[config.ID] = result
	return result, nil
}
func (sm SinkManager) terminateSinkIfExists(id string) (sink.Sink, error){

	/*
		Terminate a sink if it exists.
		Returns a copy of the terminated sink.
	*/

	var termSink sink.Sink
	termSink, ok := sm[id]
	if ok {
		err := termSink.Close()
		if err != nil {
			return termSink, fmt.Errorf("Error terminating sink: %v", err)
		}
		delete(sm, id)
	}
	return termSink, nil
}
var sm SinkManager = NewSinkManager()

func main() {
	router := gin.Default()
	router.GET("/hello-world", helloWorld)

	// Configurations
	router.POST("/configurations", CreateConfig)
	router.GET("/configurations", ListConfigs)
	router.GET("/configurations/:id", GetConfig)
	router.PUT("/configurations/:id", UpdateConfig)
	router.DELETE("/configurations/:id", DeleteConfig)

	router.POST("/ingest/:id", IngestWebhook)

	router.Run()
}
