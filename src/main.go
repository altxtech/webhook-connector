package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/types/known/timestamppb"

	conf "github.com/altxtech/webhook-connector/src/configurations"
	"github.com/altxtech/webhook-connector/src/database"
	"github.com/altxtech/webhook-connector/src/sink"
	"github.com/altxtech/webhook-connector/src/model"
	"github.com/altxtech/webhook-connector/src/utils"
)

// API Interface
type CreateConfigRequest struct {
	// Mirrors the conf.Configuration object
	Sink struct  {
		Type string `json:"type"`
		Config json.RawMessage
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
	var request CreateConfigRequest
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

	c.IndentedJSON(http.StatusOK, idConfig)
	return
}

func ConfigFromRequest(request CreateConfigRequest) (conf.Configuration, error) {

	var config conf.Configuration

	// Identify Sink type
	var sinkConfig conf.SinkConfig
	switch request.Sink.Type {
		case "jsonl":
			sinkConfig = &conf.JSONLSinkConfig{}
		case "bigquery":
			sinkConfig = &conf.BigQuerySinkConfig{}
		default:
			return config, fmt.Errorf("Invalid sink type %v", request.Sink.Type)
	}

	// Validate sink config
	err := json.Unmarshal(request.Sink.Config, sinkConfig)
	if err != nil {
		return config, fmt.Errorf("Failed to read confg for sink of type %s", request.Sink.Type)
	}

	err = sinkConfig.Validate()
	if err != nil {
		return config, fmt.Errorf("Invalid configuration for sink of type %s: %v", request.Sink.Type, err)
	}

	// Create new Config
	sink := conf.NewSink(request.Sink.Type, sinkConfig)
	newConfig := conf.NewConfiguration(sink)

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
	var request CreateConfigRequest
	err := c.ShouldBindJSON(&request)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, NewAPIErrorResponse("Invalid request body."))
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

	// Update on database
	id := c.Param("id")
	updatedConfig.SetID(id)
	result, err := db.UpdateConfig(updatedConfig)
	if err != nil {
		message := fmt.Sprintf("Error Updating configurations: %v", err)
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
	_, err := db.GetConfigByID(c.Param("id"))
	if err != nil {
		message := fmt.Sprintf("Config with id %s not found.", c.Param("id"))
		response := NewAPIErrorResponse(message)
		c.IndentedJSON(http.StatusNotFound, response)
		return
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

	// NEXT: Get the sink for this configuration

}


// Initialize database
func initDB() database.Database {
	return database.NewInMemoryDB()
}
var db database.Database = initDB()

var sinkManager map[string]sink.Sink = map[string]sink.Sink{}

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
