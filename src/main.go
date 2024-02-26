package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/altxtech/webhook-connector/src/database"
	"github.com/altxtech/webhook-connector/src/model"
)

// API Interface
type APIErrorResponse struct {
	Error string `json:"error"`
}

func NewAPIErrorResponse(error string) APIErrorResponse {
	return APIErrorResponse{Error: error}
}

type CreateConfigRequest struct {
	ProjectID string `json:"project_id"`
	Dataset   string `json:"dataset"`
	Table     string `json:"table"`
}

func helloWorld(c *gin.Context) {
	c.String(http.StatusOK, "Hello webhook connector!")
}

func ingestWebhook(c *gin.Context) {
	// 1. Fetch the configuration settings from the database
	// 2. Create the webhookEvent object
	// 3. Write to bigquery
	event := model.WebhookEvent{}
	log.Println(event)
}

// Handlers
// Configurations
func CreateConfig(c *gin.Context) {

	// Read and validate request
	var request CreateConfigRequest
	err := c.ShouldBindJSON(&request)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, NewAPIErrorResponse("Invalid Configuration object"))
		return
	}

	// Create configuration object
	newConfig := database.NewConfiguration(request.ProjectID, request.Dataset, request.Table)

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
	id := c.Param("id")
	updatedConfig := database.NewConfiguration(request.ProjectID, request.Dataset, request.Table)

	// Update on database
	result, err := db.UpdateConfig(id, updatedConfig)
	c.IndentedJSON(http.StatusOK, result)
	return
}

// Initialize database
func initDB() database.Database {
	return database.NewInMemoryDB()
}

var db database.Database = initDB()

func main() {
	router := gin.Default()
	router.GET("/hello-world", helloWorld)

	// Configurations
	router.POST("/configurations", CreateConfig)
	router.GET("/configurations", ListConfigs)
	router.GET("/configurations/:id", GetConfig)
	router.PUT("/configurations/:id", UpdateConfig)

	router.POST("/ingest/:configId", ingestWebhook)

	router.Run()
}
