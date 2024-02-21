package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"log"
	
	"github.com/altxtech/webhook-connector/src/model"
)

type Configuration struct {
	ID string `json:"id" firestore:"id"`
	ProjectID string `json:"project_id" firestore:"id"`
	Dataset string `json:"dataset" firestore:"dataset"`
}

func helloWorld(c *gin.Context){
	c.String(http.StatusOK, "Hello webhook connector!")
}

func ingestWebhook(c *gin.Context){
	// 1. Fetch the configuration settings from the database
	// 2. Create the webhookEvent object
	// 3. Write to bigquery
	event := model.WebhookEvent{}
	log.Println(event)
}

func main() {
	
	router := gin.Default()
	router.GET("/hello-world", helloWorld)

	router.POST("/ingest/:configId", ingestWebhook)

	router.Run()
}
