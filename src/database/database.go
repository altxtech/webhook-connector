package database

import (
	"github.com/google/uuid"
	"errors"
	"fmt"
)

type Configuration struct {
	ID string `json:"id" firestore:"id"` // "" means unindentified configuration
	ProjectID string `json:"project_id" firestore:"id"`
	Dataset string `json:"dataset" firestore:"dataset"`
}

func NewConfiguration( projectID string, dataset string) Configuration{
	// Creates a new configuration without identity
	return Configuration {
		ID: "",
		ProjectID: projectID,
		Dataset: dataset,
	}
}

type Database interface {
	InsertConfig(Configuration) (Configuration, error) 
	GetConfigByID(string) (Configuration, error) // Returns identified configuration
}


type InMemoryDatabase struct {
	Configurations map[string]Configuration
}

func (db *InMemoryDatabase) InsertConfig(c Configuration) (Configuration, error){

	var result Configuration 
	
	// Check if configuration is unindentified
	if c.ID != "" {
		return result, errors.New("Can't insert identified config")
	}

	result = c
	result.ID = uuid.NewString()
	
	db.Configurations[result.ID] = result
	return result, nil
}

func (db *InMemoryDatabase) GetConfigByID(id string) (Configuration, error){
	config, ok := db.Configurations[id]
	if !ok {
		return config, fmt.Errorf("Configuration with id %s not found", id)
	}
	return config, nil
}
