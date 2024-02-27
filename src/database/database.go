package database

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type Configuration struct {
	ID        string `json:"id" firestore:"id"` // "" means unindentified configuration
	ProjectID string `json:"project_id" firestore:"id"`
	Dataset   string `json:"dataset" firestore:"dataset"`
	Table     string `json:"table" firestore:"table"`
}

func NewConfiguration(projectID string, dataset string, table string) Configuration {
	// Creates a new configuration without identity
	return Configuration{
		ID:        "",
		ProjectID: projectID,
		Dataset:   dataset,
		Table:     table,
	}
}

type Database interface {
	InsertConfig(Configuration) (Configuration, error)
	ListConfigs() ([]Configuration, error)
	GetConfigByID(string) (Configuration, error) // Returns identified configuration
	UpdateConfig(string, Configuration) (Configuration, error)
	DeleteConfig(string) (Configuration, error)
}

type inMemoryDatabase struct {
	Configurations map[string]Configuration
}

func NewInMemoryDB() Database {
	return &inMemoryDatabase{
		Configurations: map[string]Configuration{},
	}
}

func (db *inMemoryDatabase) InsertConfig(c Configuration) (Configuration, error) {

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

func (db *inMemoryDatabase) ListConfigs() ([]Configuration, error) {
	var configs []Configuration = []Configuration{}
	for _, value := range db.Configurations {
		configs = append(configs, value)
	}

	// This interface, in particular, can't error
	return configs, nil
}

func (db *inMemoryDatabase) GetConfigByID(id string) (Configuration, error) {
	config, ok := db.Configurations[id]
	if !ok {
		return config, fmt.Errorf("Configuration with id %s not found", id)
	}
	return config, nil
}

func (db *inMemoryDatabase) UpdateConfig(id string, c Configuration) (Configuration, error) {

	// The input Configuration must be Unidentified and existing in the database
	var result Configuration
	if c.ID != "" {
		return result, errors.New("Configuration must be unidentified")
	}

	_, ok := db.Configurations[id]
	if !ok {
		return result, errors.New("Configuration not found")
	}

	c.ID = id
	db.Configurations[id] = c
	return c, nil
}

func (db *inMemoryDatabase) DeleteConfig(id string) (Configuration, error) {
	var result Configuration
	// Check if configuration exists
	config, ok := db.Configurations[id]
	if !ok {
		return result, errors.New(fmt.Sprintf("Configuration with id %s not found", id))
	}

	// Delete it
	delete(db.Configurations, id)
	return config, nil
}
