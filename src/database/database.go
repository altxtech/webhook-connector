package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	firestore "cloud.google.com/go/firestore/apiv1"
	conf "github.com/altxtech/webhook-connector/src/configurations"
)


type Database interface {
	InsertConfig(conf.Configuration) (conf.Configuration, error)
	ListConfigs() ([]conf.Configuration, error)
	GetConfigByID(string) (conf.Configuration, error) // Returns identified configuration
	UpdateConfig(conf.Configuration) (conf.Configuration, error)
	DeleteConfig(string) (conf.Configuration, error)
}

type inMemoryDatabase struct {
	Confs map[string]conf.Configuration
}

func NewInMemoryDB() Database {
	return &inMemoryDatabase{
		Confs: map[string]conf.Configuration{},
	}
}

func (db *inMemoryDatabase) InsertConfig(c conf.Configuration) (conf.Configuration, error) {

	var result conf.Configuration

	// Check if configuration is unindentified
	if c.ID != "" {
		return result, errors.New("Can't insert identified config")
	}

	result = c
	result.SetID(uuid.NewString())

	db.Confs[result.ID] = result
	return result, nil
}

func (db *inMemoryDatabase) ListConfigs() ([]conf.Configuration, error) {
	var configs []conf.Configuration = []conf.Configuration{}
	for _, value := range db.Confs {
		configs = append(configs, value)
	}

	// This interface, in particular, can't error
	return configs, nil
}

func (db *inMemoryDatabase) GetConfigByID(id string) (conf.Configuration, error) {
	config, ok := db.Confs[id]
	if !ok {
		return config, fmt.Errorf("conf.Configuration with id %s not found", id)
	}
	return config, nil
}

func (db *inMemoryDatabase) UpdateConfig(c conf.Configuration) (conf.Configuration, error) {

	// The input conf.Configuration must be Identified
	var result conf.Configuration
	if c.ID == "" {
		return result, errors.New("Configuration must be identified")
	}

	_, ok := db.Confs[c.ID]
	if !ok {
		return result, errors.New("Configuration not found")
	}

	db.Confs[c.ID] = c
	return c, nil
}

func (db *inMemoryDatabase) DeleteConfig(id string) (conf.Configuration, error) {
	var result conf.Configuration
	// Check if configuration exists
	config, ok := db.Confs[id]
	if !ok {
		return result, errors.New(fmt.Sprintf("conf.Configuration with id %s not found", id))
	}

	// Delete it
	delete(db.Confs, id)
	return config, nil
}

// Firestore database
type firestoreDatabase struct {
	Client *firestore.Client
}
func NewFirestoreDatabase() (Database, error){
	
	var result *firestoreDatabase

	// Initialize Client
	client, err := firestore.NewClient(context.Background())
	if err != nil {
		return result, err
	}

	result.Client = client
	return result, nil
}
func (db *firestoreDatabase) InsertConfig(config conf.Configuration) (conf.Configuration, error){
	//TODO: Implement
	var insertedConfig conf.Configuration
	return insertedConfig, nil
}
func (db *firestoreDatabase) ListConfigs() ([]conf.Configuration, error){
	//TODO: Implement
	var configs []conf.Configuration
	return configs, nil
}
func (db *firestoreDatabase) GetConfigByID(id string) (conf.Configuration, error){
	//TODO: Implement
	var config conf.Configuration
	return config, nil
}
func (db *firestoreDatabase) UpdateConfig(config conf.Configuration) (conf.Configuration, error){
	//TODO: Implement
	var updatedConfig conf.Configuration
	return updatedConfig, nil
}
func (db *firestoreDatabase) DeleteConfig(id string) (conf.Configuration, error){
	//TODO: Implement
	var deletedConfig conf.Configuration
	return deletedConfig, nil
}
