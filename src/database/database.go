package database

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	conf "github.com/altxtech/webhook-connector/src/configurations"
)


type Database interface {
	InsertConfig(conf.Configuration) (conf.Configuration, error)
	ListConfigs() ([]conf.Configuration, error)
	GetConfigByID(string) (conf.Configuration, error) // Returns identified configuration
	UpdateConfig(string, conf.Configuration) (conf.Configuration, error)
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

func (db *inMemoryDatabase) UpdateConfig(id string, c conf.Configuration) (conf.Configuration, error) {

	// The input conf.Configuration must be Unidentified and existing in the database
	var result conf.Configuration
	if c.ID != "" {
		return result, errors.New("conf.Configuration must be unidentified")
	}

	_, ok := db.Confs[id]
	if !ok {
		return result, errors.New("conf.Configuration not found")
	}

	c.ID = id
	db.Confs[id] = c
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
