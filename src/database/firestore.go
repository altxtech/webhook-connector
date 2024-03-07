// database/firestore_database.go
package database

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	conf "github.com/altxtech/webhook-connector/src/configurations"
)

// FirestoreDatabase represents a Firestore database.
type firestoreDatabase struct {
	Client *firestore.Client
}

// NewFirestoreDatabase creates a new instance of FirestoreDatabase.
func NewFirestoreDatabase(ctx context.Context, projectId string, databaseID string) (Database, error) {
	client, err := firestore.NewClientWithDatabase(ctx, projectId, databaseID)
	if err != nil {
		return nil, err
	}

	return &firestoreDatabase{
		Client: client,
	}, nil
}

func (db *firestoreDatabase) InsertConfig(config conf.Configuration) (conf.Configuration, error) {
	if config.ID != "" {
		return conf.Configuration{}, errors.New("Can't insert identified config")
	}

	
	docRef, _, err := db.Client.Collection("configurations").Add(context.Background(), config)
	if err != nil {
		return conf.Configuration{}, err
	}

	config.SetID(docRef.ID)

	// The document is initially written is a null "id" field. We have to rewrite it to set the id
	_, err = docRef.Set(context.Background(), config)
	if err != nil {
		return config, fmt.Errorf("Failure setting the id in the database: %v", err)
	}

	return config, nil
}

func (db *firestoreDatabase) ListConfigs() ([]conf.Configuration, error) {
	iter := db.Client.Collection("configurations").Documents(context.Background())
	var configs []conf.Configuration

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var config conf.Configuration
		if err := doc.DataTo(&config); err != nil {
			return nil, err
		}

		configs = append(configs, config)
	}

	return configs, nil
}

func (db *firestoreDatabase) GetConfigByID(id string) (conf.Configuration, error) {
	if id == "" {
		return conf.Configuration{}, errors.New("Config ID is required")
	}

	docRef := db.Client.Collection("configurations").Doc(id)
	doc, err := docRef.Get(context.Background())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return conf.Configuration{}, fmt.Errorf("conf.Configuration with id %s not found", id)
		}
		return conf.Configuration{}, err
	}

	var config conf.Configuration
	if err := doc.DataTo(&config); err != nil {
		return conf.Configuration{}, err
	}

	return config, nil
}

func (db *firestoreDatabase) UpdateConfig(config conf.Configuration) (conf.Configuration, error) {
	if config.ID == "" {
		return conf.Configuration{}, errors.New("Config ID is required")
	}

	_, err := db.Client.Collection("configurations").Doc(config.ID).Set(context.Background(), config)
	if err != nil {
		return conf.Configuration{}, err
	}

	return config, nil
}

func (db *firestoreDatabase) DeleteConfig(id string) (conf.Configuration, error) {
	if id == "" {
		return conf.Configuration{}, errors.New("Config ID is required")
	}

	docRef := db.Client.Collection("configurations").Doc(id)
	doc, err := docRef.Get(context.Background())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return conf.Configuration{}, fmt.Errorf("conf.Configuration with id %s not found", id)
		}
		return conf.Configuration{}, err
	}

	var config conf.Configuration
	if err := doc.DataTo(&config); err != nil {
		return conf.Configuration{}, err
	}

	if _, err := docRef.Delete(context.Background()); err != nil {
		return conf.Configuration{}, err
	}

	return config, nil
}

