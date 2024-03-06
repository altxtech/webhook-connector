package database

import (
	"context"
	"log"
	"os"
	"testing"

	conf "github.com/altxtech/webhook-connector/src/configurations"
)

func initFirestoreDB() Database {
	firestoreDB, err := NewFirestoreDatabase(context.Background(), os.Getenv("PROJECT_ID"), os.Getenv("DATABASE_ID"))
	if err != nil {
		log.Fatal(err)
	}

	return firestoreDB
}
var firestoreDB Database = initFirestoreDB()

func createDummyConfig() (conf.Configuration, error) {
	var dummySink conf.Sink = conf.NewSink("jsonl", conf.JSONLSinkConfig{FilePath: "data/events.jsonl"})
	var dummyConfig conf.Configuration = conf.NewConfiguration(dummySink)

	return dummyConfig, nil
}

func TestFirestoreDB(t *testing.T){

	// Write and delete test
	dummyConfig, err := createDummyConfig()
	if err != nil {
		t.Fatalf("Failed to create dummy config: %v", err)
	}
	insertedConfig, err := firestoreDB.InsertConfig(dummyConfig)
	if err != nil {
		t.Fatalf("Failure inserting config into db: %v", err)
	}

	// Assess that the inserted config has an id
	if insertedConfig.ID == "" {
		t.Fatal("Inserted config came back with null id")
	}

	// Try deleting it
	deletedConfig, err := firestoreDB.DeleteConfig(insertedConfig.ID)
	if err != nil {
		t.Fatalf("Failure deleting config: %v", err)
	}

	// Assess that the deleted config id is the same as the inserted
	if deletedConfig.ID != insertedConfig.ID {
		t.Fatalf("Deleted config ID %s is does not match the inserted config id %s", deletedConfig.ID, insertedConfig.ID)
	}
}
