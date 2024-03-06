package configurations

import (
	"errors"
)

type Configuration struct {
	ID        string `json:"id" firestore:"id"` // "" means unindentified configuration
	Sink Sink `json:"sink" firestore:"sink"`
}
func NewConfiguration(sink Sink) Configuration {
	// Creates a new configuration without identity
	return Configuration{
		ID:        "",
		Sink: sink,
	}
}
func (c *Configuration) SetID(id string) {
	c.ID = id
}
func (c *Configuration) SetSink(sink Sink){
	c.Sink = sink
}


type Sink struct {
	Type string `json:"type" firestore:"type"`
	Config SinkConfig `json:"config" firestore:"config"`
}
func NewSink(t string, config SinkConfig) Sink {
	return Sink {
		Type: t,
		Config: config,
	}
}

type SinkConfig interface{
	Validate() error
}


// Implementations of SinkConfig

// Local file - For testing
type JSONLSinkConfig struct {
	FilePath string `json:"file_path" firestore:"file_path"` // Absolute path, or relative to working directory of the application
}

func (f JSONLSinkConfig) Validate() error {
    if f.FilePath == "" {
        return errors.New("file_path cannot be empty")
    }
    return nil
}

// BigQuery
type BigQuerySinkConfig struct {
	ProjectID string `json:"project_id" firestore:"id"`
	Dataset   string `json:"dataset" firestore:"dataset"`
	Table     string `json:"table" firestore:"table"`
}
func (b BigQuerySinkConfig) Validate() error {
    if b.ProjectID == "" {
        return errors.New("ProjectID cannot be empty")
    }
    if b.Dataset == "" {
        return errors.New("Dataset cannot be empty")
    }
    if b.Table == "" {
        return errors.New("Table cannot be empty")
    }

	// TODO: Use the Bigquery API to check if the table exists and that the connector has the appropriate permissions
    return nil
}
