package configurations

import (
	"fmt"
	"time"
)

type Configuration struct {
	ID        string `json:"id" firestore:"id"` // "" means unindentified configuration
	Name string `json:"name" firestore:"name"`
	Sink Sink `json:"sink" firestore:"sink"`
	CreatedAt time.Time `json:"created_at" firestore:"created_at"`
	UpdatedAt time.Time `json:"updated_at" firestore:"updated_at"`
}
func NewConfiguration(name string, sink Sink) Configuration {
	// Creates a new configuration without identity
	return Configuration{
		ID:        "",
		Name: name,
		Sink: sink,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
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
	Config map[string]interface{} `json:"config" firestore:"config"`
}
func NewSink(t string, config map[string]interface{}) (Sink, error) {
	newSink := Sink {
		Type: t,
		Config: config,
	}

	err := newSink.Validate()
	if err != nil {
		return newSink, fmt.Errorf("Invalid sink configuration: %v", err)
	}

	return newSink, nil
}
func (s Sink) Validate() error{
	/*
		This method is necessary for 2 reasons:

		1. The type attribute needs to match a supported Sink Type
		2. The Config attribute must be valid for for the chosen sink type
	*/

	switch s.Type {
	case "jsonl":
		err := s.paramIsString("file_path")
		if err != nil {
			return err
		}

	case "bigquery":
		err := s.paramIsString("project")
		if err != nil {
			return err
		}
		err = s.paramIsString("dataset")
		if err != nil {
			return err
		}
		err = s.paramIsString("table")
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%s is not a supported sink type.", s.Type)
	}

	return nil
}

func (s Sink) paramIsString(key string) error {
	
	val, ok := s.Config[key]
	if !ok {
		return fmt.Errorf("Missing required parameter '%s'.", key)
	}
	_, ok = val.(string)
	if !ok {
		return fmt.Errorf("Parameter %s must be a string.", key)
	}
	return nil
	
}
