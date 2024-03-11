package sink

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	conf "github.com/altxtech/webhook-connector/src/configurations"
	"github.com/altxtech/webhook-connector/src/model"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Sink type
type Sink interface {
	 WriteRows([]protoreflect.ProtoMessage) error
	 Close() error
}

func NewSink(config conf.Sink) (Sink, error){

	// Check if config is valid
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("Invalid sink: %v", err)
	}

	// Create the appropriate sink based on type
	switch config.Type{
	case "jsonl":
		path := config.Config["file_path"].(string) 
		return NewJSONLSink(path), nil
	case "bigquery":
		project := config.Config["project"].(string)
		dataset := config.Config["dataset"].(string)
		table := config.Config["table"].(string)
		if err != nil {
			return nil, fmt.Errorf("Failed to retrieve BigQueryWriteClient: %v", err)
		}
		s, err := NewBigQuerySink(project, dataset, table, "webhook-connector")
		if err != nil {
			return s, fmt.Errorf("Failed to create bigQuerySink: %v", err)
		}
		return s, nil
	default:
		return nil, fmt.Errorf("Unsuported sink type '%s'", config.Type)
	}
}


// Helpers
func getDescriptor(message protoreflect.ProtoMessage) *descriptorpb.DescriptorProto  {
	descriptor, err := adapt.NormalizeDescriptor(message.ProtoReflect().Descriptor())
	if err != nil {
		log.Fatal("NormalizeDescriptor: ", err)
	}
	return descriptor
}

// Local file sink (for testing)
type JSONLSink struct {
	Path string
}
func NewJSONLSink (path string) JSONLSink {
	return JSONLSink{
		Path: path,
	}
}
func (sink JSONLSink) WriteRows(rows []protoreflect.ProtoMessage) error {

	/*
		TODO:
		This is a doo doo implementation because we have to reopen and close the file every time we write a new batch of rows.
		
		This is good enough for testing, tough. But should be improved in the unlikely scenario this gets used for real.
	*/

	file, err := os.OpenFile(sink.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open file: %v", err)
	}
	defer file.Close()

	for _, row := range rows {
		msg, err := protojson.Marshal(row)
		if err != nil {
			return err
		}
		msg = append(msg, '\n')
		file.Write(msg)
	}

	return nil
}
func (sink JSONLSink) Close() error {
	return nil
}

// BigQuery Sink
type bigQuerySink struct {
	Project string
	Dataset string
	Table string
	Trace string
	client *managedwriter.Client
	stream *managedwriter.ManagedStream
}

func NewBigQuerySink( project string, dataset string, table string, trace string) (Sink, error) {
	
	var sink *bigQuerySink

	// Create bigquery client
	client, err := managedwriter.NewClient(context.Background(), project)
	if err != nil {
		return sink, fmt.Errorf("Error creating Bigquery Writer: %v", err)
	}
	

	// Get the descriptor for the event message
	/*
		When I want to generalize to different many schemas...
		Create a descriptor based on the schema configuration.
	*/
	m := &model.WebhookEvent{}
	descriptor, err := adapt.NormalizeDescriptor(m.ProtoReflect().Descriptor())
	if err != nil {
		return sink, fmt.Errorf("Failed to get prot descriptor: %v", err)
	}

	// Create managed stream
	tableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)
	stream, err := client.NewManagedStream(
		context.Background(),
		managedwriter.WithDestinationTable(tableName),
		managedwriter.WithSchemaDescriptor(descriptor),
	)

	// Construct the sink object
	sink = &bigQuerySink{
		Project: project,
		Dataset: dataset,
		Table: table,
		Trace: trace,
		client: client,
		stream: stream,
	}

	return sink, nil
}



func (sink *bigQuerySink) WriteRows(rows []protoreflect.ProtoMessage,) error {

	// Encode the messages
	encoded := make([][]byte, len(rows))
	for k, v := range rows {
		b, err := proto.Marshal(v)
		if err != nil {
			return fmt.Errorf("Error marshalling rows: %v", err)
		}
		encoded[k] = b
	}

	result, err := sink.stream.AppendRows(context.Background(), encoded)
	_, err = result.GetResult(context.Background())
	if err != nil {
		return fmt.Errorf("Error appending rows: %v", err)
	}

	return nil
}
func (sink *bigQuerySink) Close() error {

	err := sink.client.Close()
	if err != nil {
		return fmt.Errorf("Error closing client: %v", err)
	}
	return nil
}
