package sink

import (
	"fmt"
	"context"
	"log"
	"os"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/encoding/protojson"
	conf "github.com/altxtech/webhook-connector/src/configurations"
)

// Clients
var bqWriteClient *storage.BigQueryWriteClient
func getBqWriteClient() (*storage.BigQueryWriteClient, error) {
	if bqWriteClient == nil {
		var err error
		bqWriteClient, err = storage.NewBigQueryWriteClient(context.Background())
		if err != nil {
			return bqWriteClient, fmt.Errorf("Failed to create BigQueryWriteClient: %v", err)
		}
	}
	return bqWriteClient, nil
}


// Sink type
type Sink interface {
	 WriteRows([]protoreflect.ProtoMessage) error
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
		client, err := getBqWriteClient()
		if err != nil {
			return nil, fmt.Errorf("Failed to retrieve BigQueryWriteClient: %v", err)
		}
		s, err := NewBigQuerySink(project, dataset, table, "webhook-connector", client)
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

// BigQuery Sink
type bigQuerySink struct {
	Project string
	Dataset string
	Table string
	Trace string
	Client *storage.BigQueryWriteClient
	Stream storagepb.BigQueryWrite_AppendRowsClient
}

func NewBigQuerySink( project string, dataset string, table string, trace string, client *storage.BigQueryWriteClient) (Sink, error) {
	
	var sink *bigQuerySink
	
	stream, err := client.AppendRows(context.Background())
	if err != nil {
		return sink, err
	}

	sink = &bigQuerySink{
		Project: project,
		Dataset: dataset,
		Table: table,
		Trace: trace,
		Client: client,
		Stream: stream,
	}

	return sink, nil
}



func (sink *bigQuerySink) WriteRows(rows []protoreflect.ProtoMessage,) error {


	// get the stream by calling AppendRows
	log.Println("calling AppendRows...")

	// serialize the rows
	log.Println("marshalling the rows...")
	var opts proto.MarshalOptions
	var data [][]byte
	for _, row := range rows {
		buf, err := opts.Marshal(row)
		if err != nil {
			return err
		}
		data = append(data, buf)
	}

	// send the rows to bigquery
	descriptor := getDescriptor(rows[0])
	log.Println("sending the data...")
	err := sink.Stream.Send(&storagepb.AppendRowsRequest{
		WriteStream: fmt.Sprintf("projects/%s/datasets/%s/tables/%s/_default", sink.Project, sink.Dataset, sink.Table),
		TraceId:     sink.Trace, // identifies this client
		Rows: &storagepb.AppendRowsRequest_ProtoRows{
			ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				// protocol buffer schema
				WriterSchema: &storagepb.ProtoSchema{
					ProtoDescriptor: descriptor,
				},
				// protocol buffer data
				Rows: &storagepb.ProtoRows{
					SerializedRows: data, // serialized protocol buffer data
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// get the response, which will tell us whether it worked
	log.Println("waiting for response...")
	r, err := sink.Stream.Recv()
	if err != nil {
		return err
	}

	if rErr := r.GetError(); rErr != nil {
		return fmt.Errorf ("result was error: %v", rErr)
	} else if rResult := r.GetAppendResult(); rResult != nil {
		log.Println("Append rows sucessfull")
	}

	log.Println("done")
	return nil
}
