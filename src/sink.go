package main

import (
	"fmt"
	"context"
	"log"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
)


// BIGQUERY

func getDescriptor(message protoreflect.ProtoMessage) *descriptorpb.DescriptorProto  {
	descriptor, err := adapt.NormalizeDescriptor(message.ProtoReflect().Descriptor())
	if err != nil {
		log.Fatal("NormalizeDescriptor: ", err)
	}
	return descriptor
}

var stream storagepb.BigQueryWrite_AppendRowsClient

func writeRows(
	rows []protoreflect.ProtoMessage,
	project string, dataset string, table string,
	trace string,
) error {

	ctx := context.Background()

	// get the stream by calling AppendRows
	log.Println("calling AppendRows...")

	var err error
	if stream == nil {
		// Stream should be called just once
		stream, err = bq.AppendRows(ctx)
		if err != nil {
			return err
		}
	}

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
	err = stream.Send(&storagepb.AppendRowsRequest{
		WriteStream: fmt.Sprintf("projects/%s/datasets/%s/tables/%s/_default", project, dataset, table),
		TraceId:     trace, // identifies this client
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
	r, err := stream.Recv()
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