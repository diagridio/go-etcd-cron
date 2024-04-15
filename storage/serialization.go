/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package storage

import (
	"bytes"
	"compress/gzip"
	"io"

	"google.golang.org/protobuf/proto"
)

func serialize(record *JobRecord) ([]byte, error) {
	data, err := proto.Marshal(record)
	if err != nil {
		return nil, err
	}

	if (data == nil) || (len(data) == 0) {
		return data, nil
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err = writer.Write(data)
	if err != nil {
		return nil, err
	}
	writer.Close()

	return compressed.Bytes(), nil
}

func deserialize(data []byte, record *JobRecord) error {
	if (data == nil) || (len(data) == 0) {
		return nil
	}

	compressed := bytes.NewReader(data)
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}

	defer reader.Close()
	decompressed, _ := io.ReadAll(reader)

	return proto.Unmarshal(decompressed, record)
}
