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

func serialize(compress bool, record *JobRecord) ([]byte, error) {
	data, err := proto.Marshal(record)
	if err != nil {
		return nil, err
	}

	if !compress || (data == nil) || (len(data) == 0) {
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

func deserialize(compressed bool, data []byte, record *JobRecord) error {
	if (data == nil) || (len(data) == 0) {
		return nil
	}

	if !compressed {
		return proto.Unmarshal(data, record)
	}

	compressedData := bytes.NewReader(data)
	reader, err := gzip.NewReader(compressedData)
	if err != nil {
		return err
	}

	defer reader.Close()
	decompressed, _ := io.ReadAll(reader)

	return proto.Unmarshal(decompressed, record)
}
