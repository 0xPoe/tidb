// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type schemaGenerator struct {
	colID int64
}

// generateKeys4Schema will generate keys for a given schema. Used only in this file.
func generateKeys4Schema(schema *Schema) {
	keyCount := len(schema.Columns) - 1
	keys := make([]KeyInfo, 0, keyCount)
	for i := range keyCount {
		keys = append(keys, []*Column{schema.Columns[i]})
	}
	schema.PKOrUK = keys
}

// generateSchema will generate a schema for test. Used only in this file.
func (s *schemaGenerator) generateSchema(colCount int) *Schema {
	cols := make([]*Column, 0, colCount)
	for range colCount {
		s.colID++
		cols = append(cols, &Column{
			UniqueID: s.colID,
		})
	}
	return NewSchema(cols...)
}

func TestSchemaClone(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	generateKeys4Schema(schema)

	uniKeys := make([]KeyInfo, 0, len(schema.Columns)-1)
	for i := range len(schema.Columns) - 1 {
		uniKeys = append(uniKeys, []*Column{schema.Columns[i]})
	}
	schema.SetUniqueKeys(uniKeys)

	clonedSchema := schema.Clone()
	require.Equal(t, schema.String(), clonedSchema.String())

	require.NotSame(t, unsafe.SliceData(schema.PKOrUK), unsafe.SliceData(clonedSchema.PKOrUK))
	require.NotSame(t, unsafe.SliceData(schema.NullableUK), unsafe.SliceData(clonedSchema.NullableUK))
}

func TestSchemaString(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] PKOrUK: [] NullableUK: []", schema.String())
	generateKeys4Schema(schema)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] PKOrUK: [[Column#1],[Column#2],[Column#3],[Column#4]] NullableUK: []", schema.String())
}

func TestSchemaRetrieveColumn(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		require.Equal(t, col, schema.RetrieveColumn(col))
	}
	require.Nil(t, schema.RetrieveColumn(colOutSchema))
}

func TestSchemaIsUniqueKey(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	generateKeys4Schema(schema)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i, col := range schema.Columns {
		if i < len(schema.Columns)-1 {
			require.Equal(t, true, schema.IsUnique(true, col))
		} else {
			require.Equal(t, false, schema.IsUnique(true, col))
		}
	}
	require.Equal(t, false, schema.IsUnique(true, colOutSchema))
}

func TestSchemaContains(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		require.Equal(t, true, schema.Contains(col))
	}
	require.Equal(t, false, schema.Contains(colOutSchema))
}

func TestSchemaColumnsIndices(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i := range len(schema.Columns) - 1 {
		colIndices := schema.ColumnsIndices([]*Column{schema.Columns[i], schema.Columns[i+1]})
		for j, res := range colIndices {
			require.Equal(t, i+j, res)
		}
	}
	require.Nil(t, schema.ColumnsIndices([]*Column{schema.Columns[0], schema.Columns[1], colOutSchema, schema.Columns[2]}))
}

func TestSchemaColumnsByIndices(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	indices := []int{0, 1, 2, 3}
	retCols := schema.ColumnsByIndices(indices)
	for i, ret := range retCols {
		require.Equal(t, fmt.Sprintf("%p", ret), fmt.Sprintf("%p", schema.Columns[i]))
	}
}

func TestSchemaMergeSchema(t *testing.T) {
	s := &schemaGenerator{}
	lSchema := s.generateSchema(5)
	generateKeys4Schema(lSchema)

	rSchema := s.generateSchema(5)
	generateKeys4Schema(rSchema)

	require.Nil(t, MergeSchema(nil, nil))
	require.Equal(t, lSchema.String(), MergeSchema(lSchema, nil).String())
	require.Equal(t, rSchema.String(), MergeSchema(nil, rSchema).String())

	schema := MergeSchema(lSchema, rSchema)
	for i := range lSchema.Columns {
		require.Equal(t, lSchema.Columns[i].UniqueID, schema.Columns[i].UniqueID)
	}
	for i := range rSchema.Columns {
		require.Equal(t, rSchema.Columns[i].UniqueID, schema.Columns[i+len(lSchema.Columns)].UniqueID)
	}
}

func TestGetUsedList(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	var usedCols []*Column
	usedCols = append(usedCols, schema.Columns[3])
	usedCols = append(usedCols, s.generateSchema(2).Columns...)
	usedCols = append(usedCols, schema.Columns[1])
	usedCols = append(usedCols, schema.Columns[3])

	used := GetUsedList(mock.NewContext(), usedCols, schema)
	require.Equal(t, []bool{false, true, false, true, false}, used)
}
