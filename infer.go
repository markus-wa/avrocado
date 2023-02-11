package avro

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/fatih/structtag"
)

type TypedSchema struct {
	Name   string      `json:"name"`
	Type   interface{} `json:"type"`
	types  []interface{}
	items  []interface{}
	Items  interface{} `json:"items,omitempty"`
	values []interface{}
	Values interface{}   `json:"values,omitempty"`
	Fields []TypedSchema `json:"fields,omitempty"`
}

func inferType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.String:
		return "string", nil
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int", nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "long", nil
	case reflect.Float32, reflect.Float64:
		return "double", nil
	}

	return "", errors.New("unsupported type")
}

func inferSchema(tag string, t reflect.Type) (s TypedSchema, err error) {
	s.Name = t.Name()

	switch t.Kind() {
	case reflect.Ptr:
		typ, err := inferSchema(tag, t.Elem())
		if err != nil {
			return s, err
		}

		s.types = append(s.types, typ)
		s.types = append(s.types, "null")

	case reflect.Struct:
		s.types = append(s.types, "record")
		s.Fields = make([]TypedSchema, t.NumField())

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			tags, err := structtag.Parse(string(field.Tag))
			if err != nil {
				return s, err
			}

			s.Fields[i], err = inferSchema(tag, field.Type)
			if err != nil {
				return s, err
			}

			if tag, err := tags.Get(tag); err == nil {

				s.Fields[i].Name = tag.Name
			} else {
				s.Fields[i].Name = field.Name
			}
		}

	case reflect.Slice:
		s.types = append(s.types, "array")

		typ, err := inferSchema(tag, t.Elem())
		if err != nil {
			return s, err
		}

		s.items = append(s.items, typ)
	case reflect.Map:
		s.types = append(s.types, "map")

		if t.Key().Kind() != reflect.String {
			return s, errors.New("map key must be string")
		}

		typ, err := inferSchema(tag, t.Elem())
		if err != nil {
			return s, err
		}

		s.values = append(s.values, typ)

	default:
		typ, err := inferType(t)
		if err != nil {
			return s, err
		}

		s.types = append(s.types, typ)
	}

	if len(s.types) == 1 {
		s.Type = s.types[0]
	} else if len(s.types) > 1 {
		s.Type = s.types
	}

	if len(s.items) == 1 {
		s.Items = s.items[0]
	} else if len(s.items) > 1 {
		s.Items = s.items
	}

	if len(s.values) == 1 {
		s.Values = s.values[0]
	} else if len(s.values) > 1 {
		s.Values = s.values
	}

	return s, nil
}

// InferSchema will infer the avro schema from a Go struct.
// The tag parameter is the name of the struct tag to use for the avro field name.
// If the tag is an empty string, "avro" will be used.
func InferSchema(tag string, v interface{}) (string, error) {
	if tag == "" {
		tag = "avro"
	}

	s, err := inferSchema(tag, reflect.TypeOf(v))
	if err != nil {
		return "", err
	}

	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
