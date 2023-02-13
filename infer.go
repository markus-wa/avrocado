package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

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

	return "", fmt.Errorf("unsupported type: %s", t.Kind())
}

func inferSchema(fallbackTag string, t reflect.Type, items, values []string) (s TypedSchema, err error) {
	s.Name = t.Name()

	switch t.Kind() {
	case reflect.Ptr:
		typ, err := inferSchema(fallbackTag, t.Elem(), nil, nil)
		if err != nil {
			return s, fmt.Errorf("ptr: %w", err)
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
				return s, fmt.Errorf("struct: %w", err)
			}

			var (
				name        string
				fieldValues []string
				fieldItems  []string
			)

			if tag, err := tags.Get("avro"); err == nil {
				name = tag.Name

				for _, opt := range tag.Options {
					if strings.HasPrefix(opt, "type=") {
						typeStr := strings.TrimPrefix(opt, "type=")
						types := strings.Split(typeStr, "|")

						for _, t := range types {
							s.Fields[i].types = append(s.Fields[i].types, t)
						}
					}
				}

				for _, opt := range tag.Options {
					if strings.HasPrefix(opt, "values=") {
						valuesStr := strings.TrimPrefix(opt, "values=")
						fieldValues = strings.Split(valuesStr, "|")
					}
				}

				for _, opt := range tag.Options {
					if strings.HasPrefix(opt, "items=") {
						itemsStr := strings.TrimPrefix(opt, "items=")
						fieldItems = strings.Split(itemsStr, "|")
					}
				}
			} else if tag, err := tags.Get(fallbackTag); err == nil {
				name = tag.Name
			} else {
				name = field.Name
			}

			if s.Fields[i].types == nil {
				s.Fields[i], err = inferSchema(fallbackTag, field.Type, fieldItems, fieldValues)
				if err != nil {
					return s, fmt.Errorf("struct: %w", err)
				}
			}

			s.Fields[i].Name = name
		}

	case reflect.Slice:
		s.types = append(s.types, "array")

		if items != nil {
			for _, i := range items {
				s.items = append(s.items, i)
			}
		} else {
			typ, err := inferSchema(fallbackTag, t.Elem(), nil, nil)
			if err != nil {
				return s, fmt.Errorf("slice: %w", err)
			}

			s.items = append(s.items, typ)
		}

	case reflect.Map:
		s.types = append(s.types, "map")

		if t.Key().Kind() != reflect.String {
			return s, errors.New("map key must be string")
		}

		if values != nil {
			for _, v := range values {
				s.values = append(s.values, v)
			}
		} else {
			typ, err := inferSchema(fallbackTag, t.Elem(), nil, nil)
			if err != nil {
				return s, fmt.Errorf("map: %w", err)
			}

			s.values = append(s.values, typ)
		}

	default:
		typ, err := inferType(t)
		if err != nil {
			return s, fmt.Errorf("default: %w", err)
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
// The fallbackTag parameter is the name of the struct tag to use if the avro tag is not present.
// The v parameter is the struct to infer the schema from.
func InferSchema(fallbackTag string, v interface{}) (string, error) {
	s, err := inferSchema(fallbackTag, reflect.TypeOf(v), nil, nil)
	if err != nil {
		return "", fmt.Errorf("infer schema: %w", err)
	}

	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshal schema: %w", err)
	}

	return string(b), nil
}
