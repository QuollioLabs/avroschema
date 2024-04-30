package avroschema

import (
	"reflect"
	"strings"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})

type Reflector struct {
	/*
	   Make all fields of Record be backward transitive, i.e., all fields are optional.
	*/
	BeBackwardTransitive bool
	Mapper               func(reflect.Type) any
}

/*
Return type is either a string, a *AvroSchema of a slice of *AvroSchema.
*/
func (r *Reflector) reflectType(t reflect.Type, forFields bool) any {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if r.Mapper != nil {
		if ret := r.Mapper(t); ret != nil {
			return ret
		}
	}

	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "int"
	case reflect.Int64, reflect.Uint64:
		return "long"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.Bool:
		return "boolean"
	case reflect.Array, reflect.Slice:
		return r.handleArray(t, forFields)
	case reflect.Struct:
		// handle special built-in types, e.g. time.Time
		if t == timeType {
			return &AvroSchema{Type: "long", LogicalType: "timestamp-millis"}
		}
		return r.handleRecord(t, true)
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			// If the key is not a string, then treat the whole object as a string.
			return "string"
		}
		return r.handleMap(t)
	default:
		return "string" // any
	}
}

func (r *Reflector) handleMap(t reflect.Type) *AvroSchema {
	return &AvroSchema{Type: "map", Values: r.reflectType(t.Elem(), false)}
}

func (r *Reflector) handleArray(t reflect.Type, forFields bool) *AvroSchema {
	if forFields {
		return &AvroSchema{Type: AvroSchema{Type: "array", Items: r.reflectType(t.Elem(), false)}}
	}
	return &AvroSchema{Type: "array", Items: r.reflectType(t.Elem(), false)}
}

func (r *Reflector) handleRecord(t reflect.Type, forFields bool) *AvroSchema {
	name := t.Name()
	tokens := strings.Split(name, ".")
	name = tokens[len(tokens)-1]

	ret := &AvroSchema{Name: name, Type: "record"}

	for i, n := 0, t.NumField(); i < n; i++ { // handle fields
		f := t.Field(i)

		jsonTag := f.Tag.Get("json")
		jsonFieldName, isOptional := GetNameAndOmit(jsonTag)
		bsonTag := f.Tag.Get("bson")

		if jsonFieldName == "" && bsonTag == "" {
			continue
		}
		ret.Fields = append(ret.Fields, r.reflectEx(f.Type, isOptional, jsonFieldName, forFields)...)
	}
	return ret
}

/*
Fill in the Name for the AvroSchema.
If the reflectType is a simple string, generate an AvroSchema and filled in Type.
But if it is already an AvroSchema, only the Name needs to be filled in.
*/
func (r *Reflector) reflectEx(t reflect.Type, isOpt bool, n string, forFields bool) []*AvroSchema {
	ret := r.reflectType(t, false)

	// optional field
	if isOpt || r.BeBackwardTransitive {
		null := interface{}(nil)
		return []*AvroSchema{{Name: n, Type: []any{"null", ret}, Default: &null}}
	}

	// primitive type
	if reflect.TypeOf(ret).Kind() == reflect.String {
		return []*AvroSchema{{Name: n, Type: ret}}
	}

	ret = r.reflectType(t, forFields)
	result, ok := ret.(*AvroSchema)
	// made by extension, i.e., a slice
	if !ok {
		if slice, ok := ret.([]*AvroSchema); ok {
			return slice
		}
		return nil // FIXME: no error handle
	}

	// the rest is single schema
	result.Name = n
	return []*AvroSchema{result}
}

func (r *Reflector) ReflectFromType(v any) (string, error) {
	t := reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	data := r.handleRecord(t, false)

	return StructToJson(data)
}

/*
For customizing mapper, etc.
*/
func (r *Reflector) Reflect(v any) (string, error) {
	return r.ReflectFromType(v)
}

func Reflect(v any) (string, error) {
	r := &Reflector{}

	return r.ReflectFromType(v)
}

func (r *Reflector) ReflectFromTypeWithArray(v any) (string, error) {
	t := reflect.TypeOf(v)

	data := r.handleArray(t, false)

	return StructToJson(data)
}

/*
For customizing mapper, etc.
*/
func (r *Reflector) ReflectWithArray(v any) (string, error) {
	return r.ReflectFromTypeWithArray(v)
}

func ReflectWithArray(v any) (string, error) {
	r := &Reflector{}

	return r.ReflectFromTypeWithArray(v)
}
