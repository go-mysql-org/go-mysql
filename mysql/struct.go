package mysql

import (
	"fmt"
	"reflect"
)

//decode the row data and set to v
//v must be a pointer to a struct
func (r *Resultset) DecodeRow(row int, v interface{}) error {
	if row < 0 || row >= len(r.Data) {
		return fmt.Errorf("invalid row %d", row)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("invalid type %s", rv.Kind().String())
	}

	pv := rv.Elem()
	pt := pv.Type()

	if pv.Kind() != reflect.Struct {
		return fmt.Errorf("invalid type %s, not struct", pv.Kind().String())
	}

	for i := 0; i < pv.NumField(); i++ {
		name := pt.Field(i).Tag.Get("mysql")
		if len(name) == 0 {
			name = pt.Field(i).Name
		}

		f := pv.Field(i)
		switch f.Kind() {
		case reflect.Bool:
			if d, err := r.GetBoolByName(row, name); err == nil {
				f.SetBool(d)
			}
		case reflect.Int:
			if d, err := r.GetIntByName(row, name); err == nil {
				f.SetInt(d)
			}
		case reflect.Int8:
			if d, err := r.GetIntByName(row, name); err == nil {
				f.SetInt(d)
			}
		case reflect.Int16:
			if d, err := r.GetIntByName(row, name); err == nil {
				f.SetInt(d)
			}
		case reflect.Int32:
			if d, err := r.GetIntByName(row, name); err == nil {
				f.SetInt(d)
			}
		case reflect.Int64:
			if d, err := r.GetIntByName(row, name); err == nil {
				f.SetInt(d)
			}
		case reflect.Uint:
			if d, err := r.GetUintByName(row, name); err == nil {
				f.SetUint(d)
			}
		case reflect.Uint8:
			if d, err := r.GetUintByName(row, name); err == nil {
				f.SetUint(d)
			}
		case reflect.Uint16:
			if d, err := r.GetUintByName(row, name); err == nil {
				f.SetUint(d)
			}
		case reflect.Uint32:
			if d, err := r.GetUintByName(row, name); err == nil {
				f.SetUint(d)
			}
		case reflect.Uint64:
			if d, err := r.GetUintByName(row, name); err == nil {
				f.SetUint(d)
			}
		case reflect.String:
			if d, err := r.GetStringByName(row, name); err == nil {
				f.SetString(d)
			}
		case reflect.Slice:
			if d, err := r.GetStringByName(row, name); err == nil {
				f.SetBytes([]byte(d))
			}
		case reflect.Float32:
			if d, err := r.GetFloatByName(row, name); err == nil {
				f.SetFloat(d)
			}
		case reflect.Float64:
			if d, err := r.GetFloatByName(row, name); err == nil {
				f.SetFloat(d)
			}
		default:
			//only support plain data
		}
	}

	return nil
}
