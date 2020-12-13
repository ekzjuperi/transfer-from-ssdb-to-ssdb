package configuration

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/hashicorp/consul/api"
)

var (
	// Key-value manipulation object
	kv *api.KV
	// configuration mode, adds additional folder like modePrefix/...
	modePrefix string
)

// Initializes consul client
// mode is used additional namespace
// to differ test, canary, prod services
func Setup(mode ...string) error {
	// DefaultConfig() method already has processing for environment vars.
	// Set CONSUL_HTTP_ADDR in evironment to configure remote address
	// for consul server, otherwise 127.0.0.1:8500 will be used.
	cli, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return err
	}
	kv = cli.KV()
	// check if mode provided
	if len(mode) == 1 {
		modePrefix = mode[0]
	}

	return nil
}

// Fills the entire config declared in interface
// should be passed pointer to struct
// fill is recursive and sets all setable
// embeddable structs
func Fill(cfg interface{}, namespace ...string) error {
	// we can only work with struct and it should be pointer
	// so we could set values to it
	if reflect.TypeOf(cfg).Kind() != reflect.Ptr {
		return ErrStructPtrReq
	}
	if reflect.Indirect(reflect.ValueOf(cfg)).Kind() != reflect.Struct {
		return ErrStructPtrReq
	}

	// Folder for searching inner keys
	folder := reflect.TypeOf(cfg).Elem().Name()

	// if namespace provided set as root folder
	if len(namespace) == 1 {
		folder = namespace[0] + "/" + folder
	}

	elem := reflect.ValueOf(cfg).Elem()
	for i := 0; i < elem.NumField(); i++ {

		// add boilerplate so we do not panic
		f := elem.Field(i)

		// check if we can obtain add
		// skip all not addressable fields
		// TODO do not skip return some error
		if !f.CanAddr() {
			continue
		}

		faddr := f.Addr()
		// check if we can use it without panic
		if !faddr.CanInterface() {
			continue
		}
		// this will panic if field unexported
		// so above check required
		addri := faddr.Interface()

		if f.Kind() == reflect.Struct {
			// go recursive with namespace
			err := Fill(addri, folder)
			if err != nil {
				return err
			}
			// skip if field is struct
			// only values for struct properties should be set
			continue
		}
		// use field name as a key
		key := elem.Type().Field(i).Name
		val, err := Get(folder + "/" + key)
		if err != nil {
			return err
		}

		switch elem.Field(i).Interface().(type) {
		case bool:
			v, err := strconv.ParseBool(val)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidBool, val)
			}
			elem.Field(i).SetBool(v)
		case int, int32:
			v, err := strconv.ParseInt(val, 0, 32)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidInt, val)
			}
			elem.Field(i).SetInt(v)
		case int64:
			v, err := strconv.ParseInt(val, 0, 64)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidInt64, val)
			}
			elem.Field(i).SetInt(v)
		case uint16:
			v, err := strconv.ParseUint(val, 0, 16)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidUint16, val)
			}
			elem.Field(i).SetUint(v)
		case uint, uint32:
			v, err := strconv.ParseUint(val, 0, 32)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidUint, val)
			}
			elem.Field(i).SetUint(v)
		case uint64:
			v, err := strconv.ParseUint(val, 0, 64)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidUint64, val)
			}
			elem.Field(i).SetUint(v)
		case float64:
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return fmt.Errorf("%v: %v", ErrInvalidFloat64, val)
			}
			elem.Field(i).SetFloat(v)
		case string:
			elem.Field(i).SetString(val)
		}
	}
	return nil
}

// Gets value from specified key
// if key not found it will recursively
// search in above folders
func Get(key string) (string, error) {
	var err error
	if len(key) == 0 {
		return "", ErrEmptyKey
	}

	var pair *api.KVPair
	// check if modePrefix set then try with prefix first
	if modePrefix != "" {
		pair, _, err = kv.Get(modePrefix+"/"+key, nil)
	}
	// Simply get value by key.
	// Skip query metadata, pass nil query options.
	// if previous get with prefixed key returned result
	// then just skip it. If not make another request
	// without prefix
	if pair == nil {
		pair, _, err = kv.Get(key, nil)
		if err != nil {
			return "", err
		}
	}

	// Critical check if pair is not empty pointer return
	if pair != nil {
		return string(pair.Value), nil

	}
	// if not found look in above folder
	// split current Key to two parts and use last part as a new Key
	dirs := strings.SplitN(key, "/", 2)
	// check if it has above folder
	if len(dirs) == 2 && strings.Contains(dirs[1], "/") {
		// use last part as new path
		return Get(dirs[1])
	}

	// if pair is nil and did not find in above folder
	// return error
	return "", fmt.Errorf("%v: %v", ErrKeyNotFound, key)
}

// Sets value for key
func Set(key string, value string) error {
	pair := &api.KVPair{
		Key:   key,
		Value: []byte(value),
	}
	_, err := kv.Put(pair, nil)
	return err
}
