package util

import (
	"github.com/eopenio/itask/v3/util/yjson"
	"reflect"
)

func GoVarToTaskJson(v interface{}) (string, error) {
	b, err := yjson.TaskJson.Marshal(v)
	return string(b), err
}

func GoVarsToTaskJsonSlice(args ...interface{}) ([]string, error) {
	var r = make([]string, len(args))
	for i, v := range args {
		yJsonStr, err := GoVarToTaskJson(v)
		if err != nil {
			return r, err
		}
		r[i] = yJsonStr
	}
	return r, nil
}

func GoValuesToTaskJsonSlice(values []reflect.Value) ([]string, error) {
	var r = make([]string, len(values))
	for i, v := range values {
		s, err := GoVarToTaskJson(v.Interface())
		if err != nil {
			return r, err
		}
		r[i] = s

	}
	return r, nil
}

func GetCallInArgs(funcValue reflect.Value, funcArgs []string, inStart int) ([]reflect.Value, error) {

	var inArgs = make([]reflect.Value, funcValue.Type().NumIn()-inStart)
	for i := inStart; i < funcValue.Type().NumIn(); i++ {
		if i-inStart >= len(funcArgs) {
			break
		}
		inType := funcValue.Type().In(i)
		inValue := reflect.New(inType)
		// yjson to go value
		err := yjson.TaskJson.Unmarshal([]byte(funcArgs[i-inStart]), inValue.Interface())

		if err != nil {
			return inArgs, err
		}
		inArgs[i-inStart] = inValue.Elem()
	}
	return inArgs, nil
}
