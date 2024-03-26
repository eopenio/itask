package yjson

import jsoniter "github.com/json-iterator/go"

var TaskJson = jsoniter.Config{
	EscapeHTML:             true,
	ValidateJsonRawMessage: true,
	TagKey:                 "yjson",
}.Froze()

func SetDebug() {
	TaskJson = jsoniter.Config{
		EscapeHTML:             true,
		ValidateJsonRawMessage: true,
		TagKey:                 "yjson",
		SortMapKeys:            true,
	}.Froze()
}
