package utils

import "encoding/json"

func JS(obj interface{}) string {
	data, _ := json.MarshalIndent(obj, "", "    ")
	return string(data)
}
