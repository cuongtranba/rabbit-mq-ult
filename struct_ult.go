package worker

import "encoding/json"

func structToString(data interface{}) string {
	dataBuf, _ := json.Marshal(data)
	return string(dataBuf)
}
