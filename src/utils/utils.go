package utils

import (
	"encoding/json"
)

func IsValidJSON(data []byte) bool {
    var js interface{}
    return json.Unmarshal(data, &js) == nil
}
