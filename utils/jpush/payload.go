package jpush

import (
	"encoding/json"
)

// If AlertStruct set to any instance, it will ignore any content in Alert when send to iOS.
// To use simple string Alert, make sure AlertStruct's value is nil.

type Payload struct {
	Alert string `json:"message,omitempty"`
	Title string `json:"title,omitempty"`

	customProperty map[string]interface{}
}

// Set a custom key with value, overwriting any existed key. If key is "aps", do nothing.
func (l *Payload) SetCustom(key string, value interface{}) {
	if l.customProperty == nil {
		l.customProperty = make(map[string]interface{})
	}
	l.customProperty[key] = value
}

// Get a custom key's value. If key is "aps", return nil.
func (l *Payload) GetCustom(key string) interface{} {
	if l.customProperty == nil {
		return nil
	}
	return l.customProperty[key]
}

func (l Payload) MarshalJSON() ([]byte, error) {
	p := make(map[string]interface{})
	p["message"] = l.Alert
	p["title"] = l.Title
	if l.customProperty != nil {
		p["extras"] = l.customProperty
	}
	return json.Marshal(p)
}
