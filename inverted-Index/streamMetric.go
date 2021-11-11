package invertedindex

import "bytes"

func (sm *StreamMetric) ToPromString() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	var name string
	for i, label := range sm.Labels {
		if label.Name == "__name__" {
			name = label.Value
			continue
		}
		buffer.WriteString(label.Name + "=" + label.Value)
		if i != len(sm.Labels)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("}")
	if name != "" {
		return name + buffer.String()
	}

	return buffer.String()
}
