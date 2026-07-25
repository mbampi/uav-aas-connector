package exporters

import "testing"

func TestRequiresLiveModelState(t *testing.T) {
	cases := map[string]bool{
		"http":         true,
		"opcua_client": false,
		"opcua_server": false,
		"mqtt":         false,
		"grpc":         false,
		"dummy":        false,
		"unknown":      false,
	}
	for exporterType, want := range cases {
		if got := RequiresLiveModelState(exporterType); got != want {
			t.Errorf("RequiresLiveModelState(%q) = %v, want %v", exporterType, got, want)
		}
	}
}
