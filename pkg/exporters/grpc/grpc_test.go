package grpc

import (
	"testing"
	"time"

	"uav-aas-connector/pkg/core"
	pb "uav-aas-connector/pkg/exporters/grpc/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRequest(t *testing.T) {
	ts := time.Unix(0, 1234567890)
	event := func(v any) *core.AASUpdateEvent {
		return &core.AASUpdateEvent{SubmodelID: "Nav", PropertyID: "Pos.Lat", Value: v, Timestamp: ts}
	}

	t.Run("metadata", func(t *testing.T) {
		req := buildRequest(event(1.0))
		assert.Equal(t, "Nav", req.SubmodelId)
		assert.Equal(t, "Pos.Lat", req.PropertyPath)
		assert.Equal(t, int64(1234567890), req.TimestampNs)
	})

	t.Run("float64", func(t *testing.T) {
		v, ok := buildRequest(event(float64(15.5))).Value.(*pb.UpdateRequest_DoubleVal)
		require.True(t, ok)
		assert.Equal(t, 15.5, v.DoubleVal)
	})
	t.Run("float32", func(t *testing.T) {
		v, ok := buildRequest(event(float32(2.5))).Value.(*pb.UpdateRequest_DoubleVal)
		require.True(t, ok)
		assert.Equal(t, 2.5, v.DoubleVal)
	})
	t.Run("int32", func(t *testing.T) {
		v, ok := buildRequest(event(int32(7))).Value.(*pb.UpdateRequest_IntVal)
		require.True(t, ok)
		assert.Equal(t, int32(7), v.IntVal)
	})
	// The MAVLink adapter normalizes all signed/unsigned integer telemetry to
	// int64/uint64 (see pkg/adapters/mavlink normalizeReflectValue), so these
	// are the types most integer properties actually arrive as - unlike
	// int/int32 above, which the pipeline never produces.
	t.Run("int64", func(t *testing.T) {
		v, ok := buildRequest(event(int64(9223372036854775807))).Value.(*pb.UpdateRequest_Int64Val)
		require.True(t, ok)
		assert.Equal(t, int64(9223372036854775807), v.Int64Val)
	})
	t.Run("uint64", func(t *testing.T) {
		v, ok := buildRequest(event(uint64(18446744073709551615))).Value.(*pb.UpdateRequest_Uint64Val)
		require.True(t, ok)
		assert.Equal(t, uint64(18446744073709551615), v.Uint64Val)
	})
	t.Run("uint32", func(t *testing.T) {
		v, ok := buildRequest(event(uint32(42))).Value.(*pb.UpdateRequest_Uint64Val)
		require.True(t, ok)
		assert.Equal(t, uint64(42), v.Uint64Val)
	})
	t.Run("string", func(t *testing.T) {
		v, ok := buildRequest(event("hi")).Value.(*pb.UpdateRequest_StringVal)
		require.True(t, ok)
		assert.Equal(t, "hi", v.StringVal)
	})
	t.Run("bool", func(t *testing.T) {
		v, ok := buildRequest(event(true)).Value.(*pb.UpdateRequest_BoolVal)
		require.True(t, ok)
		assert.True(t, v.BoolVal)
	})
}
