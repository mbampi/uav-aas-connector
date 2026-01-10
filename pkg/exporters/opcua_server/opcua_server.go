package opcua_server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/awcullen/opcua/server"
	"github.com/awcullen/opcua/ua"
)

// Exporter implements an OPC UA Server that hosts the AAS data.
type Exporter struct {
	server   *server.Server
	core     *core.AASCore
	endpoint string
	nodeMap  map[string]*server.VariableNode // Cache lookup: "SubmodelID.PropertyPath" -> Node
	mu       sync.RWMutex
}

// NewExporter creates a new OPC UA Server exporter.
func NewExporter(cfg config.ExporterConfig, aasCore *core.AASCore) (*Exporter, error) {
	if cfg.Port == 0 {
		cfg.Port = 4840 // Default OPC UA port
	}
	host := "0.0.0.0"
	if cfg.Host != "" {
		host = cfg.Host
	}

	certFile := "./pki/server.crt"
	keyFile := "./pki/server.key"
	if err := ensurePKI(certFile, keyFile); err != nil {
		return nil, fmt.Errorf("failed to generate certs: %w", err)
	}

	endpoint := fmt.Sprintf("opc.tcp://%s:%d", host, cfg.Port)

	// Create the OPC UA Server
	srv, err := server.New(
		ua.ApplicationDescription{
			ApplicationURI:  fmt.Sprintf("urn:%s:UAV-AAS-Server", host),
			ProductURI:      "http://github.com/uav-aas-connector",
			ApplicationName: ua.LocalizedText{Text: "UAV AAS Connector Server"},
			ApplicationType: ua.ApplicationTypeServer,
		},
		certFile,
		keyFile,
		endpoint,
		server.WithAnonymousIdentity(true),
		server.WithSecurityPolicyNone(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OPC UA server: %w", err)
	}

	return &Exporter{
		server:   srv,
		core:     aasCore,
		endpoint: endpoint,
		nodeMap:  make(map[string]*server.VariableNode),
	}, nil
}

// Start builds the address space and starts the server.
func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("OPCUA Server building address space")

	if err := e.buildAddressSpace(); err != nil {
		return fmt.Errorf("failed to build address space: %w", err)
	}

	// Start the Server listener in a goroutine
	go func() {
		slog.Info("OPCUA Server listening", "endpoint", e.endpoint)
		if err := e.server.ListenAndServe(); err != nil {
			slog.Error("OPCUA Server stopped", "error", err)
		}
	}()

	// Process incoming updates
	for {
		select {
		case <-ctx.Done():
			return e.Stop()
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			e.updateNode(event)
		}
	}
}

// Stop gracefully shuts down the server.
func (e *Exporter) Stop() error {
	slog.Info("OPCUA Server stopping")
	return e.server.Close()
}

// buildAddressSpace maps the AAS Submodels and Properties to OPC UA Nodes.
func (e *Exporter) buildAddressSpace() error {
	nm := e.server.NamespaceManager()

	nsIndex := nm.Add("urn:uav-aas-connector:model")

	submodels := e.core.GetAllSubmodels()
	for _, sm := range submodels {
		if sm.IDShort() == nil {
			continue
		}
		smID := *sm.IDShort()

		smNodeID := ua.NodeIDString{NamespaceIndex: nsIndex, ID: smID}

		// References: ObjectsFolder Organizes smNode
		refs := []ua.Reference{
			{
				ReferenceTypeID: ua.NodeIDNumeric{ID: 35}, // Organizes
				IsInverse:       true,
				TargetID:        ua.ExpandedNodeID{NodeID: ua.NodeIDNumeric{ID: 85}}, // ObjectsFolder
			},
		}

		smNode := server.NewObjectNode(
			e.server,
			smNodeID,
			ua.QualifiedName{NamespaceIndex: nsIndex, Name: smID},
			ua.LocalizedText{Text: smID, Locale: "en"},
			ua.LocalizedText{Text: smID, Locale: "en"},
			nil, // RolePermissions
			refs,
			0, // EventNotifier
		)

		if err := nm.AddNode(smNode); err != nil {
			return fmt.Errorf("failed to add submodel node %s: %w", smID, err)
		}

		elements := flattenElements(sm.SubmodelElements(), "")
		for path, elem := range elements {
			prop, ok := elem.(aastypes.IProperty)
			if !ok {
				continue
			}

			nodeIDPath := fmt.Sprintf("%s/%s", smID, strings.ReplaceAll(path, ".", "/"))
			varNodeID := ua.NodeIDString{NamespaceIndex: nsIndex, ID: nodeIDPath}

			initialVal := ""
			if prop.Value() != nil {
				initialVal = *prop.Value()
			}
			vt := prop.ValueType()
			val, _ := e.convertValue(initialVal, vt)
			dataTypeID := e.getDataTypeNodeID(vt)

			varRefs := []ua.Reference{
				{
					ReferenceTypeID: ua.NodeIDNumeric{ID: 47}, // HasComponent
					IsInverse:       true,
					TargetID:        ua.ExpandedNodeID{NodeID: smNodeID},
				},
			}

			// Pass value directly to DataValue
			dataVal := ua.DataValue{
				Value:           val,
				StatusCode:      ua.StatusCode(0), // Good
				SourceTimestamp: time.Now(),
			}

			varNode := server.NewVariableNode(
				e.server,
				varNodeID,
				ua.QualifiedName{NamespaceIndex: nsIndex, Name: path},
				ua.LocalizedText{Text: path, Locale: "en"},
				ua.LocalizedText{Text: path, Locale: "en"},
				nil, // RolePermissions
				varRefs,
				dataVal,
				dataTypeID,
				-1,  // ValueRankScalar
				nil, // ArrayDimensions
				1,   // AccessLevel CurrentRead
				0.0, // MinimumSamplingInterval
				false,
				nil, // HistoryReadWriter
			)

			if err := nm.AddNode(varNode); err != nil {
				slog.Error("OPCUA Server failed to add variable node", "path", path, "error", err)
				continue
			}

			cacheKey := fmt.Sprintf("%s.%s", smID, path)
			e.mu.Lock()
			e.nodeMap[cacheKey] = varNode
			e.mu.Unlock()
		}
	}
	return nil
}

// updateNode finds the node in the map and updates its value.
func (e *Exporter) updateNode(event *core.AASUpdateEvent) {
	cacheKey := fmt.Sprintf("%s.%s", event.SubmodelID, event.PropertyID)

	e.mu.RLock()
	node, exists := e.nodeMap[cacheKey]
	e.mu.RUnlock()

	if !exists {
		return
	}

	dt, err := e.core.GetElementDataType(event.SubmodelID, event.PropertyID)
	if err != nil {
		slog.Error("OPCUA Server unknown data type", "key", cacheKey, "error", err)
		return
	}

	val, err := e.convertValue(event.Value, dt)
	if err != nil {
		slog.Error("OPCUA Server conversion error", "key", cacheKey, "error", err)
		return
	}

	// Pass value directly to DataValue
	node.SetValue(ua.DataValue{
		Value:           val,
		StatusCode:      ua.StatusCode(0),
		SourceTimestamp: event.Timestamp,
		ServerTimestamp: time.Now(),
	})

	instrumentation.Record("OPCUA_SERVER_UPDATE", event.PropertyID, event.Timestamp)
}

func flattenElements(elements []aastypes.ISubmodelElement, prefix string) map[string]aastypes.ISubmodelElement {
	result := make(map[string]aastypes.ISubmodelElement)
	for _, el := range elements {
		if el.IDShort() == nil {
			continue
		}
		name := *el.IDShort()
		fullPath := name
		if prefix != "" {
			fullPath = prefix + "." + name
		}

		if prop, ok := el.(aastypes.IProperty); ok {
			result[fullPath] = prop
		} else if coll, ok := el.(aastypes.ISubmodelElementCollection); ok {
			sub := flattenElements(coll.Value(), fullPath)
			for k, v := range sub {
				result[k] = v
			}
		}
	}
	return result
}

func (e *Exporter) convertValue(val any, aasType aastypes.DataTypeDefXSD) (any, error) {
	switch aasType {
	case aastypes.DataTypeDefXSDBoolean:
		return toBool(val)
	case aastypes.DataTypeDefXSDDouble:
		return toFloat64(val)
	case aastypes.DataTypeDefXSDFloat:
		v, err := toFloat64(val)
		return float32(v), err
	case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger:
		v, err := toInt64(val)
		return int32(v), err
	case aastypes.DataTypeDefXSDLong:
		return toInt64(val)
	case aastypes.DataTypeDefXSDString:
		return fmt.Sprintf("%v", val), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

func (e *Exporter) getDataTypeNodeID(aasType aastypes.DataTypeDefXSD) ua.NodeID {
	switch aasType {
	case aastypes.DataTypeDefXSDBoolean:
		return ua.NodeIDNumeric{ID: 1} // Boolean
	case aastypes.DataTypeDefXSDDouble:
		return ua.NodeIDNumeric{ID: 11} // Double
	case aastypes.DataTypeDefXSDFloat:
		return ua.NodeIDNumeric{ID: 10} // Float
	case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger:
		return ua.NodeIDNumeric{ID: 6} // Int32
	case aastypes.DataTypeDefXSDLong:
		return ua.NodeIDNumeric{ID: 8} // Int64
	case aastypes.DataTypeDefXSDString:
		return ua.NodeIDNumeric{ID: 12} // String
	default:
		return ua.NodeIDNumeric{ID: 12} // String
	}
}

// Helper Conversion Functions

func toInt64(val any) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		var i int64
		_, err := fmt.Sscanf(v, "%d", &i)
		return i, err
	default:
		return 0, nil
	}
}

func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		var f float64
		_, err := fmt.Sscanf(v, "%f", &f)
		return f, err
	default:
		return 0.0, nil
	}
}

func toBool(val any) (bool, error) {
	if v, ok := val.(bool); ok {
		return v, nil
	}
	if s, ok := val.(string); ok {
		return strings.ToLower(s) == "true", nil
	}
	return false, nil
}

// ensurePKI checks for existing cert/key files or generates new ones.
func ensurePKI(certFile, keyFile string) error {
	// 1. Check if files already exist
	if _, err := os.Stat(certFile); err == nil {
		return nil // File exists, nothing to do
	}

	// 2. Ensure directory exists
	if err := os.MkdirAll("pki", 0755); err != nil {
		return fmt.Errorf("failed to create pki dir: %w", err)
	}

	slog.Info("PKI generating new self-signed certificate")

	// 3. Create Private Key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// 4. Create Certificate Template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"UAV AAS Connector"},
			CommonName:   "UAV-AAS-Server",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// Add localhost IP and URI
	template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))
	template.URIs = []*url.URL{{Scheme: "urn", Opaque: "localhost:UAV-AAS-Server"}}

	// 5. Create the Certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	// 6. Save Certificate to disk
	certOut, err := os.Create(certFile)
	if err != nil {
		return err
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return err
	}

	// 7. Save Private Key to disk
	keyOut, err := os.Create(keyFile)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes}); err != nil {
		return err
	}

	slog.Info("PKI certificate generated successfully")
	return nil
}
