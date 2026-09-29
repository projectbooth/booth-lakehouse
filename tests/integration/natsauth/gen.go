//go:build ignore

// Generates this module's event-bus test fixtures with booth-core's OWN credential code
// (internal/natsauth), so the integration tests run against a NATS server in exactly the JWT
// operator/account mode core deploys (ADR 0050), with a credential derived from this module's
// real manifest `events` field by core's own GrantsFor. hack/gen-nats-fixtures.sh builds it inside
// a throwaway copy of booth-core (Go's internal/ rule forbids importing it from here).
//
// Test-only keys, valid for ten years, trusted by nothing but the compose file's NATS server.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	boothv1alpha1 "github.com/projectbooth/booth-core/api/v1alpha1"
	"github.com/projectbooth/booth-core/internal/natsauth"
)

func must[T any](v T, err error) T {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return v
}

func main() {
	out := os.Args[1]
	a := must(natsauth.NewAuthority(must(natsauth.GenerateSeeds())))
	conf := must(a.ServerConfig())
	ttl := 10 * 365 * 24 * time.Hour

	// Exactly what charts/booth-lakehouse/templates/boothmodule.yaml declares.
	lakehouse := must(natsauth.GrantsFor(&boothv1alpha1.EventBusAccess{Publish: []string{"table.created", "table.updated", "table.deleted"}}))
	// A subscriber shaped like booth-catalog would be for table.* (what the tests read with).
	catalog := must(natsauth.GrantsFor(&boothv1alpha1.EventBusAccess{Subscribe: []string{"table.*"}}))

	files := map[string][]byte{
		"auth.conf":      []byte(conf),
		"core.creds":     must(a.MintUser("booth-core", natsauth.CoreGrants(), ttl)).Creds,
		"lakehouse.creds": must(a.MintUser("lakehouse", lakehouse, ttl)).Creds,
		"catalog.creds":  must(a.MintUser("catalog", catalog, ttl)).Creds,
	}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(out, name), b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
