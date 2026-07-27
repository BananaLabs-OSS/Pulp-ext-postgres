package postgresext

import (
	"testing"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

func TestStorageSQLiteCapabilityDeclaresPostgresProvider(t *testing.T) {
	t.Parallel()

	for _, capability := range ext.All() {
		if capability.Name != "storage.sqlite" {
			continue
		}
		if capability.Provider != "github.com/BananaLabs-OSS/Pulp-ext-postgres" {
			t.Fatalf("provider = %q, want github.com/BananaLabs-OSS/Pulp-ext-postgres", capability.Provider)
		}
		return
	}
	t.Fatal("storage.sqlite capability was not registered")
}
