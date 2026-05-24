package db

import "github.com/julianstephens/waldb/internal/waldb/manifest"

// ManifestInitFn exposes the manifest init function variable so tests can
// replace it with a stub to simulate manifest initialization failures.
var ManifestInitFn = &manifestInitFn

// ResetManifestInitFn restores ManifestInitFn to the real manifest.Init.
func ResetManifestInitFn() {
	manifestInitFn = manifest.Init
}
