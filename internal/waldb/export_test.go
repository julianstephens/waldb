package waldb

// ManifestInitFn exposes the manifest init function variable so tests can
// replace it with a stub to simulate manifest initialization failures.
var ManifestInitFn = &manifestInitFn

// ResetManifestInitFn restores ManifestInitFn to the function registered by
// the manifest package (set during its init()).
func ResetManifestInitFn() {
	// Re-trigger registration by calling RegisterManifestInit with the saved default.
	manifestInitFn = defaultManifestInitFn
}
