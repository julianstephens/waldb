package manifest

import (
	"errors"
	"fmt"
)

type ManifestErrorKind int

const (
	ManifestErrorKindOpen ManifestErrorKind = iota + 1
	ManifestErrorKindNotFound
	ManifestErrorKindCorrupted
	ManifestErrorKindUnsupportedVersion
	ManifestErrorKindWorkingDirectory
	ManifestErrorKindEncode
	ManifestErrorKindDecode
	ManifestErrorKindWrite
	ManifestErrorKindAlreadyExists
	ManifestErrorKindUnknown
)

var (
	ErrManifestOpen               = errors.New("manifest: unable to open file")
	ErrManifestNotFound           = errors.New("manifest: file not found")
	ErrManifestCorrupted          = errors.New("manifest: file corrupted")
	ErrManifestUnsupportedVersion = errors.New("manifest: unsupported version")
	ErrManifestWorkingDirectory   = errors.New("manifest: unable to get working directory")
	ErrManifestEncode             = errors.New("manifest: unable to encode to JSON")
	ErrManifestDecode             = errors.New("manifest: unable to decode from JSON")
	ErrManifestWrite              = errors.New("manifest: unable to write to file")
	ErrManifestAlreadyExists      = errors.New("manifest: file already exists")
)

type ManifestError struct {
	Kind ManifestErrorKind
	Err  error
}

func (e *ManifestError) Error() string {
	return fmt.Sprintf("manifest error (%v): %v", e.Kind, e.Err)
}

func (e *ManifestError) Unwrap() error {
	switch e.Kind {
	case ManifestErrorKindOpen:
		return ErrManifestOpen
	case ManifestErrorKindNotFound:
		return ErrManifestNotFound
	case ManifestErrorKindCorrupted:
		return ErrManifestCorrupted
	case ManifestErrorKindUnsupportedVersion:
		return ErrManifestUnsupportedVersion
	case ManifestErrorKindWorkingDirectory:
		return ErrManifestWorkingDirectory
	case ManifestErrorKindEncode:
		return ErrManifestEncode
	case ManifestErrorKindDecode:
		return ErrManifestDecode
	case ManifestErrorKindWrite:
		return ErrManifestWrite
	case ManifestErrorKindAlreadyExists:
		return ErrManifestAlreadyExists
	default:
		return e.Err
	}
}
