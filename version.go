package gdbx

import "fmt"

// Version constants
const (
	// Major is the major version number
	Major = 0

	// Minor is the minor version number
	Minor = 1

	// Patch is the patch version number
	Patch = 0
)

// VersionInfo contains version information (mdbx-go compatibility).
type VersionInfo struct {
	Major    uint8
	Minor    uint8
	Release  uint8
	Revision uint16
	Git      string
	Describe string
	Datetime string
	Tree     string
	Commit   string
	Sourcery string
}

// BuildInfo contains build information (mdbx-go compatibility).
type BuildInfo struct {
	Datetime string
	Target   string
	Options  string
	Compiler string
	Flags    string
}

// Version returns the version string of gdbx.
// Format is similar to mdbx-go for compatibility.
func Version() string {
	return "gdbx 0.1.0 (pure Go MDBX-compatible implementation)"
}

// GetVersionInfo returns version information (mdbx-go compatibility).
func GetVersionInfo() VersionInfo {
	return VersionInfo{
		Major:    Major,
		Minor:    Minor,
		Release:  Patch,
		Revision: 0,
		Git:      "",
		Describe: fmt.Sprintf("v%d.%d.%d", Major, Minor, Patch),
		Datetime: "",
		Tree:     "",
		Commit:   "",
		Sourcery: "gdbx",
	}
}

// GetBuildInfo returns build information (mdbx-go compatibility).
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		Datetime: "",
		Target:   "pure-go",
		Options:  "",
		Compiler: "gc",
		Flags:    "",
	}
}
