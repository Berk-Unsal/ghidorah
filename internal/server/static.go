package server

import (
	"embed"
	"io/fs"
	"log"
	"mime"
)

//go:embed all:build
var frontendFS embed.FS

var staticFS = mustStaticFS()

func init() {
	_ = mime.AddExtensionType(".js", "application/javascript")
	_ = mime.AddExtensionType(".css", "text/css")
}

func mustStaticFS() fs.FS {
	content, err := fs.Sub(frontendFS, "build")
	if err != nil {
		log.Fatalf("load embedded static assets: %v", err)
	}

	return content
}
