// Package admin provides embedded admin UI static assets for the OJS backend.
// The UI is a pre-built React SPA that connects to the admin API endpoints.
//
// To update the UI, rebuild ojs-admin-ui and copy dist/ files here:
//
//	cd ../ojs-admin-ui && npm run build
//	cp dist/* ../ojs-backend-redis/internal/admin/dist/
package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed dist/*
var distFS embed.FS

// Handler returns an http.Handler that serves the admin UI.
// It serves static files from the embedded dist/ directory and
// falls back to index.html for SPA client-side routing.
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("admin: failed to create sub filesystem: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Try to serve the exact file
		if path != "" {
			if f, err := sub.Open(path); err == nil {
				f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// SPA fallback: serve index.html for all non-file routes
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
