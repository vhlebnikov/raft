package server

import (
	"context"
	"net/http"
	"time"
)

type HTTPServer struct {
	s *http.Server
}

func NewHTTPServer(port string, handler http.Handler) *HTTPServer {
	return &HTTPServer{
		s: &http.Server{
			Handler:           handler,
			Addr:              port,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

func (s *HTTPServer) Start() error {
	return s.s.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}
