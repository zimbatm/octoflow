// Package storagetest has an object store in memory for tests.
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
)

// Store is a storage.StreamStore in memory. The zero value is empty and
// ready to use.
type Store struct {
	mu    sync.Mutex
	files map[string][]byte
	gets  []string

	// OnGet may replace what Get returns for a key
	OnGet func(key string, data []byte) []byte
	// OnPutStream may replace what PutStream stores, or fail it
	OnPutStream func(key string, data []byte) ([]byte, error)
}

func (s *Store) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[key]
	if !ok {
		return nil, errors.New("no such key: " + key)
	}
	s.gets = append(s.gets, key)
	if s.OnGet != nil {
		data = s.OnGet(key, data)
	}
	return data, nil
}

func (s *Store) Put(_ context.Context, key string, data []byte) error {
	s.Set(key, data)
	return nil
}

func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, key)
	return nil
}

type handle struct{ *bytes.Reader }

func (handle) Close() error { return nil }

func (s *Store) Open(_ context.Context, key string) (io.ReadSeekCloser, error) {
	data, ok := s.File(key)
	if !ok {
		return nil, errors.New("no such key: " + key)
	}
	return handle{bytes.NewReader(data)}, nil
}

func (s *Store) PutStream(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if s.OnPutStream != nil {
		if data, err = s.OnPutStream(key, data); err != nil {
			return err
		}
	}
	s.Set(key, data)
	return nil
}

// Set stores data under key.
func (s *Store) Set(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files == nil {
		s.files = map[string][]byte{}
	}
	s.files[key] = data
}

// File returns what is stored under key.
func (s *Store) File(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[key]
	return data, ok
}

// Keys returns every key, sorted.
func (s *Store) Keys() []string {
	keys, _ := s.List(context.Background(), "")
	return keys
}

// Gets returns the keys that Get read, in order.
func (s *Store) Gets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.gets...)
}
