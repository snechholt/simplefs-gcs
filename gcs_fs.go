// Package simplefs_gcs implements the simplefs interface on top of
// Google Cloud Storage.

package simplefs_gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/snechholt/simplefs"
	"google.golang.org/api/iterator"
	"io"
	"path"
	"sort"
	"strings"
)

func New(c context.Context, bucket, prefix string) simplefs.FS {
	client, err := storage.NewClient(c)
	if err != nil {
		return &fileSystem{err: err}
	}
	return &fileSystem{c: c, bucket: client.Bucket(bucket), prefix: prefix}
}

type fileSystem struct {
	c      context.Context
	bucket *storage.BucketHandle
	prefix string
	err    error
}

func (fs *fileSystem) Create(name string) (io.WriteCloser, error) {
	if fs.err != nil {
		return nil, fs.err
	}
	filename := path.Join(fs.prefix, name)
	w := fs.bucket.Object(filename).NewWriter(fs.c)
	return w, nil
}

func (fs *fileSystem) Append(name string) (io.WriteCloser, error) {
	r, err := fs.Open(name)
	if err != nil && err != simplefs.ErrNotFound {
		return nil, err
	}

	w, err := fs.Create(name)
	if err != nil {
		return nil, err
	}

	if r != nil {
		if _, err := io.Copy(w, r); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (fs *fileSystem) Open(name string) (simplefs.File, error) {
	filename := path.Join(fs.prefix, name)
	r, err := fs.bucket.Object(filename).NewReader(fs.c)
	if err == storage.ErrObjectNotExist {
		return nil, simplefs.ErrNotFound
	}
	return &file{r: r}, err
}

func (fs *fileSystem) ReadDir(dir string) ([]simplefs.DirEntry, error) {
	prefix := path.Join(fs.prefix, dir)
	it := fs.bucket.Objects(fs.c, &storage.Query{Prefix: prefix})
	n := len(prefix)
	var names sort.StringSlice
	var found bool
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		found = true

		// If we looked up a file directly, this is not a directory. Return not found
		if attrs.Name == prefix {
			return nil, simplefs.ErrNotFound
		}

		// Strip the prefix. E.g. parent-folder/dir/file.txt becomes file.txt
		name := attrs.Name[n+1:]

		if strings.Count(name, "/") > 0 {
			// This is a listing of a file within a sub-directory, e.g. dir = "a",
			// attrs.Name = "a/b/c.txt", name = "b/c.txt".
			// Do not include
			continue
		}
		names = append(names, name)
	}

	// If we didn't iterate over any files, this wasn't a directory.
	if !found {
		return nil, simplefs.ErrNotFound
	}

	sort.Sort(names)

	entries := make([]simplefs.DirEntry, len(names))
	for i, name := range names {
		entries[i] = &dirEntry{name: name}
	}
	return entries, nil
}

type file struct {
	r io.ReadCloser
}

func (f *file) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

func (f *file) Close() error {
	return f.r.Close()
}

func (f *file) ReadDir(n int) ([]simplefs.DirEntry, error) {
	return nil, fmt.Errorf("not implemented")
}

type dirEntry struct {
	name string
}

func (entry *dirEntry) Name() string {
	return entry.name
}

func (entry *dirEntry) IsDir() bool {
	return false
}
