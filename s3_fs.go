// Package s3 implements an S3 filesystem using afero
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/spf13/afero"
)

// directoryCacheEntry holds cached directory information
type directoryCacheEntry struct {
	fileInfos []os.FileInfo
	timestamp time.Time
}

// Fs is an FS object backed by S3.
type Fs struct {
	FileProps *UploadedFileProperties // FileProps define the file properties we want to set for all new files
	config    aws.Config              // Config for the client
	s3API     *s3.Client
	bucket    string // bucket name for backward compatibility with tests

	// Cache for directory listings to improve performance
	dirCache      map[string]*directoryCacheEntry
	dirCacheMutex sync.RWMutex
}

// UploadedFileProperties defines all the set properties applied to future files
type UploadedFileProperties struct {
	ACL          *string // ACL defines the right to apply
	CacheControl *string // CacheControl defines the Cache-Control header
	ContentType  *string // ContentType define the Content-Type header
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
// The bucket parameter is the S3 bucket name.
func NewFs(bucket string, cfg aws.Config) *Fs {
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &Fs{
		config:   cfg,
		s3API:    s3Client,
		bucket:   bucket,
		dirCache: make(map[string]*directoryCacheEntry),
	}
}

// NewFsWrapper creates a new FsWrapper object that implements afero.Fs with fixed bucket and rootPrefix.
func NewFsWrapper(cfg aws.Config, bucket, rootPrefix string) *FsWrapper {
	fs := NewFs(bucket, cfg)
	return &FsWrapper{
		Fs:         fs,
		Bucket:     bucket,
		RootPrefix: rootPrefix,
	}
}

// ErrNotImplemented is returned when this operation is not (yet) implemented
var ErrNotImplemented = errors.New("not implemented")

// ErrNotSupported is returned when this operations is not supported by S3
var ErrNotSupported = errors.New("s3 doesn't support this operation")

// ErrAlreadyOpened is returned when the file is already opened
var ErrAlreadyOpened = errors.New("already opened")

// ErrInvalidSeek is returned when the seek operation is not doable
var ErrInvalidSeek = errors.New("invalid seek offset")

// CompletedPart represents a completed part in multipart upload
type CompletedPart struct {
	PartNumber int32
	ETag       string
	Size       int64
}

// FsWrapper wraps the Fs struct to implement afero.Fs interface with fixed bucket and rootPrefix
type FsWrapper struct {
	Fs         *Fs
	Bucket     string
	RootPrefix string
}

// Name returns the type of FS object this is: Fs.
func (Fs) Name() string { return "s3" }

// S3API returns the underlying S3 client.
func (fs *Fs) S3API() *s3.Client {
	return fs.s3API
}

// Name returns the type of FS object this is: Fs.
func (fw *FsWrapper) Name() string { return "s3-wrapper" }

// Create creates a file.
func (fw *FsWrapper) Create(name string) (afero.File, error) {
	return fw.Fs.Create(name, fw.Bucket, fw.RootPrefix)
}

// Mkdir creates a directory.
func (fw *FsWrapper) Mkdir(name string, perm os.FileMode) error {
	return fw.Fs.Mkdir(name, perm, fw.Bucket, fw.RootPrefix)
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fw *FsWrapper) MkdirAll(path string, perm os.FileMode) error {
	return fw.Fs.MkdirAll(path, perm, fw.Bucket, fw.RootPrefix)
}

// Open opens a file for reading.
func (fw *FsWrapper) Open(name string) (afero.File, error) {
	return fw.Fs.Open(name, fw.Bucket, fw.RootPrefix)
}

// OpenFile opens a file with specific flags and permissions.
func (fw *FsWrapper) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return fw.Fs.OpenFile(name, flag, perm, fw.Bucket, fw.RootPrefix)
}

// Remove removes a file.
func (fw *FsWrapper) Remove(name string) error {
	return fw.Fs.Remove(name, fw.Bucket, fw.RootPrefix)
}

// RemoveAll removes a path and all its contents.
func (fw *FsWrapper) RemoveAll(path string) error {
	return fw.Fs.RemoveAll(path, fw.Bucket, fw.RootPrefix)
}

// Rename renames a file.
func (fw *FsWrapper) Rename(oldname, newname string) error {
	return fw.Fs.Rename(oldname, newname, fw.Bucket, fw.RootPrefix)
}

// Stat returns a FileInfo describing the named file.
func (fw *FsWrapper) Stat(name string) (os.FileInfo, error) {
	return fw.Fs.Stat(name, fw.Bucket, fw.RootPrefix)
}

// Chmod changes the permissions of a file.
func (fw *FsWrapper) Chmod(name string, mode os.FileMode) error {
	return fw.Fs.Chmod(name, mode, fw.Bucket, fw.RootPrefix)
}

// Chtimes changes the access and modification times of a file.
func (fw *FsWrapper) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fw.Fs.Chtimes(name, atime, mtime)
}

// Chown changes the owner and group of a file.
func (fw *FsWrapper) Chown(name string, uid, gid int) error {
	return fw.Fs.Chown(name, uid, gid)
}

// InitiateMultipartUpload initiates a multipart upload for the given key.
func (fw *FsWrapper) InitiateMultipartUpload(key string) (string, error) {
	return fw.Fs.InitiateMultipartUpload(key, fw.Bucket, fw.RootPrefix)
}

// UploadPart uploads a part for the multipart upload.
func (fw *FsWrapper) UploadPart(key, uploadID string, partNumber int32, data []byte) (string, error) {
	return fw.Fs.UploadPart(key, uploadID, fw.Bucket, fw.RootPrefix, partNumber, data)
}

// CompleteMultipartUpload completes the multipart upload with the given parts.
func (fw *FsWrapper) CompleteMultipartUpload(key, uploadID string, parts []CompletedPart) error {
	return fw.Fs.CompleteMultipartUpload(key, uploadID, fw.Bucket, fw.RootPrefix, parts)
}

// ListBuckets returns a list of all S3 buckets.
func (fs *Fs) ListBuckets() ([]string, error) {
	listBucketsOutput, err := fs.s3API.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	buckets := make([]string, 0, len(listBucketsOutput.Buckets))
	for _, bucket := range listBucketsOutput.Buckets {
		buckets = append(buckets, *bucket.Name)
	}

	return buckets, nil
}

// Create a file.
func (fs Fs) Create(name, bucket, rootPrefix string) (afero.File, error) {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	keyWithPrefix := prependRootPrefix(name, rootPrefix)
	{ // It's faster to trigger an explicit empty put object than opening a file for write, closing it and re-opening it
		req := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(cleanS3Key(keyWithPrefix)),
			Body:   bytes.NewReader([]byte{}),
		}

		if fs.FileProps != nil {
			applyFileProps(req, fs.FileProps)
		}

		// If no Content-Type was specified, we'll guess one
		if req.ContentType == nil {
			req.ContentType = aws.String(mime.TypeByExtension(filepath.Ext(name)))
		}

		_, errPut := fs.s3API.PutObject(context.Background(), req)
		if errPut != nil {
			return nil, errPut
		}
	}

	file, err := fs.OpenFile(name, os.O_WRONLY, 0750, bucket, rootPrefix)
	if err != nil {
		return file, err
	}

	// Create(), like all of S3, is eventually consistent.
	// To protect against unexpected behavior, have this method
	// wait until S3 reports the object exists.
	// Note: AWS SDK v2 doesn't have WaitUntilObjectExists, so we'll skip this for now
	// as it would require an additional HeadObject call that may not be necessary.
	return file, nil
}

// Mkdir makes a directory in S3.
func (fs Fs) Mkdir(name string, perm os.FileMode, bucket, rootPrefix string) error {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)

	// Root directory "/" doesn't need to be created in S3 as it's virtual
	if name == "/" || name == "." || name == "" {
		return nil
	}

	file, err := fs.OpenFile(fmt.Sprintf("%s/", name), os.O_CREATE, perm, bucket, rootPrefix)
	// file, err := fs.OpenFile(path.Clean(name), os.O_CREATE, perm, bucket, rootPrefix)
	if err == nil {
		err = file.Close()
	}
	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs Fs) MkdirAll(path string, perm os.FileMode, bucket, rootPrefix string) error {
	return fs.Mkdir(path, perm, bucket, rootPrefix)
}

// Open a file for reading.
func (fs *Fs) Open(name, bucket, rootPrefix string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0777, bucket, rootPrefix)
}

// OpenFile opens a file.
func (fs *Fs) OpenFile(name string, flag int, _ os.FileMode, bucket, rootPrefix string) (afero.File, error) {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	file := NewFileWithBucketAndPrefix(fs, name, bucket, rootPrefix)

	// Reading and writing is technically supported but can't lead to anything that makes sense
	if flag&os.O_RDWR != 0 {
		return nil, ErrNotSupported
	}

	// Appending is not supported by S3. It's do-able though by:
	// - Copying the existing file to a new place (for example $file.previous)
	// - Writing a new file, streaming the content of the previous file in it
	// - Writing the data you want to append
	// Quite network intensive, if used in abondance this would lead to terrible performances.
	if flag&os.O_APPEND != 0 {
		return nil, ErrNotSupported
	}

	// Creating is basically a write
	if flag&os.O_CREATE != 0 {
		flag |= os.O_WRONLY
	}

	// We either write
	if flag&os.O_WRONLY != 0 {
		return file, file.openWriteStream()
	}

	info, err := file.Stat()

	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return file, nil
	}

	return file, file.openReadStream(0)
}

// Remove a file
func (fs Fs) Remove(name, bucket, rootPrefix string) error {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	if _, err := fs.Stat(name, bucket, rootPrefix); err != nil {
		return err
	}

	// Invalidate cache for parent directory
	parentDir := path.Dir(name)
	if parentDir != "." && parentDir != "/" {
		parentKey := prependRootPrefix(parentDir, rootPrefix)
		fs.invalidateDirectoryCache(parentKey)
	}

	return fs.forceRemove(name, bucket, rootPrefix)
}

// forceRemove doesn't error if a file does not exist.
func (fs Fs) forceRemove(name, bucket, rootPrefix string) error {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	keyWithPrefix := prependRootPrefix(name, rootPrefix)
	_, err := fs.s3API.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(cleanS3Key(keyWithPrefix)),
	})
	return err
}

// RemoveAll removes a path by listing all objects under the prefix and deleting them in batches.
// It is much more efficient and reliable on S3 than recursive Readdir + per-file deletes.
func (fs *Fs) RemoveAll(name, bucket, rootPrefix string) error {
	ctx := context.Background()
	// normalize path first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	clean := name
	if clean == "/" || clean == "." || clean == "" {
		// skip root
		return nil
	}

	// Invalidate cache for this directory and parent
	fs.invalidateDirectoryCache(name)
	parentDir := path.Dir(name)
	if parentDir != "." && parentDir != "/" {
		parentKey := prependRootPrefix(parentDir, rootPrefix)
		fs.invalidateDirectoryCache(parentKey)
	}

	// Apply RootPrefix to the directory lookup
	prefixWithRoot := prependRootPrefix(clean, rootPrefix)
	prefix := strings.TrimPrefix(prefixWithRoot, "/")
	// if prefix != "" && !strings.HasSuffix(prefix, "/") {
	// 	prefix += "/"
	// }

	// 判断是否是“目录”删除
	if !strings.HasSuffix(prefix, "/") {
		// 先尝试删除单个对象
		_, err := fs.s3API.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(prefix),
		})
		if err == nil {
			// 文件存在，直接删
			return fs.deleteObjectsBatch(ctx, bucket, []types.ObjectIdentifier{{Key: aws.String(prefix)}})
		}
		// 如果不存在，则继续按目录逻辑处理
	}

	// paginator to list all objects with given prefix
	paginator := s3.NewListObjectsV2Paginator(fs.s3API, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	var batch []types.ObjectIdentifier
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := fs.deleteObjectsBatch(ctx, bucket, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range out.Contents {
			// collect for deletion
			batch = append(batch, types.ObjectIdentifier{Key: aws.String(*obj.Key)})
			// flush when reach 1000
			if len(batch) >= 1000 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}

	// flush remaining
	if err := flush(); err != nil {
		return err
	}

	// Also attempt to remove the "directory placeholder" object (e.g. "dir1/") if present.
	// It may already be deleted by the above loop, but ensure deletion of prefix itself.
	// We attempt one final DeleteObjects for the exact prefix key if it exists.
	// (ListObjects would have included it in Contents, so this is usually redundant but safe.)
	placeholderKey := prefix
	_, err := fs.s3API.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(placeholderKey),
	})
	if err == nil {
		// placeholder exists, delete it
		if err := fs.deleteObjectsBatch(ctx, bucket, []types.ObjectIdentifier{{Key: aws.String(placeholderKey)}}); err != nil {
			return err
		}
	} else {
		// if HeadObject returned not found, ignore; other errors should be returned
		var noSuchKey *types.NotFound
		if !errors.As(err, &noSuchKey) {
			// some S3 SDKs return different error shapes; if it's not a NotFound-equivalent, ignore only if it's recognized
			// to be safe, we just ignore NotFound and continue. For other errors, return them.
			// Many SDKs return *smithy.GenericAPIError or wrapped error; simplest conservative approach:
			// If err is of type *s3types.NoSuchKey or contains "NotFound" text, we might ignore.
			// For simplicity, we'll try to interpret common cases: if it's a 404-like error, ignore; else return.
			// Here, best-effort: return nil (tolerate missing placeholder).
		}
	}

	return nil
}

// deleteObjectsBatch deletes up to len(objs) (<=1000) objects in one DeleteObjects call.
// Returns an error if the API call fails or any returned Errors are non-empty.
func (fs *Fs) deleteObjectsBatch(ctx context.Context, bucket string, objs []types.ObjectIdentifier) error {
	if len(objs) == 0 {
		return nil
	}
	in := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objs,
			Quiet:   aws.Bool(true),
		},
	}

	out, err := fs.s3API.DeleteObjects(ctx, in)
	if err != nil {
		return err
	}
	// If S3 returns per-object errors, surface the first one (you can aggregate if desired)
	if len(out.Errors) > 0 {
		first := out.Errors[0]
		return fmt.Errorf("s3 delete error: key=%s code=%s msg=%s", aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
	}
	return nil
}

// Rename a file.
// There is no method to directly rename an S3 object, so the Rename
// will copy the file to an object with the new name and then delete
// the original.
func (fs Fs) Rename(oldname, newname, bucket, rootPrefix string) error {
	// Normalize names first to handle cases like "\\U1单词卡片2(1).pdf"
	oldname = normalizeName(oldname)
	newname = normalizeName(newname)
	if oldname == newname {
		return nil
	}

	// Invalidate cache for both old and new parent directories
	oldParent := path.Dir(oldname)
	newParent := path.Dir(newname)

	if oldParent != "." && oldParent != "/" {
		oldParentKey := prependRootPrefix(oldParent, rootPrefix)
		fs.invalidateDirectoryCache(oldParentKey)
	}

	if newParent != "." && newParent != "/" && newParent != oldParent {
		newParentKey := prependRootPrefix(newParent, rootPrefix)
		fs.invalidateDirectoryCache(newParentKey)
	}

	oldKeyWithPrefix := prependRootPrefix(oldname, rootPrefix)
	newKeyWithPrefix := prependRootPrefix(newname, rootPrefix)

	_, err := fs.s3API.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + cleanS3Key(oldKeyWithPrefix)),
		Key:        aws.String(cleanS3Key(newKeyWithPrefix)),
	})
	if err != nil {
		return err
	}
	_, err = fs.s3API.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(cleanS3Key(oldKeyWithPrefix)),
	})
	return err
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (fs Fs) Stat(name, bucket, rootPrefix string) (os.FileInfo, error) {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	if name == "/" || name == "" {
		// The root always exists
		// return NewFileInfo("/", true, 0, time.Unix(0, 0)), nil
		statDir, errStat := fs.statDirectory(name, bucket, rootPrefix)
		return statDir, errStat
	}
	keyWithPrefix := prependRootPrefix(name, rootPrefix)
	out, err := fs.s3API.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(cleanS3Key(keyWithPrefix)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if strings.Contains(apiErr.ErrorCode(), "NotFound") || apiErr.ErrorCode() == "404" {
				statDir, errStat := fs.statDirectory(name, bucket, rootPrefix)
				return statDir, errStat
			}
		}
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	} else if strings.HasSuffix(name, "/") {
		// user asked for a directory, but this is a file
		statDir, errStat := fs.statDirectory(name, bucket, rootPrefix)
		return statDir, errStat

		// return FileInfo{
		// 	name:        name,
		// 	directory:   true,
		// 	modTime:     *out.LastModified,
		// 	sizeInBytes: 0,
		// }, nil
		/*
			return FileInfo{}, &os.PathError{
				Op:   "stat",
				Path: name,
				Err:  os.ErrNotExist,
			}
		*/
	}
	return NewFileInfo(path.Base(name), false, *out.ContentLength, *out.LastModified), nil
}

func (fs Fs) statDirectory(name, bucket, rootPrefix string) (os.FileInfo, error) {
	nameClean := path.Clean(name)

	// Apply RootPrefix to the directory lookup
	prefixWithRoot := prependRootPrefix(nameClean, rootPrefix)
	prefix := strings.TrimPrefix(prefixWithRoot, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// For directory stat, we only need to check if it exists, not calculate total size
	// This is much faster for directories with many objects
	hasObjects := false

	// Use ListObjectsV2 with MaxKeys=1 to quickly check if directory exists
	out, err := fs.s3API.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	hasObjects = len(out.Contents) > 0

	// Directory doesn't exist if no objects found (except for root)
	if !hasObjects && (name != "" && name != "/") {
		return nil, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}

	// For performance, we return 0 as size instead of calculating it
	// The size is not critical for directory stat operations
	return NewFileInfo(path.Base(name), true, 0, time.Unix(0, 0)), nil
}

// Chmod doesn't exists in S3 but could be implemented by analyzing ACLs
func (fs Fs) Chmod(name string, mode os.FileMode, bucket, rootPrefix string) error {
	// Normalize the name first to handle cases like "\\U1单词卡片2(1).pdf"
	name = normalizeName(name)
	keyWithPrefix := prependRootPrefix(name, rootPrefix)
	var acl string

	otherRead := mode&(1<<2) != 0
	otherWrite := mode&(1<<1) != 0

	switch {
	case otherRead && otherWrite:
		acl = "public-read-write"
	case otherRead:
		acl = "public-read"
	default:
		acl = "private"
	}

	_, err := fs.s3API.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(cleanS3Key(keyWithPrefix)),
		ACL:    types.ObjectCannedACL(acl),
	})
	return err
}

// Chown doesn't exist in S3 should probably NOT have been added to afero as it's POSIX-only concept.
func (Fs) Chown(string, int, int) error {
	return ErrNotSupported
}

// Chtimes could be implemented if needed, but that would require to override object properties using metadata,
// which makes it a non-standard solution
func (Fs) Chtimes(string, time.Time, time.Time) error {
	return ErrNotSupported
}

// ListDirectory is an optimized method to list objects in a directory with limit support
// using the S3 paginator, which is much more efficient than Readdir(-1) for large directories.
// Parameters:
//   - name: directory path
//   - limit: maximum number of items to return (0 for default, -1 for no limit)
func (fw *FsWrapper) ListDirectory(name string, limit int) ([]os.FileInfo, error) {
	infos, _, err := fw.listDirectoryInternal(name, limit, "")
	return infos, err
}

func (fw *FsWrapper) listDirectoryInternal(name string, limit int, marker string) ([]os.FileInfo, string, error) {
	// Normalize the name first
	name = normalizeName(name)

	cleanName := strings.TrimPrefix(name, "/")
	if cleanName != "" && !strings.HasSuffix(cleanName, "/") {
		cleanName += "/"
	}

	// Apply RootPrefix to the directory lookup
	prefixWithRoot := prependRootPrefix(cleanName, fw.RootPrefix)
	// Ensure prefix doesn't start with / for S3
	prefixWithRoot = strings.TrimPrefix(prefixWithRoot, "/")

	// Create cache key
	cacheKey := fmt.Sprintf("%s:%s:%s:%d:%s", fw.Bucket, prefixWithRoot, marker, limit, fw.RootPrefix)

	// Try to get from cache first (cache for 30 seconds)
	// Cache for any limit to improve performance for repeated requests
	if cached, found := fw.Fs.getCachedDirectory(cacheKey, 30*time.Second); found {
		return cached, marker, nil
	}

	var fileInfos []os.FileInfo
	seen := make(map[string]bool)
	dirModTime := make(map[string]time.Time)

	// Set default limit if not specified
	if limit == 0 {
		limit = 1000 // Reasonable default for UI display
	}

	// Create list input with pagination support
	listInput := &s3.ListObjectsV2Input{
		Bucket:            aws.String(fw.Bucket),
		Prefix:            aws.String(prefixWithRoot),
		ContinuationToken: aws.String(marker),
	}

	// Apply limit if specified and positive
	if limit > 0 {
		listInput.MaxKeys = aws.Int32(int32(limit * 2)) // Get extra for deduplication
	}

	// Use paginator to efficiently fetch objects in batches
	paginator := s3.NewListObjectsV2Paginator(fw.Fs.s3API, listInput)

	var nextMarker string
	var itemsProcessed int

	for paginator.HasMorePages() && (limit < 0 || itemsProcessed < limit) {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, "", err
		}

		// Save continuation token for next page
		if page.NextContinuationToken != nil {
			nextMarker = *page.NextContinuationToken
		}

		for _, obj := range page.Contents {
			key := *obj.Key

			if key == prefixWithRoot || !strings.HasPrefix(key, prefixWithRoot) {
				continue
			}

			rel := strings.TrimPrefix(key, prefixWithRoot)
			if len(rel) > 0 && rel[0] == '/' {
				rel = rel[1:]
			}

			if idx := strings.Index(rel, "/"); idx != -1 {
				dirName := rel[:idx]

				if modTime, exists := dirModTime[dirName]; !exists || obj.LastModified.After(modTime) {
					dirModTime[dirName] = *obj.LastModified
				}

				if !seen[dirName] {
					seen[dirName] = true
					latestModTime, hasModTime := dirModTime[dirName]
					if !hasModTime {
						latestModTime = time.Unix(0, 0)
					}
					fileInfos = append(fileInfos, NewFileInfo(dirName, true, 0, latestModTime))
					itemsProcessed++
				}
				continue
			}

			fileInfos = append(fileInfos, NewFileInfo(path.Base(key), false, *obj.Size, *obj.LastModified))
			itemsProcessed++

			// Stop if we've reached the limit
			if limit > 0 && itemsProcessed >= limit {
				break
			}
		}
	}

	// Cache the result
	fw.Fs.setCachedDirectory(cacheKey, fileInfos)

	return fileInfos, nextMarker, nil
}

// ListDirectorySimple provides backward compatibility with the old signature
func (fw *FsWrapper) ListDirectorySimple(name string) ([]os.FileInfo, error) {
	infos, _, err := fw.listDirectoryInternal(name, -1, "")
	return infos, err
}

// prependRootPrefix adds the RootPrefix to the given name if RootPrefix is set
func prependRootPrefix(name, rootPrefix string) string {
	if rootPrefix == "" {
		return name
	}

	nameClean := cleanS3Key(name)
	if nameClean == "" {
		return rootPrefix
	}

	if len(rootPrefix) > 0 && rootPrefix[len(rootPrefix)-1] != '/' && len(nameClean) > 0 && nameClean[0] != '/' {
		return rootPrefix + "/" + nameClean
	}

	return rootPrefix + nameClean
}

// cleanS3Key removes the leading slash from the name to create a proper S3 key
func cleanS3Key(name string) string {
	name = strings.TrimLeft(name, "/")
	return name
}

// applyFileProps applies the properties from UploadedFileProperties to the PutObjectInput.
// This is used for both Create and Write operations.
func applyFileProps(req *s3.PutObjectInput, p *UploadedFileProperties) {
	if p.ACL != nil {
		req.ACL = types.ObjectCannedACL(*p.ACL)
	}

	if p.CacheControl != nil {
		req.CacheControl = p.CacheControl
	}

	if p.ContentType != nil {
		req.ContentType = p.ContentType
	}
}

// Deprecated: use applyFileProps instead.
// Kept for backward compatibility.
func applyFileCreateProps(req *s3.PutObjectInput, p *UploadedFileProperties) {
	applyFileProps(req, p)
}

// Deprecated: use applyFileProps instead.
// Kept for backward compatibility.
func applyFileWriteProps(req *s3.PutObjectInput, p *UploadedFileProperties) {
	applyFileProps(req, p)
}

// normalizeName normalizes file and directory names to handle special cases like "\\U1单词卡片2(1).pdf"
func normalizeName(name string) string {
	// Check if the original name has a trailing slash to preserve
	hasTrailingSlash := strings.HasSuffix(name, "/") || strings.HasSuffix(name, string(filepath.Separator))

	// First, clean the path to handle .. and . components
	name = path.Clean(name)
	// Convert any Windows-style backslashes to forward slashes (S3 standard)
	name = filepath.ToSlash(name)

	// Restore trailing slash if it was present in the original name
	if hasTrailingSlash && !strings.HasSuffix(name, "/") {
		name += "/"
	}

	// Additional processing for special cases can be added here as needed
	return name
}

// S3 multipart upload helper functions

// InitiateMultipartUpload initiates a multipart upload for the given key
func (fs *Fs) InitiateMultipartUpload(key, bucket, rootPrefix string) (string, error) {
	keyWithPrefix := prependRootPrefix(key, rootPrefix)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(cleanS3Key(keyWithPrefix)),
	}

	result, err := fs.s3API.CreateMultipartUpload(context.Background(), input)
	if err != nil {
		return "", err
	}

	return *result.UploadId, nil
}

// UploadPart uploads a part for the multipart upload
func (fs *Fs) UploadPart(key, uploadID, bucket, rootPrefix string, partNumber int32, data []byte) (string, error) {
	keyWithPrefix := prependRootPrefix(key, rootPrefix)
	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(cleanS3Key(keyWithPrefix)),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
		Body:       bytes.NewReader(data),
	}

	result, err := fs.s3API.UploadPart(context.Background(), input)
	if err != nil {
		return "", err
	}

	return *result.ETag, nil
}

// CompleteMultipartUpload completes the multipart upload with the given parts
func (fs *Fs) CompleteMultipartUpload(key, uploadID, bucket, rootPrefix string, parts []CompletedPart) error {
	keyWithPrefix := prependRootPrefix(key, rootPrefix)
	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		}
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(cleanS3Key(keyWithPrefix)),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := fs.s3API.CompleteMultipartUpload(context.Background(), input)
	return err
}

// getCachedDirectory retrieves directory listing from cache if available and not expired
func (fs *Fs) getCachedDirectory(cacheKey string, maxAge time.Duration) ([]os.FileInfo, bool) {
	fs.dirCacheMutex.RLock()
	defer fs.dirCacheMutex.RUnlock()

	if entry, exists := fs.dirCache[cacheKey]; exists {
		if time.Since(entry.timestamp) <= maxAge {
			// Return a copy to avoid modification
			copy := make([]os.FileInfo, len(entry.fileInfos))
			for i, info := range entry.fileInfos {
				copy[i] = info
			}
			return copy, true
		}
	}
	return nil, false
}

// setCachedDirectory stores directory listing in cache
func (fs *Fs) setCachedDirectory(cacheKey string, fileInfos []os.FileInfo) {
	fs.dirCacheMutex.Lock()
	defer fs.dirCacheMutex.Unlock()

	// Create a copy to store in cache
	copy := make([]os.FileInfo, len(fileInfos))
	for i, info := range fileInfos {
		copy[i] = info
	}

	fs.dirCache[cacheKey] = &directoryCacheEntry{
		fileInfos: copy,
		timestamp: time.Now(),
	}

	// Simple cache eviction: keep cache size manageable
	if len(fs.dirCache) > 100 {
		// Remove oldest entries
		for key, entry := range fs.dirCache {
			if time.Since(entry.timestamp) > 5*time.Minute {
				delete(fs.dirCache, key)
			}
		}
	}
}

// invalidateDirectoryCache removes directory from cache (e.g., after file operations)
func (fs *Fs) invalidateDirectoryCache(prefix string) {
	fs.dirCacheMutex.Lock()
	defer fs.dirCacheMutex.Unlock()

	// Invalidate all cache entries that start with the given prefix
	for key := range fs.dirCache {
		if strings.HasPrefix(key, prefix) {
			delete(fs.dirCache, key)
		}
	}
}
