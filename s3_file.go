// Package s3 brings S3 files handling to afero
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// File represents a file in S3.
// nolint: govet
type File struct {
	fs                       *Fs            // Parent file system
	name                     string         // Name of the file
	cachedInfo               os.FileInfo    // File info cached for later used
	streamRead               io.ReadCloser  // streamRead is the underlying stream we are reading from
	streamReadOffset         int64          // streamReadOffset is the offset of the read-only stream
	streamWrite              io.WriteCloser // streamWrite is the underlying stream we are reading to
	streamWriteErr           error          // streamWriteErr is the error that should be returned in case of a write
	streamWriteCloseErr      chan error     // streamWriteCloseErr is the channel containing the underlying write error
	readdirContinuationToken *string        // readdirContinuationToken is used to perform files listing across calls
	readdirNotTruncated      bool           // readdirNotTruncated is set when we shall continue reading
	// I think readdirNotTruncated can be dropped. The continuation token is probably enough.
}

// NewFile initializes an File object.
func NewFile(fs *Fs, name string) *File {
	return &File{
		fs:   fs,
		name: name,
	}
}

// Name returns the filename, i.e. S3 path without the bucket name.
func (f *File) Name() string { return f.name }

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned
// by ListObjects, in directory order. Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in
// a single slice. In this case, if Readdir succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdir returns the FileInfo read until that point
// and a non-nil error.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	if f.readdirNotTruncated {
		return nil, io.EOF
	}
	if n <= 0 {
		return f.ReaddirAll()
	}

	name := strings.TrimPrefix(f.Name(), "/")
	if name != "" && !strings.HasSuffix(name, "/") {
		name += "/"
	}

	process := func(output *s3.ListObjectsV2Output) []os.FileInfo {
		var outFis []os.FileInfo
		seen := make(map[string]bool)

		for _, obj := range output.Contents {
			key := *obj.Key

			// 跳过目录自身占位对象
			if key == name {
				continue
			}

			// 识别“目录”
			rel := strings.TrimPrefix(key, name)
			if idx := strings.Index(rel, "/"); idx != -1 {
				dir := rel[:idx]
				if !seen[dir] {
					seen[dir] = true
					outFis = append(outFis, NewFileInfo(dir, true, 0, time.Unix(0, 0)))
				}
				continue
			}

			// 普通文件
			outFis = append(outFis, NewFileInfo(path.Base(key), false, *obj.Size, *obj.LastModified))
		}

		return outFis
	}

	// 首次请求
	output, err := f.fs.s3API.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		ContinuationToken: f.readdirContinuationToken,
		Bucket:            aws.String(f.fs.bucket),
		Prefix:            aws.String(name),
		MaxKeys:           aws.Int32(int32(n)),
	})
	if err != nil {
		return nil, err
	}

	// 更新分页元信息（以便后续 Readdir 调用延续）
	f.readdirContinuationToken = output.NextContinuationToken
	if output.IsTruncated != nil && !*output.IsTruncated {
		f.readdirNotTruncated = true
	}

	fis := process(output)

	// 如果这一页有实际数据，直接返回
	if len(fis) > 0 {
		return fis, nil
	}

	// 如果当前页全被过滤掉并且有 NextContinuationToken -> 继续翻页（递归）
	if output.NextContinuationToken != nil {
		f.readdirContinuationToken = output.NextContinuationToken
		return f.Readdir(n)
	}

	// 关键 fallback：
	// 当输出里只有占位对象（或被过滤掉），且 S3 没有返回 NextContinuationToken（IsTruncated==false）时，
	// 仍然有可能占位对象之后存在实际对象（某些 S3/兼容实现或排序边界）。
	// 这时尝试用 StartAfter=name 再请求一次，获取 name 之后的条目。
	onlyPlaceholder := false
	if len(output.Contents) > 0 {
		onlyPlaceholder = true
		for _, obj := range output.Contents {
			if *obj.Key != name && !strings.HasSuffix(*obj.Key, "/") {
				onlyPlaceholder = false
				break
			}
		}
	}
	if onlyPlaceholder {
		out2, err2 := f.fs.s3API.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:     aws.String(f.fs.bucket),
			Prefix:     aws.String(name),
			StartAfter: aws.String(name),
			MaxKeys:    aws.Int32(int32(n)),
		})
		if err2 != nil {
			return nil, err2
		}
		// 更新分页元信息为第二次请求的结果
		f.readdirContinuationToken = out2.NextContinuationToken
		if out2.IsTruncated != nil && !*out2.IsTruncated {
			f.readdirNotTruncated = true
		}
		fis = process(out2)
		// 如果第二次请求仍为空但有 NextContinuationToken，继续翻页
		if len(fis) == 0 && out2.NextContinuationToken != nil {
			f.readdirContinuationToken = out2.NextContinuationToken
			return f.Readdir(n)
		}
		if len(fis) == 0 {
			// 真正没有更多内容
			f.readdirNotTruncated = true
			return nil, io.EOF
		}
		return fis, nil
	}

	// 否则确实没有更多可以返回的条目（符合 os.File.Readdir 行为）
	f.readdirNotTruncated = true
	return nil, io.EOF
}

func (f *File) ReaddirTMPPPPP(n int) ([]os.FileInfo, error) {
	if f.readdirNotTruncated {
		return nil, io.EOF
	}
	if n <= 0 {
		return f.ReaddirAll()
	}
	// ListObjects treats leading slashes as part of the directory name
	// It also needs a trailing slash to list contents of a directory.
	name := strings.TrimPrefix(f.Name(), "/") // + "/"

	// For the root of the bucket, we need to remove any prefix
	if name != "" && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	output, err := f.fs.s3API.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		ContinuationToken: f.readdirContinuationToken,
		Bucket:            aws.String(f.fs.bucket),
		Prefix:            aws.String(name),
		// TODO 会导致无法读取/dir下的/dir/readme
		// Delimiter:         aws.String("/"),
		MaxKeys: aws.Int32(int32(n)),
	})
	if err != nil {
		return nil, err
	}
	f.readdirContinuationToken = output.NextContinuationToken
	if output.IsTruncated != nil && !*output.IsTruncated {
		f.readdirNotTruncated = true
	}
	var fis = make([]os.FileInfo, 0, len(output.CommonPrefixes)+len(output.Contents))
	for _, subfolder := range output.CommonPrefixes {
		fis = append(fis, NewFileInfo(path.Base("/"+*subfolder.Prefix), true, 0, time.Unix(0, 0)))
	}

	for _, fileObject := range output.Contents {
		key := *fileObject.Key
		if key == name || strings.HasSuffix(key, "/") {
			continue
		}
		fis = append(fis, NewFileInfo(path.Base("/"+key), false, *fileObject.Size, *fileObject.LastModified))
	}

	// 👇 修复点
	if len(fis) == 0 {
		if output.IsTruncated != nil && *output.IsTruncated {
			// 说明还有下一页，但本页全被过滤，继续翻页
			f.readdirContinuationToken = output.NextContinuationToken
			return f.Readdir(n)
		}
		// 已经读到末尾，返回空 slice 和 EOF（符合标准库行为）
		f.readdirNotTruncated = true
		return nil, io.EOF
	}

	return fis, nil
}

// ReaddirAll provides list of file cachedInfo.
func (f *File) ReaddirAll() ([]os.FileInfo, error) {
	var fileInfos []os.FileInfo
	for {
		infos, err := f.Readdir(100)
		fileInfos = append(fileInfos, infos...)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return nil, err
			}
		}
	}
	return fileInfos, nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in
// a single slice. In this case, if Readdirnames succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdirnames returns the names read until that point and
// a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = path.Split(f.Name())
	}
	return names, nil
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	info, err := f.fs.Stat(f.Name())
	if err == nil {
		f.cachedInfo = info
	}
	return info, err
}

// Sync is a noop.
func (f *File) Sync() error {
	return nil
}

// Truncate changes the size of the file.
// It does not change the I/O offset.
// If there is an error, it will be of type *PathError.
func (f *File) Truncate(int64) error {
	return ErrNotImplemented
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s)) // nolint: gocritic
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	// Closing a reading stream
	if f.streamRead != nil {
		// We try to close the Reader
		defer func() {
			f.streamRead = nil
		}()
		return f.streamRead.Close()
	}

	// Closing a writing stream
	if f.streamWrite != nil {
		defer func() {
			f.streamWrite = nil
			f.streamWriteCloseErr = nil
		}()

		// We try to close the Writer
		if err := f.streamWrite.Close(); err != nil {
			return err
		}
		// And more importantly, we wait for the actual writing performed in go-routine to finish.
		// We might have at most 2*5=10MB of data waiting to be flushed before close returns. This
		// might be rather slow.
		err := <-f.streamWriteCloseErr
		close(f.streamWriteCloseErr)
		return err
	}

	// Or maybe we don't have anything to close
	return nil
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(p []byte) (int, error) {
	n, err := f.streamRead.Read(p)

	if err == nil {
		f.streamReadOffset += int64(n)
	}

	return n, err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = f.Read(p)
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	// Write seek is not supported
	if f.streamWrite != nil {
		return 0, ErrNotSupported
	}

	// Read seek has its own implementation
	if f.streamRead != nil {
		return f.seekRead(offset, whence)
	}

	// Not having a stream
	return 0, afero.ErrFileClosed
}

func (f *File) seekRead(offset int64, whence int) (int64, error) {
	startByte := int64(0)

	switch whence {
	case io.SeekStart:
		startByte = offset
	case io.SeekCurrent:
		startByte = f.streamReadOffset + offset
	case io.SeekEnd:
		startByte = f.cachedInfo.Size() - offset
	}

	if err := f.streamRead.Close(); err != nil {
		return 0, fmt.Errorf("couldn't close previous stream: %w", err)
	}
	f.streamRead = nil

	if startByte < 0 {
		return startByte, ErrInvalidSeek
	}

	return startByte, f.openReadStream(startByte)
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	n, err := f.streamWrite.Write(p)

	// If we have an error, it's only the "read/write on closed pipe" and we
	// should report the underlying one
	if err != nil {
		return 0, f.streamWriteErr
	}

	return n, err
}

func (f *File) openWriteStream() error {
	if f.streamWrite != nil {
		return ErrAlreadyOpened
	}

	reader, writer := io.Pipe()

	f.streamWriteCloseErr = make(chan error)
	f.streamWrite = writer

	go func() {
		input := &s3.PutObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(cleanS3Key(f.name)),
			Body:   reader,
		}

		if f.fs.FileProps != nil {
			applyFileWriteProps(input, f.fs.FileProps)
		}

		// If no Content-Type was specified, we'll guess one
		if input.ContentType == nil {
			input.ContentType = aws.String(mime.TypeByExtension(filepath.Ext(f.name)))
		}

		uploader := manager.NewUploader(f.fs.s3API)
		_, err := uploader.Upload(context.Background(), input)

		if err != nil {
			f.streamWriteErr = err
			_ = f.streamWrite.Close()
		}

		f.streamWriteCloseErr <- err
		// close(f.streamWriteCloseErr)
	}()
	return nil
}

func (f *File) openReadStream(startAt int64) error {
	if f.streamRead != nil {
		return ErrAlreadyOpened
	}

	var streamRange *string

	if startAt > 0 {
		streamRange = aws.String(fmt.Sprintf("bytes=%d-", startAt))
	}

	resp, err := f.fs.s3API.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(cleanS3Key(f.name)),
		Range:  streamRange,
	})
	if err != nil {
		return err
	}

	f.streamReadOffset = startAt
	f.streamRead = resp.Body
	return nil
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Write(p)
	return
}
