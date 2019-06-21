package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
)

// S3 provides a wrapper around your S3 credentials. It carries no other internal state
// and can be copied freely.
type S3 struct {
	bucket   string
	accessId string
	secret   string
	endpoint string
}

// NewS3 allocates a new S3 with the provided credentials.
func NewS3(bucket, accessId, secret string) *S3 {
	return &S3{
		bucket:   bucket,
		accessId: accessId,
		secret:   secret,
		endpoint: fmt.Sprintf("%s.s3.amazonaws.com", bucket),
	}
}

func (s4 *S3) signRequest(req *http.Request) (er error) {
	signer := v4.Signer{
		Credentials:            credentials.NewStaticCredentials(s4.accessId, s4.secret, ""),
		DisableURIPathEscaping: true,
	}

	t := time.Now()
	if req.Header.Get("Date") == "" {
		req.Header.Set("Date", t.Format(time.RFC1123))
	}

	var seeker io.ReadSeeker
	if req.Body != nil {
		seeker = bytes.NewReader(streamToByte(req.Body))
	}
	_, err := signer.Sign(req, seeker, "s3", endpoints.ApNortheast1RegionID, t)
	if err != nil {
		return er
	}
	return nil
}

func streamToByte(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}

func (s3 *S3) resource(path string, values url.Values) string {
	tmp := fmt.Sprintf("https://%s/%s", s3.endpoint, path)

	if values != nil {
		tmp += "?" + values.Encode()
	}

	return tmp
}

func (s3 *S3) putMultipart(r io.Reader, size int64, path string, contentType string) (er error) {
	mp, er := s3.StartMultipart(path)
	if er != nil {
		return er
	}
	defer func() {
		if er != nil {
			mp.Abort()
		}
	}()

	var chunkSize int64 = 7 * 1024 * 1024
	chunk := bytes.NewBuffer(make([]byte, chunkSize))
	md5hash := md5.New()
	remaining := size

	for ; remaining > 0; remaining -= chunkSize {
		chunk.Reset()
		md5hash.Reset()

		if remaining < chunkSize {
			chunkSize = remaining
		}

		wr := io.MultiWriter(chunk, md5hash)

		if _, er := io.CopyN(wr, r, chunkSize); er != nil {
			return er
		}

		if er := mp.AddPart(chunk, chunkSize, md5hash.Sum(nil)); er != nil {
			return er
		}
	}

	return mp.Complete(contentType)
}

// Put uploads content to S3. The length of r must be passed as size. md5sum optionally contains
// the MD5 hash of the content for end-to-end integrity checking; if omitted no checking is done.
// contentType optionally contains the MIME type to send to S3 as the Content-Type header; when
// files are later accessed, S3 will echo back this in their response headers.
//
// If the passed size exceeds 3GB, the multipart API is used, otherwise the single-request API is used.
// It should be noted that the multipart API uploads in 7MB segments and computes checksums of each
// one -- it does NOT use the passed md5sum, so don't bother with it if you're uploading huge files.
func (s3 *S3) Put(r io.Reader, size int64, path string, md5sum []byte, contentType string) error {
	if size > 3*1024*1024*1024 {
		return s3.putMultipart(r, size, path, contentType)
	}

	req, er := http.NewRequest("PUT", s3.resource(path, nil), r)
	if er != nil {
		return er
	}

	if md5sum != nil {
		md5value := base64.StdEncoding.EncodeToString(md5sum)
		req.Header.Set("Content-MD5", md5value)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))
	req.ContentLength = size

	er = s3.signRequest(req)
	if er != nil {
		return er
	}

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return er
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		er := wrapError(resp)

		if newEndpoint := er.newEndpoint(); newEndpoint != "" {
			s3.endpoint = newEndpoint
			er.ShouldRetry = true
		}

		return er
	}

	return nil
}

// Get fetches content from S3, returning both a ReadCloser for the data and the HTTP headers
// returned by S3. You can use the headers to extract the Content-Type that the data was sent
// with.
func (s3 *S3) Get(path string) (io.ReadCloser, http.Header, error) {
	req, er := http.NewRequest("GET", s3.resource(path, nil), nil)
	if er != nil {
		return nil, http.Header{}, er
	}

	er = s3.signRequest(req)
	if er != nil {
		return nil, http.Header{}, er
	}

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return nil, http.Header{}, er
	}

	if resp.StatusCode != 200 {
		er := wrapError(resp)

		if newEndpoint := er.newEndpoint(); newEndpoint != "" {
			s3.endpoint = newEndpoint
			er.ShouldRetry = true
		}

		return nil, http.Header{}, er
	}

	return resp.Body, resp.Header, nil
}

// Head is similar to Get, but returns only the response headers. The response body is not
// transferred across the network. This is useful for checking if a file exists remotely,
// and what headers it was configured with.
func (s3 *S3) Head(path string) (http.Header, error) {
	req, er := http.NewRequest("HEAD", s3.resource(path, nil), nil)
	if er != nil {
		return http.Header{}, er
	}

	er = s3.signRequest(req)
	if er != nil {
		return http.Header{}, er
	}

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return http.Header{}, er
	}

	if resp.StatusCode != 200 {
		er := wrapError(resp)

		if newEndpoint := er.newEndpoint(); newEndpoint != "" {
			s3.endpoint = newEndpoint
			er.ShouldRetry = true
		}

		return http.Header{}, er
	}

	return resp.Header, nil
}

// Test attempts to write and read back a single, short file from S3. It is intended to be
// used to test runtime configuration to fail quickly when credentials are invalid.
func (s3 *S3) Test() error {
	testString := fmt.Sprintf("roundtrip-test-%d", rand.Int())
	testReader := strings.NewReader(testString)

	if er := s3.Put(testReader, int64(testReader.Len()), "writetest", nil, "text/x-empty"); er != nil {
		if s3er, ok := er.(*S3Error); ok {
			if s3er.ShouldRetry {
				return s3.Test()
			}
		}

		return er
	}

	actualReader, header, er := s3.Get("writetest")
	if er != nil {
		if s3er, ok := er.(*S3Error); ok {
			if s3er.ShouldRetry {
				return s3.Test()
			}
		}

		return er
	}
	defer actualReader.Close()

	actualBytes, er := ioutil.ReadAll(actualReader)
	if er != nil {
		return er
	}

	if string(actualBytes) != testString {
		return fmt.Errorf("String read back from S3 was different than what we put there.")
	}

	if header.Get("Content-Type") != "text/x-empty" {
		return fmt.Errorf("Content served back from S3 had a different Content-Type than what we put there")
	}

	return nil
}

// StartMultipart initiates a multipart upload.
func (s3 *S3) StartMultipart(path string) (*S3Multipart, error) {
	req, er := http.NewRequest("POST", s3.resource(path, nil)+"?uploads", nil)
	if er != nil {
		return nil, er
	}

	er = s3.signRequest(req)
	if er != nil {
		return nil, er
	}

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return nil, er
	}
	defer resp.Body.Close()

	xmlBytes, er := ioutil.ReadAll(resp.Body)
	if er != nil {
		return nil, er
	}

	if resp.StatusCode != 200 {
		er := wrapError(resp)

		if newEndpoint := er.newEndpoint(); newEndpoint != "" {
			s3.endpoint = newEndpoint
			er.ShouldRetry = true
		}

		return nil, er
	}

	var xmlResp s3multipartResp
	if er := xml.Unmarshal(xmlBytes, &xmlResp); er != nil {
		return nil, er
	}

	mp := &S3Multipart{
		uploadId: xmlResp.UploadId,
		key:      xmlResp.Key,
		s3:       s3,
	}

	runtime.SetFinalizer(mp, func(mp *S3Multipart) {
		mp.Abort()
	})

	return mp, nil
}
