package ks3

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	argoerrs "github.com/argoproj/argo-workflows/v3/errors"
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	waitutil "github.com/argoproj/argo-workflows/v3/util/wait"
	"github.com/argoproj/argo-workflows/v3/workflow/artifacts/common"
	pkgerr "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/ks3sdklib/aws-sdk-go/aws"
	"github.com/ks3sdklib/aws-sdk-go/aws/credentials"
	"github.com/ks3sdklib/aws-sdk-go/service/s3"
)

// ArtifactDriver is a driver for ks3
type ArtifactDriver struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

var (
	_            common.ArtifactDriver = &ArtifactDriver{}
	defaultRetry                       = wait.Backoff{Duration: time.Second * 2, Factor: 2.0, Steps: 5, Jitter: 0.1}
)

func (ks3Driver *ArtifactDriver) newKs3Client() (*s3.S3, error) {
	credentials := credentials.NewStaticCredentials(ks3Driver.AccessKey, ks3Driver.SecretKey, "")
	client := s3.New(&aws.Config{
		Credentials:      credentials,
		Endpoint:         ks3Driver.Endpoint,
		Region:           ks3Driver.Region,
		DisableSSL:       true,
		LogHTTPBody:      false,
		LogLevel:         1,
		S3ForcePathStyle: false,
		DomainMode:       false,
		SignerVersion:    "V2",
		MaxRetries:       1,
	})

	return client, nil
}

func (ks3Driver *ArtifactDriver) Load(inputArtifact *wfv1.Artifact, path string) error {
	err := waitutil.Backoff(defaultRetry, func() (bool, error) {
		log.Infof("Ks3 Load path: %s, key: %s", path, inputArtifact.Ks3.Key)
		ks3cli, err := ks3Driver.newKs3Client()
		if err != nil {
			return false, err
		}
		return loadS3Artifact(ks3cli, inputArtifact, path)
	})

	return err
}

func (ks3Driver *ArtifactDriver) OpenStream(artifact *wfv1.Artifact) (io.ReadCloser, error) {
	ks3cli, err := ks3Driver.newKs3Client()
	if err != nil {
		return nil, err
	}
	bucketName := artifact.Ks3.Bucket
	objectName := artifact.Ks3.Key
	resp, err := ks3cli.GetObject(&s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &objectName,
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (ks3Driver *ArtifactDriver) Save(path string, outputArtifact *wfv1.Artifact) error {
	err := waitutil.Backoff(defaultRetry, func() (bool, error) {
		ks3cli, err := ks3Driver.newKs3Client()
		if err != nil {
			return false, err
		}
		bucketName := outputArtifact.Ks3.Bucket
		objectName := outputArtifact.Ks3.Key
		file, err := os.Open(path)
		if err != nil {
			return false, err
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			return false, err
		}
		if stat.Size() <= 1024*1024*5 {
			_, err = ks3cli.PutObject(&s3.PutObjectInput{
				Bucket:      aws.String(bucketName),
				Key:         aws.String(objectName),
				ContentType: aws.String("application/octet-stream"),
				Body:        file,
			})
			if err != nil {
				return false, err
			}
			return true, nil
		}
		initRet, err := ks3cli.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket:      &bucketName,
			Key:         &objectName,
			ContentType: aws.String("application/octet-stream"),
		})
		if err != nil {
			return false, err
		}
		uploadId := *initRet.UploadID
		log.Infof("uploadId: %s", uploadId)

		defer func() {
			if err != nil {
				log.Infof("abort upload: %s", uploadId)
				_, abortErr := ks3cli.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
					Bucket:      &bucketName,
					Key:         &objectName,
					ContentType: aws.String("application/octet-stream"),
					UploadID:    &uploadId,
				})
				if abortErr != nil {
					log.Errorf("abort upload failed: %v", abortErr)
				}
			}
		}()

		var i int64 = 1
		compParts := []*s3.CompletedPart{}
		partsNum := []int64{0}
		buffer := make([]byte, 500*1024*1024)
		for {
			n, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				return false, err
			} else if n == 0 {
				break
			} else {
				resp, err := ks3cli.UploadPart(&s3.UploadPartInput{
					Bucket:        &bucketName,
					Key:           &objectName,
					PartNumber:    aws.Long(i),
					UploadID:      aws.String(uploadId),
					Body:          bytes.NewReader(buffer[:n]),
					ContentLength: aws.Long(int64(len(buffer[:n]))),
				})
				if err != nil {
					return false, err
				}
				partsNum = append(partsNum, i)
				compParts = append(compParts, &s3.CompletedPart{PartNumber: &partsNum[i], ETag: resp.ETag})
				log.Infof("upload part %d, etag: %s", i, *resp.ETag)
				i++
			}
		}

		compRet, err := ks3cli.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectName),
			UploadID: aws.String(uploadId),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: compParts,
			},
		})
		if err != nil {
			return false, err
		}
		log.Infof("upload complete: %v", *compRet)
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("upload to ks3 failed: %w", err)
	}
	return nil
}

func (ks3Driver *ArtifactDriver) Delete(artifact *wfv1.Artifact) error {
	return common.ErrDeleteNotSupported
}

func (ks3Driver *ArtifactDriver) ListObjects(artifact *wfv1.Artifact) ([]string, error) {
	var files []string
	err := waitutil.Backoff(defaultRetry, func() (bool, error) {
		ks3cli, err := ks3Driver.newKs3Client()
		if err != nil {
			return false, err
		}
		bucketName := artifact.Ks3.Bucket
		resp, err := ks3cli.ListObjects(&s3.ListObjectsInput{
			Bucket: &bucketName,
			Prefix: &artifact.Ks3.Key,
		})
		if err != nil {
			return false, err
		}
		for _, object := range resp.Contents {
			files = append(files, *object.Key)
		}
		return true, nil
	})
	return files, err
}

// loadS3Artifact downloads artifacts from an S3 compliant storage
// returns true if the download is completed or can't be retried (non-transient error)
// returns false if it can be retried (transient error)
func loadS3Artifact(ks3cli *s3.S3, inputArtifact *wfv1.Artifact, path string) (bool, error) {
	bucketName := inputArtifact.Ks3.Bucket
	objectName := inputArtifact.Ks3.Key
	originErr := ks3cli.GetObjectToFile(bucketName, objectName, path, "")
	if originErr == nil {
		return true, nil
	}
	if !isErr(originErr, s3.ErrCodeNoSuchKey) {
		return false, fmt.Errorf("failed to get file: %v", originErr)
	}
	isDir, err := isDirectory(ks3cli, bucketName, objectName)
	if err != nil {
		return false, fmt.Errorf("failed to check if key is a directory: %v", err)
	}
	if !isDir {
		return false, argoerrs.New(argoerrs.CodeNotFound, originErr.Error())
	}
	if err = getDirectory(ks3cli, bucketName, objectName, path); err != nil {
		return false, fmt.Errorf("failed to download directory: %v", err)
	}
	return true, nil
}

func (ks3Driver *ArtifactDriver) IsDirectory(artifact *wfv1.Artifact) (bool, error) {
	ks3cli, err := ks3Driver.newKs3Client()
	if err != nil {
		return false, err
	}
	bucketName := artifact.Ks3.Bucket
	objectName := artifact.Ks3.Key
	return isDirectory(ks3cli, bucketName, objectName)
}

func isDirectory(ks3cli *s3.S3, bucket, keyPrefix string) (bool, error) {
	if keyPrefix != "" {
		keyPrefix = filepath.Clean(keyPrefix) + "/"
	}
	resp, err := ks3cli.ListObjects(&s3.ListObjectsInput{
		Bucket:  &bucket,
		MaxKeys: &[]int64{2}[0],
		Prefix:  &keyPrefix,
	})
	if err != nil {
		return false, err
	}
	return len(resp.Contents) > 1, nil
}

func getDirectory(ks3cli *s3.S3, bucketName, keyPrefix, path string) error {
	keys, err := listDirectory(ks3cli, bucketName, keyPrefix)
	if err != nil {
		return err
	}
	for _, objKey := range keys {
		relKeyPath := strings.TrimPrefix(objKey, keyPrefix)
		localPath := filepath.Join(path, relKeyPath)

		// Create the directory if it doesn't exist
		st, err := os.Stat(localPath)
		if err == nil {
			if st.IsDir() {
				return fmt.Errorf("fileName is a directory: %s", localPath)
			}
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err != nil {
			err = os.MkdirAll(filepath.Dir(localPath), os.ModePerm)
			if err != nil {
				return err
			}
		}
		err = ks3cli.GetObjectToFile(bucketName, objKey, localPath, "")
		if err != nil {
			return pkgerr.WithStack(err)
		}
	}
	return nil
}

func listDirectory(ks3cli *s3.S3, bucketName, keyPrefix string) ([]string, error) {
	var out []string

	if keyPrefix != "" {
		keyPrefix = filepath.Clean(keyPrefix) + "/"
	}

	nextMarker := ""
	for {
		listObjectInput := &s3.ListObjectsInput{
			Bucket:  &bucketName,
			MaxKeys: &[]int64{1000}[0],
			Prefix:  &keyPrefix,
			Marker:  &nextMarker,
		}
		resp, err := ks3cli.ListObjects(listObjectInput)
		if err != nil {
			return nil, err
		}
		for _, object := range resp.Contents {
			if !strings.HasSuffix(*object.Key, "/") {
				out = append(out, *object.Key)
			}
			nextMarker = *object.Key
		}
		if !*resp.IsTruncated {
			break
		}
	}

	return out, nil
}

func isErr(err error, code string) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), code)
}
