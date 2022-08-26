package filemanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func supressMinorErrors(err error) error {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				pkgLogger.Debug("Received 409. Container already exists")
				return nil
			}
		}
	}
	return err
}

func (manager *AzureBlobStorageManager) getBaseURL() *url.URL {
	protocol := "https"
	if manager.Config.DisableSSL != nil && *manager.Config.DisableSSL {
		protocol = "http"
	}

	endpoint := "blob.core.windows.net"
	if manager.Config.EndPoint != nil && *manager.Config.EndPoint != "" {
		endpoint = *manager.Config.EndPoint
	}

	baseURL := url.URL{
		Scheme: protocol,
		Host:   fmt.Sprintf("%s.%s", manager.Config.AccountName, endpoint),
	}

	if manager.useSASTokens() {
		baseURL.RawQuery = manager.Config.SASToken
	}

	if manager.Config.ForcePathStyle != nil && *manager.Config.ForcePathStyle {
		baseURL.Host = endpoint
		baseURL.Path = fmt.Sprintf("/%s/", manager.Config.AccountName)
	}

	return &baseURL
}

func (manager *AzureBlobStorageManager) getCredentials() (azblob.Credential, error) {
	if manager.useSASTokens() {
		return azblob.NewAnonymousCredential(), nil
	}

	accountName, accountKey := manager.Config.AccountName, manager.Config.AccountKey
	if len(accountName) == 0 || len(accountKey) == 0 {
		return nil, errors.New("either the AccountName or AccountKey is not correct")
	}

	// Create a default request pipeline using your storage account name and account key.
	return azblob.NewSharedKeyCredential(accountName, accountKey)
}

func (manager *AzureBlobStorageManager) getContainerURL() (azblob.ContainerURL, error) {
	if manager.Config.Container == "" {
		return azblob.ContainerURL{}, errors.New("no container configured")
	}

	credential, err := manager.getCredentials()
	if err != nil {
		return azblob.ContainerURL{}, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	baseURL := manager.getBaseURL()
	serviceURL := azblob.NewServiceURL(*baseURL, p)
	containerURL := serviceURL.NewContainerURL(manager.Config.Container)

	return containerURL, nil
}

func (manager *AzureBlobStorageManager) useSASTokens() bool {
	return manager.Config.UseSASTokens != nil && *manager.Config.UseSASTokens
}

func (manager *AzureBlobStorageManager) skipCreationOfContainers() bool {
	if manager.useSASTokens() {
		return true
	}
	return false
}

// Upload passed in file to Azure Blob Storage
func (manager *AzureBlobStorageManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return UploadOutput{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	if !manager.skipCreationOfContainers() {
		_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		err = supressMinorErrors(err)
		if err != nil {
			return UploadOutput{}, err
		}
	}

	fileName := path.Join(manager.Config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16,
	})
	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: manager.blobLocation(&blobURL), ObjectName: fileName}, nil
}

func (manager *AzureBlobStorageManager) blobLocation(blobURL *azblob.BlockBlobURL) string {
	if manager.useSASTokens() {
		blobURLParts := azblob.NewBlobURLParts(blobURL.URL())
		blobURLParts.SAS = azblob.SASQueryParameters{}
		newBlobURL := blobURLParts.URL()
		return newBlobURL.String()
	}
	return blobURL.String()
}

func (manager *AzureBlobStorageManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return []*FileObject{}, err
	}

	blobListingDetails := azblob.BlobListingDetails{
		Metadata: true,
	}
	segmentOptions := azblob.ListBlobsSegmentOptions{
		Details:    blobListingDetails,
		Prefix:     prefix,
		MaxResults: int32(maxItems),
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	// List the blobs in the container
	var marker string
	response, err := containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{Val: &marker}, segmentOptions)
	if err != nil {
		return
	}

	fileObjects = make([]*FileObject, len(response.Segment.BlobItems))
	for idx := range response.Segment.BlobItems {
		fileObjects[idx] = &FileObject{response.Segment.BlobItems[idx].Name, response.Segment.BlobItems[idx].Properties.LastModified}
	}
	return
}

func (manager *AzureBlobStorageManager) Download(ctx context.Context, output *os.File, key string) error {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

	blobURL := containerURL.NewBlockBlobURL(key)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	// Here's how to download the blob
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return err
	}

	// NOTE: automatically retries are performed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// read the body into a buffer
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)
	if err != nil {
		return err
	}

	_, err = output.Write(downloadedData.Bytes())
	return err
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url

	https://account-name.blob.core.windows.net/container-name/key - >> key
*/
func (manager *AzureBlobStorageManager) GetObjectNameFromLocation(location string) (string, error) {
	strToken := strings.Split(location, fmt.Sprintf("%s/", manager.Config.Container))
	return strToken[len(strToken)-1], nil
}

func (manager *AzureBlobStorageManager) GetDownloadKeyFromFileLocation(location string) string {
	str := strings.Split(location, fmt.Sprintf("%s/", manager.Config.Container))
	return str[len(str)-1]
}

type AzureBlobStorageManager struct {
	Config  *AzureBlobStorageConfig
	timeout time.Duration
}

func (manager *AzureBlobStorageManager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *AzureBlobStorageManager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}

	return getBatchRouterDurationConfig("timeout", "AZURE_BLOB", 120, time.Second)
}

func GetAzureBlogStorageConfig(config map[string]interface{}) *AzureBlobStorageConfig {
	var containerName, accountName, accountKey, sasToken, prefix string
	var endPoint *string
	var forcePathStyle, disableSSL, useSASTokens *bool
	if config["containerName"] != nil {
		tmp, ok := config["containerName"].(string)
		if ok {
			containerName = tmp
		}
	}
	if config["prefix"] != nil {
		tmp, ok := config["prefix"].(string)
		if ok {
			prefix = tmp
		}
	}
	if config["accountName"] != nil {
		tmp, ok := config["accountName"].(string)
		if ok {
			accountName = tmp
		}
	}
	if config["useSASTokens"] != nil {
		tmp, ok := config["useSASTokens"].(bool)
		if ok {
			useSASTokens = &tmp
		}
	}
	if config["sasToken"] != nil {
		tmp, ok := config["sasToken"].(string)
		if ok {
			sasToken = tmp
		}
	}
	if config["accountKey"] != nil {
		tmp, ok := config["accountKey"].(string)
		if ok {
			accountKey = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = &tmp
		}
	}
	if config["forcePathStyle"] != nil {
		tmp, ok := config["forcePathStyle"].(bool)
		if ok {
			forcePathStyle = &tmp
		}
	}
	if config["disableSSL"] != nil {
		tmp, ok := config["disableSSL"].(bool)
		if ok {
			disableSSL = &tmp
		}
	}
	return &AzureBlobStorageConfig{
		Container:      containerName,
		Prefix:         prefix,
		AccountName:    accountName,
		AccountKey:     accountKey,
		UseSASTokens:   useSASTokens,
		SASToken:       sasToken,
		EndPoint:       endPoint,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
	}
}

type AzureBlobStorageConfig struct {
	Container      string
	Prefix         string
	AccountName    string
	AccountKey     string
	SASToken       string
	EndPoint       *string
	ForcePathStyle *bool
	DisableSSL     *bool
	UseSASTokens   *bool
}

func (manager *AzureBlobStorageManager) DeleteObjects(ctx context.Context, keys []string) (err error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

	for _, key := range keys {
		blobURL := containerURL.NewBlockBlobURL(key)

		_ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
		_, err := blobURL.Delete(_ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			cancel()
			return err
		}
		cancel()
	}
	return
}

func (manager *AzureBlobStorageManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
