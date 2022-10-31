package badgerdb_test

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

// BenchmarkBackupRestore benchmarks the backup and restore time of the badger repository
// after seeding it with a very large number of suppressions.
func BenchmarkBackupRestore(b *testing.B) {
	b.StopTimer()
	totalSuppressions := 40_000_000
	batchSize := 10000
	backupFilename := path.Join(b.TempDir(), "backup.badger")

	repo1Path := path.Join(b.TempDir(), "repo-1")
	repo1, err := badgerdb.NewRepository(repo1Path, logger.NOP)
	require.NoError(b, err)

	for i := 0; i < totalSuppressions/batchSize; i++ {
		suppressions := generateSuppressions(i*batchSize, batchSize)
		token := []byte(fmt.Sprintf("token%d", i))
		require.NoError(b, repo1.Add(suppressions, token))
	}

	b.Run("backup", func(b *testing.B) {
		b.StopTimer()
		start := time.Now()
		f, err := os.Create(backupFilename)
		w, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
		defer func() { _ = f.Close() }()
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, repo1.Backup(w))
		w.Close()
		b.StopTimer()
		dur := time.Since(start)
		fileInfo, err := f.Stat()
		require.NoError(b, err)
		b.ReportMetric(float64(fileInfo.Size()/1024/1024), "filesize(MB)")
		b.ReportMetric(dur.Seconds(), "duration(sec)")
		dSize, err := totalSize(repo1Path)
		require.NoError(b, err)
		b.ReportMetric(float64(dSize/1024/1024), "dirsize(MB)")

		require.NoError(b, repo1.Stop())

		repo1Gz := path.Join(b.TempDir(), "repo-1.tar.gz")
		gzipStart := time.Now()
		require.NoError(b, compress(repo1Path, repo1Gz))
		b.ReportMetric(time.Since(gzipStart).Seconds(), "gzip(sec)")

		gzipSize, err := totalSize(repo1Gz)
		require.NoError(b, err)
		b.ReportMetric(float64(gzipSize/1024/1024), "gzipsize(MB)")
	})

	b.Run("restore", func(b *testing.B) {
		b.StopTimer()
		repo2Path := path.Join(b.TempDir(), "repo-2")
		repo2, err := badgerdb.NewRepository(repo2Path, logger.NOP)
		require.NoError(b, err)

		f, err := os.Open(backupFilename)
		require.NoError(b, err)
		defer func() { _ = f.Close() }()
		r, err := gzip.NewReader(f)
		require.NoError(b, err)
		fileInfo, err := f.Stat()
		require.NoError(b, err)
		b.ReportMetric(float64(fileInfo.Size()/1024/1024), "filesize(MB)")

		start := time.Now()
		b.StartTimer()
		require.NoError(b, repo2.Restore(r))
		b.StopTimer()
		r.Close()
		b.ReportMetric(time.Since(start).Seconds(), "duration(sec)")

		require.NoError(b, repo2.Stop())

		dSize, err := totalSize(repo2Path)
		require.NoError(b, err)
		b.ReportMetric(float64(dSize/1024/1024), "dirsize(MB)")

		repo2Gz := path.Join(b.TempDir(), "repo-2.tar.gz")
		gzipStart := time.Now()
		require.NoError(b, compress(repo2Path, repo2Gz))
		b.ReportMetric(time.Since(gzipStart).Seconds(), "gzip(sec)")

		gzipSize, err := totalSize(repo2Gz)
		require.NoError(b, err)
		b.ReportMetric(float64(gzipSize/1024/1024), "gzipsize(MB)")
	})
}

func generateSuppressions(startFrom, batchSize int) []model.Suppression {
	var res []model.Suppression

	for i := startFrom; i < startFrom+batchSize; i++ {
		var sourceIDs []string
		wildcard := randomInt(2) == 0
		if wildcard {
			sourceIDs = []string{}
		} else {
			sourceIDs = []string{fmt.Sprintf("source%d", i), "otherSource", "anotherSource"}
		}
		res = append(res, model.Suppression{
			Canceled:    randomInt(2) == 0,
			WorkspaceID: fmt.Sprintf("workspace%d", i),
			UserID:      fmt.Sprintf("user%d", i),
			SourceIDs:   sourceIDs,
		})
	}
	return res
}

func randomInt(lt int) int {
	return rand.Int() % lt // skipcq: GSC-G404
}

func totalSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func compress(source, target string) error {
	writer, err := os.Create(target)
	if err != nil {
		return err
	}
	zr, err := gzip.NewWriterLevel(writer, gzip.BestSpeed)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zr)
	if err != nil {
		return err
	}

	err = filepath.Walk(source, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(file)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := zr.Close(); err != nil {
		return err
	}
	return nil
}
