package server

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"zotregistry.dev/zot/v2/pkg/api/config"
)

func TestIsSharedStorageUnclustered(t *testing.T) {
	remoteStorage := map[string]any{"name": "s3"}
	remoteCache := map[string]any{"name": "redis"}

	Convey("isSharedStorageUnclustered", t, func() {
		Convey("remote storage and remote cache without a cluster section", func() {
			cfg := config.New()
			cfg.Storage.StorageDriver = remoteStorage
			cfg.Storage.RemoteCache = true
			cfg.Storage.CacheDriver = remoteCache

			So(isSharedStorageUnclustered(cfg), ShouldBeTrue)
		})

		Convey("the same with cluster membership", func() {
			cfg := config.New()
			cfg.Storage.StorageDriver = remoteStorage
			cfg.Storage.RemoteCache = true
			cfg.Storage.CacheDriver = remoteCache
			cfg.Cluster = &config.ClusterConfig{Members: []string{"127.0.0.1:9000", "127.0.0.1:9001"}}

			So(isSharedStorageUnclustered(cfg), ShouldBeFalse)
		})

		Convey("local storage with a remote cache", func() {
			cfg := config.New()
			cfg.Storage.RemoteCache = true
			cfg.Storage.CacheDriver = remoteCache

			So(isSharedStorageUnclustered(cfg), ShouldBeFalse)
		})

		Convey("remote storage with a local cache", func() {
			cfg := config.New()
			cfg.Storage.StorageDriver = remoteStorage

			So(isSharedStorageUnclustered(cfg), ShouldBeFalse)
		})

		Convey("remote storage with remoteCache off", func() {
			cfg := config.New()
			cfg.Storage.StorageDriver = remoteStorage
			cfg.Storage.CacheDriver = remoteCache

			So(isSharedStorageUnclustered(cfg), ShouldBeFalse)
		})

		Convey("remote storage only on a subpath", func() {
			cfg := config.New()
			cfg.Storage.RemoteCache = true
			cfg.Storage.CacheDriver = remoteCache
			cfg.Storage.SubPaths = map[string]config.StorageConfig{
				"/a": {RootDirectory: "/a", StorageDriver: remoteStorage},
			}

			So(isSharedStorageUnclustered(cfg), ShouldBeTrue)
		})
	})
}
