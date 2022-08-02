package backendconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// new goroutine here to cache the config
func Cache() {
	ch := backendConfig.Subscribe(context.TODO(), TopicProcessConfig)
	for config := range ch {
		cachedConfig := ConfigT{}
		if config.Data != nil {
			sources := config.Data.(ConfigT)
			for _, source := range sources.Sources {
				cachedConfig.Sources = append(cachedConfig.Sources, SourceT{
					ID:               source.ID,
					Name:             source.Name,
					WorkspaceID:      source.WorkspaceID,
					WriteKey:         source.WriteKey,
					Enabled:          source.Enabled,
					SourceDefinition: source.SourceDefinition,
				})
			}
			// persist to database
			configBytes, err := json.Marshal(cachedConfig)
			if err != nil {
				panic(err)
			}
			writeToFile(configBytes)
		}
	}
}

// write the config to a file
func writeToFile(configBytes []byte) {
	// write to file
	localTmpDirName := "/rudder-config-cache/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, `config`)

	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()
	_, err = jsonFile.Write(configBytes)
	if err != nil {
		panic(err)
	}
}

// another method to fetch the cached config when needed
func getCachedConfig() (ConfigT, error) {
	// read from file
	localTmpDirName := "/rudder-config-cache/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, `config`)
	jsonFile, err := os.Open(jsonPath)
	if err != nil {
		return ConfigT{}, err
	}
	defer jsonFile.Close()
	var cachedConfig ConfigT
	err = json.NewDecoder(jsonFile).Decode(&cachedConfig)
	if err != nil {
		return ConfigT{}, err
	}
	return cachedConfig, nil
}
