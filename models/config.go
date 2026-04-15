package models

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
    CollectorSocket string `json:"collector_socket"`
    LogDir          string `json:"log_dir"`
    CSVDelimiter    string `json:"csv_delimiter"`
    Rotation        struct {
        MaxRecordsPerFile int  `json:"max_records_per_file"`
        MaxFilesCount     int  `json:"max_files_count"`
        Enabled           bool `json:"enabled"`
    } `json:"rotation"`
    ControlSocket string `json:"control_socket"`
}

func NewConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %v", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	return &config, nil
}
