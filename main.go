package main

import (
	"logger/models"
	"logger/utils"
	"os"
)

func main() {
	log := utils.GlobalLogger().SetLevel(utils.Debug)
	defer log.Info("Logger Done!")

	log.Info("Logger starts!")

	if len(os.Args) < 2 {
		log.Error("Usage: logger <config_file>")
		os.Exit(1)
	}

	cfg, err := models.NewConfig(os.Args[1])
	if err != nil {
		log.Error("get config error: +%v", err)
		os.Exit(1)
	}

	csvLogger, err := models.NewCSVLogger(cfg)
	if err != nil {
		log.Error("create logger error: +%v", err)
		os.Exit(1)
	}

	if err := csvLogger.Start(); err != nil {
		log.Error("Failed to start logger: %v\n", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	<-sigChan

	csvLogger.Stop()
}
