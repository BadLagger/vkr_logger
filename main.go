package main

import "logger/utils"

func main() {
	log := utils.GlobalLogger()
	defer log.Info("Logger Done!")

	log.Info("Logger starts!")
}