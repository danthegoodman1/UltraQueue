package utils

import (
	"fmt"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
)

func GetEnvInt(key string) (int, error) {
	s := os.Getenv(key)
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func GetEnvOrDefault(env, defaultVal string) string {
	e := os.Getenv(env)
	if e == "" {
		return defaultVal
	} else {
		return e
	}
}

func GetEnvOrFail(env string) string {
	e := os.Getenv(env)
	if e == "" {
		log.Error().Msg(fmt.Sprintf("Failed to find env var '%s'", env))
		os.Exit(1)
		return ""
	} else {
		return e
	}
}
