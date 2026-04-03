package newkafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

type Config struct {
	Brokers         []string
	ClientID        string
	MetadataTimeout time.Duration
}

// ConfigFromViper
//
//	@Description: 从 viper 中读取 Kafka 基础配置
//	@return Config
//	@return error
func ConfigFromViper() (Config, error) {
	cfg := Config{
		Brokers:         viper.GetStringSlice("kafka.addr"),
		MetadataTimeout: 5 * time.Second,
	}

	if len(cfg.Brokers) == 0 {
		return Config{}, fmt.Errorf("kafka.addr is empty")
	}

	return cfg, nil
}

// Validate
//
//	@Description: 校验 Kafka 基础配置是否合法
//	@receiver c
//	@return error
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka.addr is empty")
	}
	return nil
}

// baseConfigMap
//
//	@Description: 生成 Kafka 基础 ConfigMap
//	@receiver c
//	@return *kafka.ConfigMap
//	@return error
func (c Config) baseConfigMap() (*kafka.ConfigMap, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Brokers, ","),
	}

	if c.ClientID != "" {
		configMap.SetKey("client.id", c.ClientID)
	}

	return configMap, nil
}
