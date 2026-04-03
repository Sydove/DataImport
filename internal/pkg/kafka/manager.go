package newkafka

import (
	"errors"
	"fmt"
	"sync"
)

type managedCloser interface {
	Close() error
}

var ErrManagerClosed = errors.New("kafka manager is closed")

type Manager struct {
	cfg     Config
	mu      sync.Mutex
	closed  bool
	closers []managedCloser
}

// NewManager
//
//	@Description: 创建 Kafka 资源管理器
//	@param cfg
//	@return *Manager
//	@return error
func NewManager(cfg Config) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Manager{
		cfg:     cfg,
		closers: make([]managedCloser, 0, 4),
	}, nil
}

// NewManagerFromViper
//
//	@Description: 从 viper 配置创建 Kafka 资源管理器
//	@return *Manager
//	@return error
func NewManagerFromViper() (*Manager, error) {
	cfg, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}
	return NewManager(cfg)
}

// NewProducer
//
//	@Description: 通过管理器创建 Producer 并纳入统一关闭管理
//	@receiver m
//	@param opts
//	@return *Producer
//	@return error
func (m *Manager) NewProducer(opts ProducerOptions) (*Producer, error) {
	if err := m.ensureOpen(); err != nil {
		return nil, err
	}

	baseConfig, err := m.cfg.baseConfigMap()
	if err != nil {
		return nil, err
	}

	producer, err := newProducer(baseConfig, opts)
	if err != nil {
		return nil, err
	}

	if err := m.register(producer); err != nil {
		return nil, err
	}
	return producer, nil
}

// NewConsumer
//
//	@Description: 通过管理器创建 Consumer 并纳入统一关闭管理
//	@receiver m
//	@param opts
//	@return *Consumer
//	@return error
func (m *Manager) NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if err := m.ensureOpen(); err != nil {
		return nil, err
	}

	baseConfig, err := m.cfg.baseConfigMap()
	if err != nil {
		return nil, err
	}

	consumer, err := newConsumer(baseConfig, opts)
	if err != nil {
		return nil, err
	}

	if err := m.register(consumer); err != nil {
		return nil, err
	}
	return consumer, nil
}

// NewAdmin
//
//	@Description: 通过管理器创建 Admin Client 并纳入统一关闭管理
//	@receiver m
//	@return *Admin
//	@return error
func (m *Manager) NewAdmin() (*Admin, error) {
	if err := m.ensureOpen(); err != nil {
		return nil, err
	}

	baseConfig, err := m.cfg.baseConfigMap()
	if err != nil {
		return nil, err
	}

	admin, err := newAdmin(baseConfig, m.cfg.MetadataTimeout)
	if err != nil {
		return nil, err
	}

	if err := m.register(admin); err != nil {
		return nil, err
	}
	return admin, nil
}

// Close
//
//	@Description: 逆序关闭管理器持有的 Kafka 资源
//	@receiver m
//	@return error
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}

	m.closed = true
	closers := append([]managedCloser(nil), m.closers...)
	m.closers = nil
	m.mu.Unlock()

	var firstErr error
	// 逆序关闭，尽量符合“后创建的资源先释放”的生命周期习惯。
	for i := len(closers) - 1; i >= 0; i-- {
		if err := closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return fmt.Errorf("close kafka manager resources: %w", firstErr)
	}
	return nil
}

// ensureOpen
//
//	@Description: 校验 Manager 是否仍处于可创建资源状态
//	@receiver m
//	@return error
func (m *Manager) ensureOpen() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	return nil
}

// register
//
//	@Description: 将可关闭资源登记到 Manager 中；若 Manager 已关闭则立即回收资源
//	@receiver m
//	@param closer
//	@return error
func (m *Manager) register(closer managedCloser) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		if err := closer.Close(); err != nil {
			return fmt.Errorf("%w: %w", ErrManagerClosed, err)
		}
		return ErrManagerClosed
	}
	m.closers = append(m.closers, closer)
	m.mu.Unlock()
	return nil
}
