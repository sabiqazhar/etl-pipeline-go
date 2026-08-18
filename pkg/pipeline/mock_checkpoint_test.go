package pipeline

import "context"

// mockCheckpointStore tracks Save calls for testing.
type mockCheckpointStore struct {
	saved map[string][]byte
}

func newMockCheckpointStore() *mockCheckpointStore {
	return &mockCheckpointStore{saved: make(map[string][]byte)}
}

func (m *mockCheckpointStore) Save(ctx context.Context, componentID string, token []byte) error {
	m.saved[componentID] = token
	return nil
}

func (m *mockCheckpointStore) Load(ctx context.Context, componentID string) ([]byte, error) {
	return m.saved[componentID], nil
}

func (m *mockCheckpointStore) Delete(ctx context.Context, componentID string) error {
	delete(m.saved, componentID)
	return nil
}
func (m *mockCheckpointStore) List(ctx context.Context) ([]string, error) { return nil, nil }
func (m *mockCheckpointStore) Check() error                               { return nil }
func (m *mockCheckpointStore) Compact() error                             { return nil }
func (m *mockCheckpointStore) Close() error                               { return nil }
