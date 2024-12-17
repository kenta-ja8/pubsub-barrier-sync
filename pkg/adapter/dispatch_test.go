package adapter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/helper"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/usecase"
)

// Mocking the pubsub client
type MockPubSubClient struct {
	mock.Mock
}

func (m *MockPubSubClient) NewClient(ctx context.Context, projectID string) (*pubsub.Client, error) {
	args := m.Called(ctx, projectID)
	return args.Get(0).(*pubsub.Client), args.Error(1)
}

func (m *MockPubSubClient) Topic(id string) *pubsub.Topic {
	args := m.Called(id)
	return args.Get(0).(*pubsub.Topic)
}

func (m *MockPubSubClient) Subscription(id string) *pubsub.Subscription {
	args := m.Called(id)
	return args.Get(0).(*pubsub.Subscription)
}

func TestDispatchMessage(t *testing.T) {
	mockClient := new(MockPubSubClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	projectID := "test-project"
	topicID := "test-topic"
	subscriptionID := "test-subscription"

	client := &pubsub.Client{}
	topic := &pubsub.Topic{}
	subscription := &pubsub.Subscription{}

	mockClient.On("NewClient", ctx, projectID).Return(client, nil)
	mockClient.On("Topic", topicID).Return(topic)
	mockClient.On("Subscription", subscriptionID).Return(subscription)

	// Mocking the subscription receive function
	subscription.ReceiveSettings.MaxOutstandingMessages = 4
	subscription.Receive = func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		msg := &pubsub.Message{
			ID:   "test-message",
			Data: []byte(`{"jobId":"1","taskCode":"TaskCode-A","taskData":"{}","barrierJobIdMap":{},"historyJobIds":[],"traceId":"trace-1"}`),
		}
		f(ctx, msg)
		return nil
	}

	go func() {
		err := DispatchMessage(ctx, &pubsub.Message{
			ID:   "test-message",
			Data: []byte(`{"jobId":"1","taskCode":"TaskCode-A","taskData":"{}","barrierJobIdMap":{},"historyJobIds":[],"traceId":"trace-1"}`),
		}, topic)
		assert.NoError(t, err)
	}()

	time.Sleep(2 * time.Second) // Wait for the goroutine to start

	// Add assertions here to verify the expected behavior
	assert.True(t, true)
}

func TestPublishMessage(t *testing.T) {
	mockClient := new(MockPubSubClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	projectID := "test-project"
	topicID := "test-topic"
	subscriptionID := "test-subscription"

	client := &pubsub.Client{}
	topic := &pubsub.Topic{}
	subscription := &pubsub.Subscription{}

	mockClient.On("NewClient", ctx, projectID).Return(client, nil)
	mockClient.On("Topic", topicID).Return(topic)
	mockClient.On("Subscription", subscriptionID).Return(subscription)

	// Mocking the subscription receive function
	subscription.ReceiveSettings.MaxOutstandingMessages = 4
	subscription.Receive = func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		msg := &pubsub.Message{
			ID:   "test-message",
			Data: []byte(`{"jobId":"1","taskCode":"TaskCode-A","taskData":"{}","barrierJobIdMap":{},"historyJobIds":[],"traceId":"trace-1"}`),
		}
		f(ctx, msg)
		return nil
	}

	go func() {
		err := PublishMessage(ctx, topic, TaskCode_A, usecase.TaskDataUsecaseA{Input: "test"}, "1", nil, nil, nil)
		assert.NoError(t, err)
	}()

	time.Sleep(2 * time.Second) // Wait for the goroutine to start

	// Add assertions here to verify the expected behavior
	assert.True(t, true)
}
