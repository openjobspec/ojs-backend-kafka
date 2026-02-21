package core

import (
	common "github.com/openjobspec/ojs-go-backend-common/core"
)

const (
	EventJobStateChanged = common.EventJobStateChanged
	EventJobProgress     = common.EventJobProgress
	EventServerShutdown  = common.EventServerShutdown
)

type JobEvent = common.JobEvent
type EventPublisher = common.EventPublisher
type EventSubscriber = common.EventSubscriber

var NewStateChangedEvent = common.NewStateChangedEvent
