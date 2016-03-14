// Copyright 2015 go-dockerclient authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package docker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"sync/atomic"
	"time"
)

// APIEvents represents an event returned by the API.
type APIEvents struct {
	Status string `json:"Status,omitempty" yaml:"Status,omitempty"`
	ID     string `json:"ID,omitempty" yaml:"ID,omitempty"`
	From   string `json:"From,omitempty" yaml:"From,omitempty"`
	Time   int64  `json:"Time,omitempty" yaml:"Time,omitempty"`
}

type eventMonitoringState struct {
	sync.RWMutex
	sync.WaitGroup
	enabled   bool
	lastSeen  *int64
	C         chan *APIEvents
	errC      chan error
	listeners []chan<- *APIEvents
}

const (
	maxMonitorConnRetries = 5
	retryInitialWaitTime  = 10.
)

var (
	// ErrNoListeners is the error returned when no listeners are available
	// to receive an event.
	ErrNoListeners = errors.New("no listeners present to receive event")

	// ErrListenerAlreadyExists is the error returned when the listerner already
	// exists.
	ErrListenerAlreadyExists = errors.New("listener already exists for docker events")

	// EOFEvent is sent when the event listener receives an EOF error.
	EOFEvent = &APIEvents{
		Status: "EOF",
	}
)

// AddEventListener adds a new listener to container events in the Docker API.
//
// The parameter is a channel through which events will be sent.
func (c *Client) AddEventListener(listener chan<- *APIEvents) error {
	log.Printf("[go-dockerclient] Adding event listener")
	var err error
	if !c.eventMonitor.isEnabled() {
		err = c.eventMonitor.enableEventMonitoring(c)
		if err != nil {
			return err
		}
	}
	err = c.eventMonitor.addListener(listener)
	if err != nil {
		return err
	}
	return nil
}

// RemoveEventListener removes a listener from the monitor.
func (c *Client) RemoveEventListener(listener chan *APIEvents) error {
	err := c.eventMonitor.removeListener(listener)
	if err != nil {
		return err
	}
	if len(c.eventMonitor.listeners) == 0 {
		c.eventMonitor.disableEventMonitoring()
	}
	return nil
}

func (eventState *eventMonitoringState) addListener(listener chan<- *APIEvents) error {
	eventState.Lock()
	defer eventState.Unlock()
	if listenerExists(listener, &eventState.listeners) {
		return ErrListenerAlreadyExists
	}
	eventState.Add(1)
	eventState.listeners = append(eventState.listeners, listener)
	return nil
}

func (eventState *eventMonitoringState) removeListener(listener chan<- *APIEvents) error {
	eventState.Lock()
	defer eventState.Unlock()
	if listenerExists(listener, &eventState.listeners) {
		var newListeners []chan<- *APIEvents
		for _, l := range eventState.listeners {
			if l != listener {
				newListeners = append(newListeners, l)
			}
		}
		eventState.listeners = newListeners
		eventState.Add(-1)
	}
	return nil
}

func (eventState *eventMonitoringState) closeListeners() {
	for _, l := range eventState.listeners {
		close(l)
		eventState.Add(-1)
	}
	eventState.listeners = nil
}

func listenerExists(a chan<- *APIEvents, list *[]chan<- *APIEvents) bool {
	for _, b := range *list {
		if b == a {
			return true
		}
	}
	return false
}

func (eventState *eventMonitoringState) enableEventMonitoring(c *Client) error {
	eventState.Lock()
	defer eventState.Unlock()
	if !eventState.enabled {
		eventState.enabled = true
		var lastSeenDefault = int64(0)
		eventState.lastSeen = &lastSeenDefault
		eventState.C = make(chan *APIEvents, 100)
		eventState.errC = make(chan error, 1)
		go eventState.monitorEvents(c)
	}
	return nil
}

func (eventState *eventMonitoringState) disableEventMonitoring() error {
	eventState.Lock()
	defer eventState.Unlock()

	eventState.closeListeners()

	eventState.Wait()

	if eventState.enabled {
		eventState.enabled = false
		close(eventState.C)
		close(eventState.errC)
	}
	return nil
}

func (eventState *eventMonitoringState) monitorEvents(c *Client) {
	log.Printf("[go-dockerclient] entering monitorEvents")
	var err error
	for eventState.noListeners() {
		time.Sleep(10 * time.Millisecond)
	}
	if err = eventState.connectWithRetry(c); err != nil {
		log.Printf("[go-dockerclient] monitorEvents exceeded retries, disabling event monitoring")
		// terminate if connect failed
		eventState.disableEventMonitoring()
		return
	}
	for eventState.isEnabled() {
		timeout := time.After(100 * time.Millisecond)
		select {
		case ev, ok := <-eventState.C:
			if !ok {
				return
			}
			if ev == EOFEvent {
				eventState.disableEventMonitoring()
				return
			}
			eventState.updateLastSeen(ev)
			go eventState.sendEvent(ev)
		case err = <-eventState.errC:
			if err == ErrNoListeners {
				eventState.disableEventMonitoring()
				return
			} else if err != nil {
				log.Printf("[go-dockerclient] monitorEvents received error, restarting self")
				defer func() { go eventState.monitorEvents(c) }()
				return
			}
		case <-timeout:
			continue
		}
	}
}

func (eventState *eventMonitoringState) connectWithRetry(c *Client) error {
	var retries int
	var err error
	for err = c.eventHijack(atomic.LoadInt64(eventState.lastSeen), eventState.C, eventState.errC); err != nil && retries < maxMonitorConnRetries; retries++ {
		waitTime := int64(retryInitialWaitTime * math.Pow(2, float64(retries)))
		time.Sleep(time.Duration(waitTime) * time.Millisecond)
		err = c.eventHijack(atomic.LoadInt64(eventState.lastSeen), eventState.C, eventState.errC)
		if err != nil {
			log.Printf("[go-dockerclient] connectWithRetry (retries = %v, err = %v)", retries, err)
		}
	}
	return err
}

func (eventState *eventMonitoringState) noListeners() bool {
	eventState.RLock()
	defer eventState.RUnlock()
	return len(eventState.listeners) == 0
}

func (eventState *eventMonitoringState) isEnabled() bool {
	eventState.RLock()
	defer eventState.RUnlock()
	return eventState.enabled
}

func (eventState *eventMonitoringState) sendEvent(event *APIEvents) {
	eventState.RLock()
	defer eventState.RUnlock()
	eventState.Add(1)
	defer eventState.Done()
	if eventState.enabled {
		if len(eventState.listeners) == 0 {
			eventState.errC <- ErrNoListeners
			return
		}

		for _, listener := range eventState.listeners {
			listener <- event
		}
	}
}

func (eventState *eventMonitoringState) updateLastSeen(e *APIEvents) {
	eventState.Lock()
	defer eventState.Unlock()
	if atomic.LoadInt64(eventState.lastSeen) < e.Time {
		atomic.StoreInt64(eventState.lastSeen, e.Time)
	}
}

type loggingReader struct {
	r io.Reader
}

func (lr *loggingReader) Read(p []byte) (n int, err error) {
	n, err = lr.r.Read(p)
	log.Printf("[go-dockerclient] [%v] %v %v", string(p[:n]), n, err)
	return
}

func (c *Client) eventHijack(startTime int64, eventChan chan *APIEvents, errChan chan error) error {
	uri := "/events"
	if startTime != 0 {
		uri += fmt.Sprintf("?since=%d", startTime)
	}
	protocol := c.endpointURL.Scheme
	address := c.endpointURL.Path
	if protocol != "unix" {
		protocol = "tcp"
		address = c.endpointURL.Host
	}
	var dial net.Conn
	var err error
	if c.TLSConfig == nil {
		dial, err = c.Dialer.Dial(protocol, address)
	} else {
		dial, err = tlsDialWithDialer(c.Dialer, protocol, address, c.TLSConfig)
	}
	if err != nil {
		log.Printf("[go-dockerclient] eventHijack dial error %v", err)
		return err
	}
	conn := httputil.NewClientConn(dial, nil)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		log.Printf("[go-dockerclient] eventHijack new request error %v", err)
		return err
	}
	log.Printf("[go-dockerclient] eventHijack new request %v", req)
	res, err := conn.Do(req)
	if err != nil {
		log.Printf("[go-dockerclient] eventHijack do request error %v", err)
		return err
	}
	go func(res *http.Response, conn *httputil.ClientConn) {
		defer conn.Close()
		defer res.Body.Close()
		decoder := json.NewDecoder(&loggingReader{res.Body})
		for {
			log.Printf("[go-dockerclient] eventHijack waiting for event")
			var event APIEvents
			if err = decoder.Decode(&event); err != nil {
				log.Printf("[go-dockerclient] eventHijack event decode error %v", err)
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					log.Printf("[go-dockerclient] eventHijack IO EOF %v", err)
					if c.eventMonitor.isEnabled() {
						// Signal that we're exiting.
						eventChan <- EOFEvent
					}
					break
				}
				errChan <- err
			}
			if event.Time == 0 {
				continue
			}
			if !c.eventMonitor.isEnabled() {
				return
			}
			log.Printf("[go-dockerclient] decoded event %v", event)
			eventChan <- &event
		}
	}(res, conn)
	return nil
}
