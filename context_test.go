// Copyright (c) 2015-present Jeevanandam M (jeeva@myjeeva.com), All rights reserved.
// 2016 Andrew Grigorev (https://github.com/ei-grad)
// resty source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.
// SPDX-License-Identifier: MIT

package resty

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSetContext(t *testing.T) {
	ts := createGetServer(t)
	defer ts.Close()

	c := dcnl()

	assertNil(t, c.Context())

	c.SetContext(context.Background())

	resp, err := c.R().Get(ts.URL + "/")

	assertError(t, err)
	assertEqual(t, http.StatusOK, resp.StatusCode())
	assertEqual(t, "200 OK", resp.Status())
	assertEqual(t, "TestGet: text response", resp.String())

	logResponse(t, resp)
}

func TestRequestSetContext(t *testing.T) {
	ts := createGetServer(t)
	defer ts.Close()

	resp, err := dcnl().R().
		SetContext(context.Background()).
		Get(ts.URL + "/")

	assertError(t, err)
	assertEqual(t, http.StatusOK, resp.StatusCode())
	assertEqual(t, "200 OK", resp.Status())
	assertEqual(t, "TestGet: text response", resp.String())

	logResponse(t, resp)
}

func TestSetContextWithError(t *testing.T) {
	ts := createGetServer(t)
	defer ts.Close()

	resp, err := dcnlr().
		SetContext(context.Background()).
		Get(ts.URL + "/mypage")

	assertError(t, err)
	assertEqual(t, http.StatusBadRequest, resp.StatusCode(), "expected bad request status code")
	assertEqual(t, "", resp.String(), "expected empty response body on bad request")

	logResponse(t, resp)
}

func TestSetContextCancel(t *testing.T) {
	ch := make(chan struct{})
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			ch <- struct{}{} // tell test request is finished
		}()
		t.Logf("Server: %v %v", r.Method, r.URL.Path)
		ch <- struct{}{}
		<-ch // wait for client to finish request
		select {
		case <-r.Context().Done():
			t.Log("Server: context cancelled, aborting write")
			return
		default:
		}
		n, err := w.Write([]byte("TestSetContextCancel: response"))
		t.Logf("Server: wrote %d bytes", n)
		t.Logf("Server: err is %v ", err)
	})
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-ch // wait for server to start request handling
		cancel()
	}()

	_, err := dcnl().R().
		SetContext(ctx).
		Get(ts.URL + "/")

	ch <- struct{}{} // tell server to continue request handling

	<-ch // wait for server to finish request handling

	t.Logf("Error: %v", err)
	if !errIsContextCanceled(err) {
		t.Errorf("Got unexpected error: %v", err)
	}
}

func TestSetContextCancelRetry(t *testing.T) {
	reqCount := 0
	ch := make(chan struct{})
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		defer func() {
			ch <- struct{}{} // tell test request is finished
		}()
		t.Logf("Server: %v %v", r.Method, r.URL.Path)
		ch <- struct{}{}
		<-ch // wait for client to finish request
		select {
		case <-r.Context().Done():
			t.Log("Server: context cancelled, aborting write")
			return
		default:
		}
		n, err := w.Write([]byte("TestSetContextCancel: response"))
		t.Logf("Server: wrote %d bytes", n)
		t.Logf("Server: err is %v ", err)
	})
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-ch // wait for server to start request handling
		cancel()
	}()

	c := dcnl().
		SetTimeout(time.Second * 3).
		SetRetryCount(3)

	_, err := c.R().
		SetContext(ctx).
		Get(ts.URL + "/")

	ch <- struct{}{} // tell server to continue request handling

	<-ch // wait for server to finish request handling

	t.Logf("Error: %v", err)
	if !errIsContextCanceled(err) {
		t.Errorf("Got unexpected error: %v", err)
	}

	if reqCount != 1 {
		t.Errorf("Request was retried %d times instead of 1", reqCount)
	}
}

// TestSetContextDeadlineStopsRetries ensures that when the user-provided
// request context hits its deadline, [Request.Execute] stops its retry loop
// and returns promptly with context.DeadlineExceeded.
//
// The current contract is that the subsequent backoff timer / ctx.Done()
// select below this block catches the dead context and stops the loop. This
// test pins that behavior so any future change to that control flow (or to
// the backoff select) is caught.
func TestSetContextDeadlineStopsRetries(t *testing.T) {
	var reqCount int32
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		// Sleep longer than the parent ctx deadline so every attempt collides
		// with the deadline.
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	c := dcnl().AddRetryConditions(func(_ *Response, err error) bool {
		return err != nil
	})

	start := time.Now()
	resp, err := c.R().
		SetContext(ctx).
		SetRetryCount(5).
		SetRetryWaitTime(time.Millisecond).
		SetRetryMaxWaitTime(time.Millisecond).
		Get(ts.URL + "/")
	elapsed := time.Since(start)

	assertErrorIs(t, context.DeadlineExceeded, err, "expected context deadline exceeded error")

	// The first attempt burns the ctx deadline. Resty must stop the retry
	// loop as soon as it observes the dead context, without cycling through
	// any further attempts (even "no-op" ones that fail locally at Do()).
	if resp.Request.Attempt != 1 {
		t.Errorf("expected Resty to stop after 1 attempt once ctx deadline hit, got Attempt=%d (reqCount=%d)",
			resp.Request.Attempt, reqCount)
	}

	// And Execute must return close to the deadline, not keep spinning
	// through backoff waits.
	if elapsed > 500*time.Millisecond {
		t.Errorf("Execute ran well past ctx deadline: elapsed=%s", elapsed)
	}
}

func TestSetContextCancelWithError(t *testing.T) {
	ch := make(chan struct{})
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			ch <- struct{}{} // tell test request is finished
		}()
		t.Logf("Server: %v %v", r.Method, r.URL.Path)
		t.Log("Server: sending StatusBadRequest response")
		w.WriteHeader(http.StatusBadRequest)
		ch <- struct{}{}
		<-ch // wait for client to finish request
		select {
		case <-r.Context().Done():
			t.Log("Server: context cancelled, aborting write")
			return
		default:
		}
		n, err := w.Write([]byte("TestSetContextCancelWithError: response"))
		t.Logf("Server: wrote %d bytes", n)
		t.Logf("Server: err is %v ", err)
	})
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-ch // wait for server to start request handling
		cancel()
	}()

	_, err := dcnl().R().
		SetContext(ctx).
		Get(ts.URL + "/")

	ch <- struct{}{} // tell server to continue request handling

	<-ch // wait for server to finish request handling

	t.Logf("Error: %v", err)
	if !errIsContextCanceled(err) {
		t.Errorf("Got unexpected error: %v", err)
	}
}

func TestClientRetryWithSetContext(t *testing.T) {
	var attempt int32
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Method: %v", r.Method)
		t.Logf("Path: %v", r.URL.Path)
		if atomic.AddInt32(&attempt, 1) <= 4 {
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = w.Write([]byte("TestClientRetry page"))
	})
	defer ts.Close()

	c := dcnl().
		SetTimeout(50 * time.Millisecond).
		SetRetryCount(3)

	_, err := c.R().
		SetContext(context.Background()).
		Get(ts.URL + "/")

	assertNotNil(t, ts)
	assertNotNil(t, err)
	assertErrorIs(t, context.DeadlineExceeded, err, "expected context deadline exceeded error")
}

func TestRequestContext(t *testing.T) {
	client := dcnl()
	r := client.NewRequest()
	assertNotNil(t, r.Context(), "expected default context to be non-nil")

	r.SetContext(context.Background())
	assertNotNil(t, r.Context(), "expected context to be set")
}

func errIsContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
