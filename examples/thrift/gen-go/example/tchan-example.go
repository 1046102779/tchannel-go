// @generated Code generated by thrift-gen. Do not modify.

// Package example is generated code used to make or handle TChannel calls using Thrift.
package example

import (
	"fmt"

	"github.com/1046102779/tchannel-go/thrift"
	athrift "github.com/apache/thrift/lib/go/thrift"
)

// Interfaces for the service and client for the services defined in the IDL.

// TChanBase is the interface that defines the server handler and client interface.
type TChanBase interface {
	BaseCall(ctx thrift.Context) error
}

// TChanFirst is the interface that defines the server handler and client interface.
type TChanFirst interface {
	TChanBase

	AppError(ctx thrift.Context) error
	Echo(ctx thrift.Context, msg string) (string, error)
	Healthcheck(ctx thrift.Context) (*HealthCheckRes, error)
}

// TChanSecond is the interface that defines the server handler and client interface.
type TChanSecond interface {
	Test(ctx thrift.Context) error
}

// Implementation of a client and service handler.

type tchanBaseClient struct {
	thriftService string
	client        thrift.TChanClient
}

func NewTChanBaseInheritedClient(thriftService string, client thrift.TChanClient) *tchanBaseClient {
	return &tchanBaseClient{
		thriftService,
		client,
	}
}

// NewTChanBaseClient creates a client that can be used to make remote calls.
func NewTChanBaseClient(client thrift.TChanClient) TChanBase {
	return NewTChanBaseInheritedClient("Base", client)
}

func (c *tchanBaseClient) BaseCall(ctx thrift.Context) error {
	var resp BaseBaseCallResult
	args := BaseBaseCallArgs{}
	success, err := c.client.Call(ctx, c.thriftService, "BaseCall", &args, &resp)
	if err == nil && !success {
		switch {
		default:
			err = fmt.Errorf("received no result or unknown exception for BaseCall")
		}
	}

	return err
}

type tchanBaseServer struct {
	handler TChanBase
}

// NewTChanBaseServer wraps a handler for TChanBase so it can be
// registered with a thrift.Server.
func NewTChanBaseServer(handler TChanBase) thrift.TChanServer {
	return &tchanBaseServer{
		handler,
	}
}

func (s *tchanBaseServer) Service() string {
	return "Base"
}

func (s *tchanBaseServer) Methods() []string {
	return []string{
		"BaseCall",
	}
}

func (s *tchanBaseServer) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	switch methodName {
	case "BaseCall":
		return s.handleBaseCall(ctx, protocol)

	default:
		return false, nil, fmt.Errorf("method %v not found in service %v", methodName, s.Service())
	}
}

func (s *tchanBaseServer) handleBaseCall(ctx thrift.Context, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	var req BaseBaseCallArgs
	var res BaseBaseCallResult

	if err := req.Read(protocol); err != nil {
		return false, nil, err
	}

	err :=
		s.handler.BaseCall(ctx)

	if err != nil {
		return false, nil, err
	} else {
	}

	return err == nil, &res, nil
}

type tchanFirstClient struct {
	TChanBase

	thriftService string
	client        thrift.TChanClient
}

func NewTChanFirstInheritedClient(thriftService string, client thrift.TChanClient) *tchanFirstClient {
	return &tchanFirstClient{
		NewTChanBaseInheritedClient(thriftService, client),
		thriftService,
		client,
	}
}

// NewTChanFirstClient creates a client that can be used to make remote calls.
func NewTChanFirstClient(client thrift.TChanClient) TChanFirst {
	return NewTChanFirstInheritedClient("First", client)
}

func (c *tchanFirstClient) AppError(ctx thrift.Context) error {
	var resp FirstAppErrorResult
	args := FirstAppErrorArgs{}
	success, err := c.client.Call(ctx, c.thriftService, "AppError", &args, &resp)
	if err == nil && !success {
		switch {
		default:
			err = fmt.Errorf("received no result or unknown exception for AppError")
		}
	}

	return err
}

func (c *tchanFirstClient) Echo(ctx thrift.Context, msg string) (string, error) {
	var resp FirstEchoResult
	args := FirstEchoArgs{
		Msg: msg,
	}
	success, err := c.client.Call(ctx, c.thriftService, "Echo", &args, &resp)
	if err == nil && !success {
		switch {
		default:
			err = fmt.Errorf("received no result or unknown exception for Echo")
		}
	}

	return resp.GetSuccess(), err
}

func (c *tchanFirstClient) Healthcheck(ctx thrift.Context) (*HealthCheckRes, error) {
	var resp FirstHealthcheckResult
	args := FirstHealthcheckArgs{}
	success, err := c.client.Call(ctx, c.thriftService, "Healthcheck", &args, &resp)
	if err == nil && !success {
		switch {
		default:
			err = fmt.Errorf("received no result or unknown exception for Healthcheck")
		}
	}

	return resp.GetSuccess(), err
}

type tchanFirstServer struct {
	thrift.TChanServer

	handler TChanFirst
}

// NewTChanFirstServer wraps a handler for TChanFirst so it can be
// registered with a thrift.Server.
func NewTChanFirstServer(handler TChanFirst) thrift.TChanServer {
	return &tchanFirstServer{
		NewTChanBaseServer(handler),
		handler,
	}
}

func (s *tchanFirstServer) Service() string {
	return "First"
}

func (s *tchanFirstServer) Methods() []string {
	return []string{
		"AppError",
		"Echo",
		"Healthcheck",

		"BaseCall",
	}
}

func (s *tchanFirstServer) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	switch methodName {
	case "AppError":
		return s.handleAppError(ctx, protocol)
	case "Echo":
		return s.handleEcho(ctx, protocol)
	case "Healthcheck":
		return s.handleHealthcheck(ctx, protocol)

	case "BaseCall":
		return s.TChanServer.Handle(ctx, methodName, protocol)
	default:
		return false, nil, fmt.Errorf("method %v not found in service %v", methodName, s.Service())
	}
}

func (s *tchanFirstServer) handleAppError(ctx thrift.Context, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	var req FirstAppErrorArgs
	var res FirstAppErrorResult

	if err := req.Read(protocol); err != nil {
		return false, nil, err
	}

	err :=
		s.handler.AppError(ctx)

	if err != nil {
		return false, nil, err
	} else {
	}

	return err == nil, &res, nil
}

func (s *tchanFirstServer) handleEcho(ctx thrift.Context, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	var req FirstEchoArgs
	var res FirstEchoResult

	if err := req.Read(protocol); err != nil {
		return false, nil, err
	}

	r, err :=
		s.handler.Echo(ctx, req.Msg)

	if err != nil {
		return false, nil, err
	} else {
		res.Success = &r
	}

	return err == nil, &res, nil
}

func (s *tchanFirstServer) handleHealthcheck(ctx thrift.Context, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	var req FirstHealthcheckArgs
	var res FirstHealthcheckResult

	if err := req.Read(protocol); err != nil {
		return false, nil, err
	}

	r, err :=
		s.handler.Healthcheck(ctx)

	if err != nil {
		return false, nil, err
	} else {
		res.Success = r
	}

	return err == nil, &res, nil
}

type tchanSecondClient struct {
	thriftService string
	client        thrift.TChanClient
}

func NewTChanSecondInheritedClient(thriftService string, client thrift.TChanClient) *tchanSecondClient {
	return &tchanSecondClient{
		thriftService,
		client,
	}
}

// NewTChanSecondClient creates a client that can be used to make remote calls.
func NewTChanSecondClient(client thrift.TChanClient) TChanSecond {
	return NewTChanSecondInheritedClient("Second", client)
}

func (c *tchanSecondClient) Test(ctx thrift.Context) error {
	var resp SecondTestResult
	args := SecondTestArgs{}
	success, err := c.client.Call(ctx, c.thriftService, "Test", &args, &resp)
	if err == nil && !success {
		switch {
		default:
			err = fmt.Errorf("received no result or unknown exception for Test")
		}
	}

	return err
}

type tchanSecondServer struct {
	handler TChanSecond
}

// NewTChanSecondServer wraps a handler for TChanSecond so it can be
// registered with a thrift.Server.
func NewTChanSecondServer(handler TChanSecond) thrift.TChanServer {
	return &tchanSecondServer{
		handler,
	}
}

func (s *tchanSecondServer) Service() string {
	return "Second"
}

func (s *tchanSecondServer) Methods() []string {
	return []string{
		"Test",
	}
}

func (s *tchanSecondServer) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	switch methodName {
	case "Test":
		return s.handleTest(ctx, protocol)

	default:
		return false, nil, fmt.Errorf("method %v not found in service %v", methodName, s.Service())
	}
}

func (s *tchanSecondServer) handleTest(ctx thrift.Context, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	var req SecondTestArgs
	var res SecondTestResult

	if err := req.Read(protocol); err != nil {
		return false, nil, err
	}

	err :=
		s.handler.Test(ctx)

	if err != nil {
		return false, nil, err
	} else {
	}

	return err == nil, &res, nil
}
