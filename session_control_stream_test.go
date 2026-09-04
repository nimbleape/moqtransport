package moqtransport

import (
	"testing"
	"time"

	"github.com/mengelbart/moqtransport/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func sessionCloseError(s *Session) error {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()
	return s.closeErr
}

func acceptedControlStream(s *Session) *remoteControlStream {
	s.controlStreamLock.Lock()
	defer s.controlStreamLock.Unlock()
	return s.remoteControlStream
}

// capturingWriter records the control messages written to it.
type capturingWriter struct {
	messages []wire.ControlMessage
}

func (w *capturingWriter) Write(msg wire.ControlMessage) error {
	w.messages = append(w.messages, msg)
	return nil
}

// The SETUP message the session sends announces the MoQT implementation it
// speaks.
func TestSendSetupIncludesImplementationParameter(t *testing.T) {
	writer := &capturingWriter{}
	session := &Session{
		logger:             defaultLogger,
		conn:               newTestConnection(t),
		localControlStream: newLocalControlStream(writer),
		path:               "/path",
	}

	session.sendSetup()

	require.Len(t, writer.messages, 1)
	setup, ok := writer.messages[0].(*wire.Setup)
	require.True(t, ok)
	assert.Contains(t, setup.Options, wire.KeyValuePair{
		Type:  wire.MoqtImplementationParameterKey,
		Bytes: []byte(MOQT18.String()),
	})
}

// A session has exactly one remote control stream. A second one from the peer
// is a protocol violation and must not replace the first.
func TestDuplicateControlStreamClosesSession(t *testing.T) {
	conn := newTestConnection(t)
	session, err := NewSession(conn, "")
	require.NoError(t, err)

	first := conn.acceptUniStream(encodeControlMessage(t, setupWithPath("/path")))
	<-first.drained
	accepted := acceptedControlStream(session)
	require.NotNil(t, accepted)

	conn.acceptUniStream(encodeControlMessage(t, setupWithPath("/path")))
	require.Eventually(t, func() bool {
		return sessionCloseError(session) != nil
	}, time.Second, time.Millisecond)

	assert.ErrorIs(t, sessionCloseError(session), &SessionError{Code: uint64(ErrorCodeProtocolViolation)})
	assert.Same(t, accepted, acceptedControlStream(session))

	session.CloseWithError(0, "closing")
	goleak.VerifyNone(t)
}
