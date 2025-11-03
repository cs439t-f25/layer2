package layer2

import (
	"io/ioutil"
	"log"
	"testing"
)

func init() {
	// comment out to see log output during tests
	log.SetOutput(ioutil.Discard)
}

func TestNewSwitch(t *testing.T) {
	s := NewSwitch(100, 10*1000, 0.1)
	if s.BufferSize != 100 {
		t.Errorf("expected buffer size 100, got %d", s.BufferSize)
	}
	if s.MaxSendDelayMicroSeconds != 10*1000 {
		t.Errorf("expected max send delay 10000, got %d", s.MaxSendDelayMicroSeconds)
	}
	if s.DuplicationChance != 0.1 {
		t.Errorf("expected duplication chance 0.1, got %f", s.DuplicationChance)
	}
}

func TestPlug(t *testing.T) {
	s := NewSwitch(100, 10*1000, 0.1)
	mac := MacAddr{1, 2}
	conn, err := s.Plug(1, mac)
	if err != nil {
		t.Fatalf("Failed to plug in MAC %v: %v", mac, err)
	}
	if conn.MyMac != mac {
		t.Errorf("Expected MAC %v, got %v", mac, conn.MyMac)
	}
	if conn.Switch != s {
		t.Errorf("Expected switch %v, got %v", s, conn.Switch)
	}
}

func TestPlugBroadcastMac(t *testing.T) {
	s := NewSwitch(100, 10*1000, 0.1)
	_, err := s.Plug(1, BroadcastMac)
	if err == nil {
		t.Fatalf("Expected error when plugging in broadcast MAC, got nil")
	}
}

func TestSendFrameOversize(t *testing.T) {
	s := NewSwitch(100, 10*1000, 0.1)
	mac := MacAddr{1, 2}
	conn, err := s.Plug(1, mac)
	if err != nil {
		t.Fatalf("Failed to plug in MAC %v: %v", mac, err)
	}
	oversizeData := make([]byte, MTU+1)
	err = conn.SendFrame(MacAddr{3, 4}, oversizeData)
	if err == nil {
		t.Fatalf("Expected error when sending oversize frame, got nil")
	}
}

func TestSendReceiveFrame(t *testing.T) {
	s := NewSwitch(100, 0, 0.0)

	// side-channel to verify reception -- test only
	ch := make(chan bool, 1)

	// Receiver
	recieveMac := MacAddr{3, 4}
	recieveConn, err := s.Plug(2, recieveMac)
	if err != nil {
		t.Errorf("Failed to plug in MAC %v: %v", recieveMac, err)
	}

	go func() {
		frame := <-recieveConn.FromPhysicalLayer
		if string(frame.Payload) != "test data" {
			t.Errorf("Expected payload 'test data', got %s", frame.Payload)
		}

		ch <- true
	}()

	// Sender
	sendMac := MacAddr{1, 2}
	sendConn, err := s.Plug(1, sendMac)
	if err != nil {
		t.Fatalf("Failed to plug in MAC %v: %v", sendMac, err)
	}
	data := []byte("test data")
	err = sendConn.SendFrame(recieveMac, data)
	if err != nil {
		t.Fatalf("Failed to send frame: %v", err)
	}

	<-ch

}
