package layer2

import "fmt"
import "log"
import "math/rand"
import "sync/atomic"
import "time"

// Simulated Layer 2 (Data Link Layer) for a network stack

// Maximum Transmission Unit
const MTU = 1500

/////////////////
// MAC Address //
/////////////////

// Abstract representation of a MAC address
type MacAddr struct {
	low  uint32
	high uint16
}

var broadcastMac = MacAddr{low: 0xFFFFFFFF, high: 0xFFFF}

////////////////////
// Ethernet Frame //
////////////////////

type EtherType uint16

const (
	EtherTypeIPv4 EtherType = 0x0800
	EtherTypeARP  EtherType = 0x0806
)

// An abstract representation of an Ethernet frame
type EtherFrame struct {
	// the destination MAC address
	Dst MacAddr

	// the source MAC address
	Src MacAddr

	// The type field (IPV4, ARP, ...)
	Type EtherType

	// The payload of the frame (<= MTU bytes)
	Payload []byte
}

///////////////////////
// Switch Connection //
///////////////////////

type SwitchConnection struct {
	// The switch this connection is plugged into
	Switch *Switch

	// The MAC address of the NIC on this connection
	MyMac MacAddr

	// Channels to the physical layer (sends into the switch)
	ToPhysicalLayer chan *EtherFrame

	// Channels from the physical layer (receives from the switch)
	FromPhysicalLayer chan *EtherFrame
}

////////////
// Switch //
////////////

// Simualated Layer 2 switch with unlimited bandwidth and ports

type Switch struct {
	// The routing table, populated on send
	Routing     map[MacAddr](chan *EtherFrame)
	Connections []*SwitchConnection

	// This is a simulation-only parameter used to guarantee forward progress
	// and avoid deadlocks in tests and simulations.
	// It has the side benefit of simulating limited buffer sizes in real hardware.
	bufferSize int

	// This is a simulation-only parameter used to simulate send delays
	maxSendDelayMicroSeconds int

	// This is a simulation-only parameter used to simulate frame duplication
	duplicationChance float32

	// stats
	NSendAttempts     uint64
	NBroadcastFrames  uint64
	NDroppedFrames    uint64
	NSentFrames       uint64
	NDuplicatedFrames uint64
	NIllegalFrames    uint64
}

func NewSwitch(bufferSize int, maxSendDelayMicroSeconds int, duplicationChance float32) *Switch {
	return &Switch{
		Routing:                  make(map[MacAddr](chan *EtherFrame)),
		Connections:              make([]*SwitchConnection, 0),
		bufferSize:               bufferSize,
		maxSendDelayMicroSeconds: maxSendDelayMicroSeconds,
		duplicationChance:        duplicationChance,
	}
}

// Simulate plugging a NIC into the switch at the given port with the given MAC address
func (s *Switch) Plug(port uint, mac MacAddr) (*SwitchConnection, error) {

	if mac == broadcastMac {
		return nil, fmt.Errorf("cannot use broadcast MAC address as source MAC")
	}

	conn := &SwitchConnection{
		Switch:            s,
		MyMac:             mac,
		FromPhysicalLayer: make(chan *EtherFrame, s.bufferSize),
	}
	s.Connections = append(s.Connections, conn)

	return conn, nil
}

// Send a frame into the switch from a given connection
// Could drop, duplicate, delay, and reorder frames to simulate real network conditions. None
// of those are considered errors.
func (sc *SwitchConnection) SendFrame(dest MacAddr, data []byte) error {
	atomic.AddUint64(&sc.Switch.NSendAttempts, 1)

	// Check for oversize frame
	if len(data) > MTU {
		atomic.AddUint64(&sc.Switch.NIllegalFrames, 1)
		return fmt.Errorf("buffer size exceeds MTU: %d > %d", len(data), MTU)
	}

	// Remember the source MAC
	sc.Switch.Routing[sc.MyMac] = sc.FromPhysicalLayer

	// Send asynchronously:
	//      - sender never blocks
	//      - gives this a place to simulate delays, reordering, loss, etc.
	//      - drop a frame on timeout
	// The goroutine ready queue is effectively turning into a buffer of pending sends
	doSend := func() {

		// Simulate send delay
		if sc.Switch.maxSendDelayMicroSeconds > 0 {
			delayMicroSeconds := rand.Intn(sc.Switch.maxSendDelayMicroSeconds)
			time.Sleep(time.Duration(delayMicroSeconds) * time.Microsecond)
		}

		frame := &EtherFrame{
			Dst:     dest,
			Src:     sc.MyMac,
			Type:    EtherTypeIPv4,
			Payload: data,
		}

		// Race conditions are possible and encouraged in this simulation
		// Receivers must be prepared to deal with:
		//     - out-of-order frames
		//     - lost frames (never arrive)
		//     - duplicated frames
		//     - delayed frames
		//     - mis-delivered frames (arrive at wrong destination)
		outChan, ok := sc.Switch.Routing[dest]
		if ok {
			// Known destination, send directly
			select {
			case outChan <- frame:
			default:
				log.Printf("failed to send frame to %v, dropping\n", dest)
				atomic.AddUint64(&sc.Switch.NDroppedFrames, 1)
				// Channel is full, drop the frame
			}
		} else {
			// Unknown destination, broadcast to all except sender
			// Broadcast MAC works because we disallow it as a source MAC

			log.Printf("broadcasting frame from %v to %v\n", sc.MyMac, dest)
			atomic.AddUint64(&sc.Switch.NBroadcastFrames, 1)

			for _, conn := range sc.Switch.Connections {
				if conn != sc {
					log.Printf("  sending to %v\n", conn.MyMac)
					select {
					case conn.FromPhysicalLayer <- frame:
						atomic.AddUint64(&sc.Switch.NSentFrames, 1)
					default:
						log.Printf("failed to send frame to %v, dropping\n", conn.MyMac)
						atomic.AddUint64(&sc.Switch.NDroppedFrames, 1)
						// Channel is full, drop the frame
					}
				}
			}

		}
	}

	// send once, could fail
	doSend()

	// possibly duplicate
	if sc.Switch.duplicationChance > 0.0 {
		if rand.Float32() >= sc.Switch.duplicationChance {
			log.Printf("duplicating frame from %v to %v\n", sc.MyMac, dest)
			atomic.AddUint64(&sc.Switch.NDuplicatedFrames, 1)
			go doSend()
		}
	}

	return nil
}
