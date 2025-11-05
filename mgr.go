package layer2

// None of what follows belongs in layer2 and all of it is a gross over-simplification
// of reality.
//
// Intended to allow students to unit test their implementation of reliable streams while
// ignoring the rest of the protocol(s) details
//
// Why is it here? To keep the interface seperate from the implementation

type Connection struct {
	ToNetwork   chan byte
	FromNetwork chan byte
}

type Mgr interface {
	// configure the network interface, can be called at most once
	IfConfig(myMac MacAddr, myName string) error

	// create a connection to another host (including yourself)
	// a connection is TCP-like:
	//     - implements a reliable, full-duplex byte stream
	//     - uniquely identified by (port1, host1, port2, host2)
	//
	// notice that we pretend IP addresses and sockets don't exist
	//
	// fails if called before IfConfig
	//
	Connect(myPort uint16, otherPort uint16, otherName string) (*Connection, error)

	// listen for for connections on the given port
	Listen(myPort uint16) (*Connection, error)
}
