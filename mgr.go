package layer2

// None of what follows belongs in layer2 but it's just an interface definition
// to be implemented by higher layers

// A simplified TCP-like session abstraction with very limited functionality: no ports,
// no connection management, no handshakes, no IP addresses, no DNS, etc.
//
// We use a string to represent a configured interface and allow a maximum of one
// session between any pair of names.

type Mgr interface {
	NewStream(dstName string) (chan byte, chan byte, error)
}

type ConfigMgr interface {
	// a successful call to IfConfig:
	//    (1) creates and returns a Mgr that can create sessions over the given interface
	//    (2) associates the given name with the given interface.
	//    (3) the behavior is undefined if another interface has already been associated with the same name.
	IfConfig(sw *SwitchConnection, name string) (Mgr, error)
}
