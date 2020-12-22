package mydynamo

// VectorClock map of (NodeId, version) [1,0,0] length = number of node cluster
type VectorClock struct {
	VectorClock map[string]int
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	return VectorClock{
		VectorClock: make(map[string]int),
	}
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	less := false
	for nodeID, version := range s.VectorClock {
		if _, ok := otherClock.VectorClock[nodeID]; !ok && version != 0 {
			return false
		} else if version < otherClock.VectorClock[nodeID] {
			less = true
		} else if version > otherClock.VectorClock[nodeID] {
			return false
		}
	}

	for nodeID, version := range otherClock.VectorClock {
		if _, ok := s.VectorClock[nodeID]; !ok && version != 0 {
			less = true
		}
	}

	return less
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	return !s.LessThan(otherClock) && !otherClock.LessThan(s)
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	s.VectorClock[nodeId]++
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	// add non existing key to s
	for _, clock := range clocks {
		for nodeID, otherVersion := range clock.VectorClock {
			if version, found := s.VectorClock[nodeID]; !found || (found && version < otherVersion) {
				s.VectorClock[nodeID] = otherVersion
			}
		}
	}
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	for nodeId, version := range s.VectorClock {
		if otherVersion, found := otherClock.VectorClock[nodeId]; (!found && version != 0) || otherVersion != version {
			return false
		}
	}

	for nodeId, version := range otherClock.VectorClock {
		if otherVersion, found := s.VectorClock[nodeId]; (!found && version != 0) || otherVersion != version {
			return false
		}
	}

	return true
}
