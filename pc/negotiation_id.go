package pc

// invalidNegotiationID marks a description that carries no negotiation id.
const invalidNegotiationID = 0

// nextNegotiationID advances the local negotiation counter, skipping the reserved
// invalidNegotiationID sentinel when the uint32 wraps around so every offer carries a
// usable id.
func nextNegotiationID(cur uint32) uint32 {
	next := cur + 1
	if next == invalidNegotiationID {
		next++
	}
	return next
}

// negotiationAnswerAction is the decision for an incoming answer's negotiation id.
type negotiationAnswerAction int

const (
	negotiationAnswerAccept negotiationAnswerAction = iota
	negotiationAnswerStale
	negotiationAnswerDuplicate
	negotiationAnswerMismatch
)

// classifyAnswerNegotiationID decides what to do with an answer's negotiation id given the
// last accepted (remote) and the pending offer (local) ids. It is wrap-safe: the ids are
// uint32 counters that wrap, so recency is compared on the ring via a signed delta rather
// than a plain "<", which would treat a freshly wrapped id as stale. invalidNegotiationID
// (used when the peer does not carry an id) always accepts.
func classifyAnswerNegotiationID(negotiationID, remote, local uint32) negotiationAnswerAction {
	if negotiationID == invalidNegotiationID {
		return negotiationAnswerAccept
	}
	if remote == invalidNegotiationID {
		// No answer accepted yet (remote is only ever set to accepted, non-sentinel ids),
		// so there is no baseline to be stale against. This matters when the local counter
		// is seeded into the upper half of the range, where a ring comparison against 0
		// would otherwise read the first id as negative/"stale". Accept the pending offer;
		// anything else is a mismatch.
		if negotiationID != local {
			return negotiationAnswerMismatch
		}
		return negotiationAnswerAccept
	}
	switch delta := int32(negotiationID - remote); {
	case delta < 0:
		return negotiationAnswerStale
	case delta == 0:
		return negotiationAnswerDuplicate
	case negotiationID != local:
		return negotiationAnswerMismatch
	default:
		return negotiationAnswerAccept
	}
}
