//go:build !linux

package accountdb

import "time"

// LookupBound bounds one account lookup as far as callers assume. Outside
// Linux the lookup goes straight to os/user with no subprocess to time out, so
// this is a sizing assumption rather than an enforced limit.
const LookupBound = 5 * time.Second

var (
	lookupUID   = osUserLookupUID
	lookupName  = osUserLookupName
	lookupGroup = osUserLookupGroup
	groupIDs    = osUserGroupIDs
)
