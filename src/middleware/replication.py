INITIAL_VERSION = 0

VERSIONED_FIELDS = ("worker_id", "host", "port", "models", "status", "owner")
NEW = "new"
NEWER = "newer"
STALE = "stale"
DUPLICATE = "duplicate"
CONFLICT_WON = "conflict_won"
CONFLICT_LOST = "conflict_lost"
ACCEPTED_OUTCOMES = {NEW, NEWER, CONFLICT_WON}

def validate_version(value):
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"version must be a non-negative int, got {value!r}")
    return value

def get_version(state):
    if state is None:
        raise ValueError("state must be a None")
    return validate_version(state.get("version", INITIAL_VERSION))

def bump_version(state):
    updated = dict(state)
    updated["version"] = 1 + get_version(state)

    return updated

def versioned_fields_changed(old_state, new_state):
    return any([old_state(field) != new_state.get(field) for field in VERSIONED_FIELDS])




def compare_versions(a, b):
    a = validate_version(a)
    b = validate_version(b)

    return (a > b) - (a < b)

def is_stale(local, incoming):
    if local is None: return False
    return compare_versions(get_version(incoming), get_version(local)) < 0

def is_duplicate(local, incoming):
    if local is None: return False
    return get_version(incoming) == get_version(local) and incoming.get("owner") == local.get("owner")

def resolve_update(local, incoming):
    if incoming is None: 
        raise ValueError("incoming state must not be None")
    
    incoming_version = get_version(incoming)
    
    if local is None:
        return incoming, NEW

    if local.get("worker_id") != incoming.get("worker_id"):
        raise ValueError("cannot resolve states for different workers")

    order = compare_versions(incoming_version, get_version(local))

    if order > 0:
        return incoming, NEWER

    if order < 0:
        return local, STALE

    local_owner = str(local.get("owner") or "")
    incoming_owner = str(incoming.get("owner") or "")

    if incoming_owner == local_owner:
        return local, DUPLICATE

    if incoming_owner > local_owner:
        return incoming, CONFLICT_WON

    return local, CONFLICT_LOST

def should_apply(local, incoming):
    _, outcome = resolve_update(local, incoming)

    return outcome in ACCEPTED_OUTCOMES

def apply_update(states, incoming):
    worker_id = incoming.get("worker_id")

    if not isinstance(worker_id, str) or not worker_id:
        raise ValueError("incoming state needs a worker_id")

    winner, outcome = resolve_update(states.get(worker_id), incoming)

    if outcome in ACCEPTED_OUTCOMES:
        states[worker_id] = dict(winner)

    return outcome