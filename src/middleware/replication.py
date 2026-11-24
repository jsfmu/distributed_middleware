
import copy
import json

INITIAL_VERSION = 0

VERSIONED_FIELDS = (
    "worker_id",
    "host",
    "port",
    "models",
    "status",
    "owner",
    "active_requests",
    "capacity",
    "cpu_load_1m",
    "hardware",
    "deleted",
)
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
    if not isinstance(state, dict):
        raise ValueError("state must be a dictionary")

    return validate_version(
        state.get("version", INITIAL_VERSION)
    )

def bump_version(state):
    current_version = get_version(state)

    updated = copy.deepcopy(state)
    updated["version"] = current_version + 1

    return updated

def versioned_fields_changed(old_state, new_state):
    if not isinstance(new_state, dict):
        raise ValueError("new_state must be a dictionary")

    if old_state is None:
        return True

    if not isinstance(old_state, dict):
        raise ValueError("old_state must be a dictionary or None")

    return any(
        old_state.get(field) != new_state.get(field)
        for field in VERSIONED_FIELDS
    )

def compare_versions(a, b):
    a = validate_version(a)
    b = validate_version(b)

    return (a > b) - (a < b)

def _conflict_key(state):
    """
    Deterministic tie-breaker for equal-version states.

    Prefer higher owner ID, then compare the versioned
    worker fields in a stable representation.
    """

    fields = {
        field: state.get(field)
        for field in VERSIONED_FIELDS
    }

    return (
        str(state.get("owner") or ""),
        json.dumps(
            fields,
            sort_keys=True,
            separators=(",", ":")
        )
    )

def is_stale(local, incoming):
    if local is None: return False
    return compare_versions(get_version(incoming), get_version(local)) < 0

def is_duplicate(local, incoming):
    if local is None:
        return False

    return (
        get_version(local) == get_version(incoming)
        and _conflict_key(local) == _conflict_key(incoming)
    )


def resolve_update(local, incoming):
    if not isinstance(incoming, dict):
        raise ValueError(
            "incoming state must be a dictionary"
        )

    incoming_version = get_version(incoming)

    if local is None:
        return incoming, NEW

    if not isinstance(local, dict):
        raise ValueError(
            "local state must be a dictionary or None"
        )

    if local.get("worker_id") != incoming.get("worker_id"):
        raise ValueError(
            "cannot resolve states for different workers"
        )

    order = compare_versions(
        incoming_version,
        get_version(local)
    )

    if order > 0:
        return incoming, NEWER

    if order < 0:
        return local, STALE

    # Equal versions with identical state are duplicates.
    if is_duplicate(local, incoming):
        return local, DUPLICATE

    # Equal versions with different state need a stable winner.
    # Higher owner ID wins first; if owners are equal,
    # use the canonical representation of versioned fields.
    if _conflict_key(incoming) > _conflict_key(local):
        return incoming, CONFLICT_WON

    return local, CONFLICT_LOST

def should_apply(local, incoming):
    _, outcome = resolve_update(local, incoming)

    return outcome in ACCEPTED_OUTCOMES

def apply_update(states, incoming):
    if not isinstance(incoming, dict):
        raise ValueError(
            "incoming state must be a dictionary"
        )

    worker_id = incoming.get("worker_id")

    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError(
            "incoming state needs a worker_id"
        )

    winner, outcome = resolve_update(
        states.get(worker_id),
        incoming
    )

    if outcome in ACCEPTED_OUTCOMES:
        states[worker_id] = copy.deepcopy(winner)

    return outcome