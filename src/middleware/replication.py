INITIAL_VERSION = 0

VERSIONED_FIELDS = ("worker_id", "host", "port", "models", "status", "owner")

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