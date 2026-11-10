"""Boolean environment switches for the security subsystems."""
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def env_flag(name: str, default: bool = False, *, invalid: Optional[bool] = None) -> bool:
    """Read a boolean environment variable.

    1/true/yes/on and 0/false/no/off are accepted in any case, with surrounding whitespace
    ignored. An unset or blank variable gives ``default``.

    Any other value is a mistake, and guessing is how a protection gets turned off by a typo
    (ENABLE_POLICIES=ture). By default it raises ValueError naming the variable, so the process
    refuses to start. Pass ``invalid=True`` or ``invalid=False`` to log a warning and use that
    value instead; choose the value that leaves the protection ON.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().casefold()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    if invalid is None:
        raise ValueError(
            f"{name}={raw!r} is not a recognised boolean; use 1/true/yes/on or 0/false/no/off")
    logger.warning("%s=%r is not a recognised boolean; treating it as %s", name, raw, invalid)
    return invalid
