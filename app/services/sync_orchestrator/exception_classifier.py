"""Map an exception to a FailureCategory (spec §5).

Shared by `_tracked_job` (legacy scheduler audit path) and the
orchestrator executor's layer-failure path. One classifier, one
taxonomy.
"""

from __future__ import annotations

import httpx
import psycopg
import psycopg.errors

from app.security.secrets_crypto import MasterKeyNotLoadedError
from app.services.sync_orchestrator.layer_types import FailureCategory, LayerRefreshFailed


def classify_exception(exc: BaseException) -> FailureCategory:
    """Return the FailureCategory that best describes `exc`.

    Conservative: anything not specifically recognised goes to
    INTERNAL_ERROR (retriable). Schema-drift classification requires
    payload-shape evidence a bare exception classifier cannot see —
    adapters that do detect drift should raise an explicit error with
    SCHEMA_DRIFT stored at the call site (out of scope for this helper).
    """
    # LayerRefreshFailed already carries an explicit category — honour
    # it instead of re-classifying. A caller that wraps a raw exception
    # in LayerRefreshFailed(SCHEMA_DRIFT, ...) has strictly more
    # information than this helper can recover from the exception type.
    if isinstance(exc, LayerRefreshFailed):
        return exc.category
    # #643 — broker-encryption key not loaded. Distinct from the
    # generic AUTH_EXPIRED (which means the credential decrypted but
    # the upstream provider rejected it) and from the catch-all
    # INTERNAL_ERROR. Surfaced with an operator-actionable banner
    # via REMEDIES[MASTER_KEY_MISSING] instead of the opaque
    # "Unclassified error" the path used to hit.
    if isinstance(exc, MasterKeyNotLoadedError):
        return FailureCategory.MASTER_KEY_MISSING
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (401, 403):
            return FailureCategory.AUTH_EXPIRED
        if status == 429:
            return FailureCategory.RATE_LIMITED
        if 500 <= status < 600:
            return FailureCategory.SOURCE_DOWN
        return FailureCategory.INTERNAL_ERROR
    # httpx.TransportError is the base for ConnectError, ReadTimeout,
    # ConnectTimeout, WriteTimeout, PoolTimeout, RemoteProtocolError,
    # and other network-layer faults. The base-class check catches
    # future TransportError subclasses without this file needing an
    # update. HTTPStatusError is NOT a TransportError subclass, so the
    # preceding branch still owns status-code classification.
    if isinstance(exc, httpx.TransportError):
        return FailureCategory.SOURCE_DOWN
    if isinstance(exc, psycopg.errors.IntegrityError):
        return FailureCategory.DB_CONSTRAINT
    # OperationalError covers connection failures, lock timeouts, server
    # shutdowns — transient infrastructure, not logic bugs. SOURCE_DOWN
    # is self_heal=True so the retry budget kicks in.
    if isinstance(exc, psycopg.OperationalError):
        return FailureCategory.SOURCE_DOWN
    return FailureCategory.INTERNAL_ERROR
