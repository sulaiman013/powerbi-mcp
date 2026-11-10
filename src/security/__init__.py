"""
Power BI MCP Security Module
PII detection, audit logging, access policy enforcement and secret redaction.

This is defence in depth for an AI client that is already allowed to query the model. The
security boundary is row-level and object-level security in the model itself.
"""

from .env import env_flag

from .redaction import (
    redact_secrets,
    redact_structure
)

from .pii_detector import (
    PIIDetector,
    PIIType,
    MaskingStrategy,
    mask_pii
)

from .audit_logger import (
    AuditLogger,
    AuditEventType,
    AuditSeverity,
    AuditWriteError,
    get_audit_logger,
    configure_audit_logger
)

from .access_policy import (
    AccessPolicyEngine,
    PolicyAction,
    PolicyLevel,
    PolicyConfigError,
    TablePolicy,
    ColumnPolicy,
    GlobalPolicy,
    PolicyCheckResult,
    create_default_policy_engine
)

from .security_layer import (
    SecurityLayer,
    get_security_layer,
    configure_security_layer
)

__all__ = [
    # Environment switches
    'env_flag',
    # Secret redaction
    'redact_secrets',
    'redact_structure',
    # PII Detection
    'PIIDetector',
    'PIIType',
    'MaskingStrategy',
    'mask_pii',
    # Audit Logging
    'AuditLogger',
    'AuditEventType',
    'AuditSeverity',
    'AuditWriteError',
    'get_audit_logger',
    'configure_audit_logger',
    # Access Policies
    'AccessPolicyEngine',
    'PolicyAction',
    'PolicyLevel',
    'PolicyConfigError',
    'TablePolicy',
    'ColumnPolicy',
    'GlobalPolicy',
    'PolicyCheckResult',
    'create_default_policy_engine',
    # Unified Security Layer
    'SecurityLayer',
    'get_security_layer',
    'configure_security_layer',
]
