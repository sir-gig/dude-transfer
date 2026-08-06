"""Public exception hierarchy for actionable transfer failures."""


class DudeError(Exception):
    """Base class for Dude failures."""


class ConfigurationError(DudeError, ValueError):
    """Raised when transfer options are invalid or unsafe."""


class CheckpointError(DudeError):
    """Raised when durable resume state cannot be trusted."""


class SourceChangedError(DudeError):
    """Raised when a source file changes while an upload is running."""


class UploadVerificationError(DudeError):
    """Raised when S3 does not contain the object Dude just committed."""

