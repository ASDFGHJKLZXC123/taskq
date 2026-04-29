class RetriableError(Exception):
    """Raised by handlers when the failure may succeed on retry (5xx, 429, network blip)."""


class FatalError(Exception):
    """Raised by handlers when retrying cannot help (bad payload, 4xx); task goes straight to DEAD."""
