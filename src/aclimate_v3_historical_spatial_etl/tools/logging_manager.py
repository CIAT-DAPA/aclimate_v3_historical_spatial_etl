import logging
from typing import Optional, Dict, Any
from functools import partial
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
import os
from dotenv import load_dotenv

class LoggingManager:
    """Centralized logging management with SigNoz/standard logging fallback."""
    
    def __init__(self, service_name: str = "historical_spatial_etl_service"):
        load_dotenv()
        self.service_name = service_name
        self.endpoint = os.getenv('OTLP_ENDPOINT', 'localhost:4317')
        self._signoz_enabled = False
        self._initialize_logging()

    def _initialize_logging(self):
        """Initialize logging with SigNoz if available, otherwise standard logging."""
        # Basic standard logging setup
        self.logger = logging.getLogger(self.service_name)
        self.logger.setLevel(logging.INFO)

        # Console handler as fallback
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(console_handler)

        # Try SigNoz configuration
        try:

            try:
                resource = Resource(attributes={
                    "service.name": self.service_name,
                    "service.version": "1.0",
                    "deployment.environment": "production"
                })

                logger_provider = LoggerProvider(resource=resource)
                _logs.set_logger_provider(logger_provider)

                exporter = OTLPLogExporter(
                    endpoint=self.endpoint,
                    insecure=True
                )

                logger_provider.add_log_record_processor(
                    BatchLogRecordProcessor(exporter)
                )

                class SigNozLogHandler(LoggingHandler):
                    def emit(self, record: logging.LogRecord) -> None:
                        extra_data = getattr(record, 'extra', {})
                        # Rename conflicting keys before passing to parent
                        safe_extra = {}
                        for key, value in extra_data.items():
                            if key in ['args', 'msg', 'levelname', 'created']:
                                safe_key = f"_{key}"
                            else:
                                safe_key = key
                            safe_extra[safe_key] = value
                        super().emit(record)

                signoz_handler = SigNozLogHandler(
                    logger_provider=logger_provider,
                    level=logging.INFO
                )
                self.logger.addHandler(signoz_handler)
                self._signoz_enabled = True
                
                self.logger.info(
                    "SigNoz configured successfully",
                    extra={'component': 'logging_setup'}
                )
                
            except Exception as signoz_error:
                self.logger.warning(
                    f"Failed to configure SigNoz: {str(signoz_error)}",
                    extra={'component': 'logging_setup'}
                )

        except ImportError:
            self.logger.warning(
                "OpenTelemetry not available - using standard logging",
                extra={'component': 'logging_setup'}
            )

    def log(
        self,
        level: str,
        message: str,
        component: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Unified logging method.
        
        Args:
            level: Log level ('info', 'warning', 'error', etc.)
            message: Message to log
            component: Component/module generating the log
            extra: Additional metadata for structured logging
        """
        extra = extra or {}
        if component:
            extra['component'] = component

        # Handle reserved attribute names
        safe_extra = {}
        for key, value in extra.items():
            if key in ['args', 'msg', 'levelname', 'created']:
                safe_key = f"_{key}"
            else:
                safe_key = key
            safe_extra[safe_key] = value

        log_method = getattr(self.logger, level.lower(), self.logger.info)
        
        if self._signoz_enabled:
            log_method(message, extra=safe_extra)
        else:
            if safe_extra:
                message = f"{message} | Metadata: {safe_extra}"
            log_method(message)

    # Convenience methods
    def info(self, message: str, component: Optional[str] = None, **kwargs):
        self.log('info', message, component, kwargs)

    def warning(self, message: str, component: Optional[str] = None, **kwargs):
        self.log('warning', message, component, kwargs)

    def error(self, message: str, component: Optional[str] = None, **kwargs):
        self.log('error', message, component, kwargs)

    def debug(self, message: str, component: Optional[str] = None, **kwargs):
        self.log('debug', message, component, kwargs)

    def exception(self, message: str, component: Optional[str] = None, **kwargs):
        self.log('exception', message, component, kwargs)


# Create a default instance for easy import
logging_manager = LoggingManager()
log = logging_manager.log
info = logging_manager.info
warning = logging_manager.warning
error = logging_manager.error
debug = logging_manager.debug
exception = logging_manager.exception