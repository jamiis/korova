import logging
import logging.config


levels = { 'debug': logging.DEBUG,
           'info': logging.INFO,
           'warning': logging.WARNING,
           'error': logging.ERROR,
           'critical': logging.CRITICAL}

# TODO should definitely be a Class but fuck it
def setup(name, filename, level=logging.INFO):
    config = {
            # 'disable_existing_loggers': False,
            'version': 1,
            'formatters': {
                'short': {
                    'format': '%(asctime)s %(levelname)s: %(message)s'
                    },
                },
            'handlers': {
                'file': {
                    'level': level,
                    'formatter': 'short',
                    'class': 'logging.FileHandler',
                    'filename': filename,
                    },
                'console': {
                    'level': level,
                    'formatter': 'short',
                    'class': 'logging.StreamHandler',
                    },
                },
            'loggers': {
                name: {
                    'handlers': ['file','console'],
                    'level': level,
                    },
                },
            }
    logging.config.dictConfig(config)
    logger = logging.getLogger(name)
    return logger

__all__ = ['setup_logging', 'loglevels']
