import logging
import unittest
from src.utils import setup_logging, load_config


class TestUtils(unittest.TestCase):

    def test_setup_logging(self):
        # Test if setup_logging returns a logger instance
        logger = setup_logging("./config/logging_config.json")
        self.assertIsNotNone(logger)
        self.assertIsInstance(logger, logging.Logger)

    def test_load_config(self):
        # Test if load_config returns a dictionary
        config = load_config("./config/config.yaml")
        self.assertIsNotNone(config)
        self.assertIsInstance(config, dict)

        # Test if specific configuration keys are present
        self.assertIn('spark', config)
        self.assertIn('data', config)


if __name__ == '__main__':
    unittest.main()
