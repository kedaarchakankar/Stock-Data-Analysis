# Write stuff here
from app import parse_iso_utc
import unittest
from datetime import datetime, timezone

# Unit test class
class TestParseIsoUtc(unittest.TestCase):
    def test_z_suffix(self):
        dt_str = "2024-07-02T15:30:00Z"
        result = parse_iso_utc(dt_str)
        expected = datetime(2024, 7, 2, 15, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)
    
    def test_with_explicit_utc_offset(self):
        dt_str = "2024-07-02T15:30:00+00:00"
        result = parse_iso_utc(dt_str)
        expected = datetime(2024, 7, 2, 15, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_naive_datetime(self):
        dt_str = "2024-07-02T15:30:00"
        result = parse_iso_utc(dt_str)
        expected = datetime(2024, 7, 2, 15, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_with_non_utc_offset(self):
        dt_str = "2024-07-02T17:30:00+02:00"
        result = parse_iso_utc(dt_str)
        expected = datetime(2024, 7, 2, 17, 30, 0, tzinfo=datetime.fromisoformat(dt_str).tzinfo)
        self.assertEqual(result, expected)

    def test_invalid_format(self):
        dt_str = "2024/07/02 15:30:00"
        with self.assertRaises(ValueError):
            parse_iso_utc(dt_str)

# To run the tests
if __name__ == "__main__":
    unittest.main()
