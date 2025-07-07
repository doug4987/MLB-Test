import unittest
from enhanced_bet_resolver import EnhancedBetResolver

class DummyDB:
    pass

class MarketParsingTests(unittest.TestCase):
    def setUp(self):
        self.resolver = EnhancedBetResolver(DummyDB())
        self.box_score = {
            'earned_runs': 2,
            'strikeouts_pitched': 8,
            'walks_allowed': 3,
            'walks': 1
        }

    def test_earned_run_phrase(self):
        result = self.resolver._calculate_actual_result('earned runs allowed', self.box_score)
        self.assertEqual(result, 2.0)

    def test_earned_run_abbrev(self):
        result = self.resolver._calculate_actual_result('ER', self.box_score)
        self.assertEqual(result, 2.0)

    def test_er_not_in_pitcher(self):
        result = self.resolver._calculate_actual_result('Pitcher Strikeouts', self.box_score)
        self.assertEqual(result, 8.0)

    def test_walks_allowed(self):
        result = self.resolver._calculate_actual_result('Walks Allowed', self.box_score)
        self.assertEqual(result, 3.0)

    def test_walks_hitter(self):
        result = self.resolver._calculate_actual_result('Walks', self.box_score)
        self.assertEqual(result, 1.0)

if __name__ == '__main__':
    unittest.main()
