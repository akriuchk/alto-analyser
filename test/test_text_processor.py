from unittest import TestCase

from processor import text_processor
from processor.text_processor import collect_window_stats, merge_tokens_stats


class Test(TestCase):
    def test_find_ngrams(self):
      text_processor.token_cache.clear()
      result = {}
      updates = collect_window_stats(result,["a", "b", "c"], [3, 5])
      for token in updates:
        token.recalculate_distance_bags()

      print(result)
      # assert tokens are present
      assert 'a' in result.keys()
      assert 'b' in result.keys()
      assert 'c' in result.keys()

      # assert both ranges are present
      assert 3 in result['a'].stats.keys()
      assert 5 in result['b'].stats.keys()

      # assert random value
      assert 1 == result['c'].stats[3].counter[1]['b']

      assert 2 == result['a'].stats[3].total_neighbors()


    def test_find_ngrams_update(self):
      text_processor.token_cache.clear()
      result = {}
      updates = collect_window_stats(result,["d", "b", "a", "b", "c", "z"], [3, 5])
      for token in updates:
        token.recalculate_distance_bags()

      print(updates)

      assert 2 == result['a'].stats[3].counter[1]['b']
      assert 2 == result['a'].stats[5].counter[1]['b']

      assert 1 == result['d'].stats[3].counter[1]['b']
      assert 1 == result['d'].stats[5].counter[5]['z']

      assert 1 == result['c'].stats[3].counter[1]['b']
      assert 1 == result['c'].stats[5].counter[1]['b']

      assert 4 == result['c'].stats[3].total_neighbors()
      assert 5 == result['c'].stats[5].total_neighbors()

      assert 5 == result['a'].stats[3].total_neighbors()

      assert 2 == result['b'].frequency

      print(result['a'].stats[3].get_distance_prob())
      text_processor.print_cache_stats()

    def test_token_add(self):
      text_processor.token_cache.clear()
      result_original = {}
      result_adding = {}

      updates_original = collect_window_stats(result_original,["a", "b", "c"], [3, 5])
      for token in updates_original:
        token.recalculate_distance_bags()

      updates_adding = collect_window_stats(result_adding,["d", "c", "b"], [3, 5])
      for token in updates_adding:
        token.recalculate_distance_bags()

      merge_tokens_stats(result_original, result_adding)

      print(result_original)

      assert 2 == result_original['b'].stats[3].counter[1]['c']
      assert 2 == result_original['c'].stats[3].counter[1]['b']

      assert 1 == result_original['d'].stats[3].counter[1]['c']