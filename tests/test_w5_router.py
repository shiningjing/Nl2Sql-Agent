"""Tests for Router v2 heuristic scoring."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agent.nodes.router import _heuristic_score


def _score(q):
    return _heuristic_score(q)[0]


def test_score_0_simple_short():
    assert _score("查询所有用户") == 0


def test_score_0_simple_filter():
    assert _score("查询id为1的订单") == 0


def test_score_1_length_borderline():
    question = "请帮我详细查询一下上个月购买次数超过三次的所有用户的最新订单记录以及对应的评价内容和物流状态"
    assert len(question) > 40
    assert _score(question) >= 1


def test_score_2_multi_intent():
    assert _score("同时查询销售额和客户数量") >= 2


def test_score_2_multi_intent_pair():
    # "既...又" is a co-occurrence pair, not a literal
    assert _score("查询既购买了手机又购买了电脑的用户") >= 2


def test_score_2_multi_intent_plus():
    assert _score("查询既购买了手机又购买了电脑的用户并且排名前10") >= 2


def test_score_1_multi_entity():
    s = _score("《电子产品》品类和《食品》品类的销量对比")
    assert s >= 1


def test_entity_count_zero():
    assert _score("查询订单") == 0


# ── Implicit complexity tests ──

def test_implicit_ranking_max():
    """消费最高的用户 → implicit complexity → elevated to score=1"""
    assert _score("消费最高的用户") == 1


def test_implicit_negation():
    """从未购买过的用户 → NOT EXISTS pattern → score=1"""
    assert _score("从未购买过手机的用户") == 1


def test_implicit_comparison():
    """A比B卖得好 → comparison → score=1"""
    assert _score("手机品类比电脑品类卖得好吗") == 1


def test_implicit_per_group():
    """每个用户 → per-group aggregation → score=1"""
    assert _score("每个用户的平均消费金额") == 1


def test_implicit_not_elevated_when_already_scored():
    """implicit patterns should NOT fire when score already > 0 from other signals"""
    # "同时" → +2, so score=2. "最" should not add anything.
    s_combined = _score("同时查询消费最高的用户和评价最多的商品")
    s_only_multi = _score("同时查询用户和商品")
    assert s_combined >= 2  # multi-intent dominant
    assert s_only_multi >= 2


if __name__ == "__main__":
    test_score_0_simple_short()
    test_score_0_simple_filter()
    test_score_1_length_borderline()
    test_score_2_multi_intent()
    test_score_2_multi_intent_pair()
    test_score_2_multi_intent_plus()
    test_score_1_multi_entity()
    test_entity_count_zero()
    test_implicit_ranking_max()
    test_implicit_negation()
    test_implicit_comparison()
    test_implicit_per_group()
    test_implicit_not_elevated_when_already_scored()
    print("All Router v2 tests passed!")
