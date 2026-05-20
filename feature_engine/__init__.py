"""
feature_engine
==============

L2 of the FORGE 14-layer architecture.

Reads raw OHLCV + L1 collector output, computes derived features
(Hurst exponent, market breadth, sector rotation, liquidity, realized
vol), and publishes them to the ``computed_features`` + ``stock_profiles``
tables and to Redis keys ``features:{ticker}`` (2h TTL).

Disabled when ``settings.feature_engine_enabled`` is ``False``.
"""
