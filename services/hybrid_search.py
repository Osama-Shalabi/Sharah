from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


EMBEDDING_MODEL = "local-arabic-char-word-v1"
EMBEDDING_DIM = 384
MIN_SEMANTIC_ONLY_SCORE = 0.3
_ARABIC_DIACRITICS_RE = re.compile(r"[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06ed]")
_NON_WORD_RE = re.compile(r"[^\w\u0600-\u06ff]+", re.UNICODE)


def normalize_arabic(text: str) -> str:
    text = str(text or "").strip().lower()
    text = _ARABIC_DIACRITICS_RE.sub("", text)
    text = text.replace("ـ", "")
    text = re.sub("[إأآا]", "ا", text)
    text = text.replace("ى", "ي")
    text = text.replace("ؤ", "و")
    text = text.replace("ئ", "ي")
    text = text.replace("ة", "ه")
    text = _NON_WORD_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def query_words(text: str) -> List[str]:
    return [w for w in normalize_arabic(text).split() if w]


def keyword_score(query: str, title: str) -> float:
    words = query_words(query)
    if not words:
        return 0.0

    normalized_title = normalize_arabic(title)
    if not normalized_title:
        return 0.0

    matched = sum(1 for word in words if word in normalized_title)
    score = matched / len(words)
    if normalize_arabic(query) and normalize_arabic(query) in normalized_title:
        score = min(1.0, score + 0.2)
    return score


def _hash_feature(feature: str) -> int:
    digest = hashlib.blake2b(feature.encode("utf-8", errors="ignore"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % EMBEDDING_DIM


def _features(text: str) -> Counter[str]:
    normalized = normalize_arabic(text)
    features: Counter[str] = Counter()
    words = normalized.split()

    for word in words:
        features[f"w:{word}"] += 2
        padded = f" {word} "
        for n in (3, 4):
            for i in range(0, max(0, len(padded) - n + 1)):
                features[f"c{n}:{padded[i:i+n]}"] += 1

    for i in range(0, max(0, len(words) - 1)):
        features[f"b:{words[i]} {words[i + 1]}"] += 2

    return features


def create_embedding(text: str) -> List[float]:
    vector = [0.0] * EMBEDDING_DIM
    for feature, count in _features(text).items():
        vector[_hash_feature(feature)] += float(count)

    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]


def embedding_to_json(embedding: List[float]) -> str:
    return json.dumps(embedding, separators=(",", ":"))


def embedding_from_json(raw: str) -> List[float]:
    data = json.loads(raw or "[]")
    if not isinstance(data, list):
        return []
    return [float(v) for v in data]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    value = sum(x * y for x, y in zip(a, b))
    return max(0.0, min(1.0, value))


def parse_upload_date(value: Optional[str]) -> float:
    if not value:
        return 0.0
    s = str(value).strip()
    candidates = [
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(s, fmt).timestamp()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def hybrid_rank_reels(
    *,
    query: str,
    reels: Iterable[Dict[str, Any]],
    stored_embeddings: Dict[str, List[float]],
    limit: int,
) -> List[Dict[str, Any]]:
    query_embedding = create_embedding(query)
    ranked: List[Dict[str, Any]] = []

    for reel in reels:
        reel_id = str(reel.get("id") or "")
        title = str(reel.get("title") or "")
        kw_score = keyword_score(query, title)
        semantic_score = cosine_similarity(query_embedding, stored_embeddings.get(reel_id, []))
        if kw_score <= 0 and semantic_score < MIN_SEMANTIC_ONLY_SCORE:
            continue

        final_score = (kw_score * 0.7) + (semantic_score * 0.3)
        if final_score <= 0:
            continue

        enriched = dict(reel)
        enriched["keywordScore"] = round(kw_score, 6)
        enriched["semanticScore"] = round(semantic_score, 6)
        enriched["score"] = round(final_score, 6)
        ranked.append(enriched)

    ranked.sort(
        key=lambda r: (
            round(float(r.get("score") or 0.0), 3),
            parse_upload_date(r.get("uploadDate")),
        ),
        reverse=True,
    )
    return ranked[: max(1, int(limit or 20))]
